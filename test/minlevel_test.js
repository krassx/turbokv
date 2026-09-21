// minLevel: which tier a record is allowed to occupy, per operation.
//
// Measured motivation (bench/scan_pollution.js): one pass over a cold keyspace
// evicts 93.7% of a hot working set from L1 -- 100% when the cold values are the
// same size as the hot ones, which is why a size threshold cannot substitute for
// the caller's own knowledge that it is scanning. The scan is also 3.8x cheaper
// when it does not promote, so both sides win.
//
// The invariant under test: minLevel changes only WHERE a record lives and how
// fast it is reached. It never changes which value is observed.
const cluster = require('cluster');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');

const ARENA = '/tcml_' + process.pid;

if (cluster.isPrimary && !process.env.TC_CHILD) {
    let fail = 0;
    const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };

    const c = TurboKV.createPrimary(ARENA, 32 << 20, 1 << 14,
                                    { storage: 'bytes', transport: 'shm', submitRingBytes: 1 << 16 });
    TurboKV.install(cluster);

    // --- set: bypassing L1 still reaches L2
    c.set('bypass', 'V', { minLevel: TurboKV.L2 });
    ok(c.l1Size === 0, `a minLevel L2 write does not occupy L1 (l1Size=${c.l1Size})`);
    ok(native.get('bypass') === 'V', 'and it does reach L2');
    ok(c.get('bypass') === 'V', 'and the value is still observable — placement only');

    // --- the stale-read guard: bypassing must EVICT an existing copy
    c.clearLocal();
    c.set('evict', 'OLD');
    ok(c.get('evict') === 'OLD' && c.l1Size === 1, 'a normal write is resident in L1');
    c.set('evict', 'NEW', { minLevel: TurboKV.L2 });
    ok(c.get('evict') === 'NEW',
       'a bypassing overwrite evicts the stale L1 copy rather than leaving it to be served');

    // --- get: the fill is bounded, the value is not
    c.clearLocal();
    const viaL2 = c.get('bypass', { minLevel: TurboKV.L2 });
    ok(viaL2 === 'V', 'a minLevel L2 read returns the value');
    ok(c.l1Size === 0, `and leaves L1 untouched (l1Size=${c.l1Size})`);
    ok(c.get('bypass') === 'V' && c.l1Size === 1, 'while a default read does fill L1');

    // --- clamping: L3 does not exist yet, so it behaves as L2 and stores anyway
    c.clearLocal();
    c.set('clamped', 'C', { minLevel: TurboKV.L3 });
    ok(native.get('clamped') === 'C',
       'minLevel L3 clamps to the highest tier that exists and still stores the value');
    ok(c.l1Size === 0, 'and behaves as L2 for placement today');

    // --- an invalid level is a mistake, not a preference
    for (const bad of [0, 4, 2.5, 'l2', null]) {
        let threw = false;
        try { c.get('bypass', { minLevel: bad }); } catch (e) { threw = e instanceof TypeError; }
        ok(threw, `minLevel ${JSON.stringify(bad)} throws a TypeError`);
    }

    const w = cluster.fork({ TC_CHILD: '1', TC_ARENA: ARENA });
    w.on('message', (m) => {
        if (!m || m.t !== 'done') return;
        ok(m.ownReadAfterBypass === undefined,
           `a worker's own read after a bypassing write MISSES rather than serving L2's ` +
           `previous value (got ${JSON.stringify(m.ownReadAfterBypass)})`);
        ok(m.eventualValue === 'WORKER-NEW',
           `and once the write lands the new value is observable (got ${JSON.stringify(m.eventualValue)})`);
        ok(m.l1AfterBypass === 0, `the bypassing write left nothing in the worker's L1 (${m.l1AfterBypass})`);

        const ws = Object.values(cluster.workers);
        let left = ws.length;
        const done = () => { if (--left === 0) {
            // --- levels are not contiguous once L3 exists -----------------
            //
            // A worker that has lost its primary has L1 and L3 but no L2, so
            // "clamp down to the highest available level" cannot be computed
            // as a single ceiling. Run last: these instances share the id-0
            // arena state with the primary `c` above, and close() on them
            // tears that arena down -- which would otherwise race the
            // worker's attachWorker() if run any earlier.
            {
                const { makeFake } = require('./l3_fake');
                const withL3 = new TurboKV({ storage: 'bytes', l3: makeFake().adapter });
                ok(withL3.__unsafeResolveLevel(3) === 3, 'L3 stays L3 when an adapter is configured');
                ok(withL3.__unsafeResolveLevel(2) === 2, 'L2 stays L2 while the primary is alive');

                const noL3 = new TurboKV({ storage: 'bytes' });
                ok(noL3.__unsafeResolveLevel(3) === 2, 'L3 clamps to L2 with no adapter');

                withL3.__unsafeForcePrimaryDead();
                ok(withL3.__unsafeResolveLevel(3) === 3, 'a degraded worker keeps L3');
                ok(withL3.__unsafeResolveLevel(2) === 1, 'a degraded worker clamps L2 to L1');

                const deadNoL3 = new TurboKV({ storage: 'bytes' });
                deadNoL3.__unsafeForcePrimaryDead();
                ok(deadNoL3.__unsafeResolveLevel(3) === 1, 'no adapter and no primary clamps L3 to L1');
                ok(deadNoL3.__unsafeResolveLevel(2) === 1, 'no adapter and no primary clamps L2 to L1');

                withL3.close(); noL3.close();
            }
            native.destroy();
            console.log(fail ? `  ${fail} FAILURES` : '  all passed');
            process.exit(fail ? 1 : 0);
        } };
        const guard = setTimeout(() => { for (const x of ws) x.kill('SIGTERM'); }, 3000);
        guard.unref && guard.unref();
        for (const x of ws) { x.once('exit', done); try { x.send({ t: 'bye' }); } catch { x.kill('SIGTERM'); } }
    });
    setTimeout(() => { console.log('  TIMEOUT'); process.exit(1); }, 20000);
} else {
    const c = TurboKV.attachWorker(process.env.TC_ARENA, cluster.worker.id,
                                   { storage: 'bytes', transport: 'shm' });
    process.on('message', (m) => { if (m && m.t === 'bye') process.exit(0); });

    // Seed a value the worker can read, so its L1 holds something to go stale.
    c.set('wkey', 'WORKER-OLD');
    setTimeout(() => {
        c.get('wkey');                                   // resident in this worker's L1
        c.set('wkey', 'WORKER-NEW', { minLevel: TurboKV.L2 });
        // The write is in flight. L2 still holds WORKER-OLD, so a stale read is
        // exactly what the pending marking exists to prevent.
        const ownReadAfterBypass = c.get('wkey');
        const l1AfterBypass = c.l1Size;
        setTimeout(() => {
            process.send({ t: 'done', ownReadAfterBypass, l1AfterBypass, eventualValue: c.get('wkey') });
        }, 500);
    }, 300);
}
