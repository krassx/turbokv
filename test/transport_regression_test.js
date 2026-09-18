// Regressions for defects found by the adversarial review of the shared-memory
// transport. Every case runs through the REAL public path in a REAL worker
// process, on BOTH transports.
//
// That framing is the point. The primary-L1-coherence defect these guard against
// already had a regression test, but that test called applyBatch() directly --
// so when set() started bypassing applyBatch for the shm ring, the test kept
// passing while the shipped default path served stale data.
const cluster = require('cluster');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');

const T = process.env.TC_T || 'shm';
const ARENA = '/tctr_' + T + '_' + (process.env.TC_RUN || '0');

if (cluster.isPrimary && !process.env.TC_CHILD) {
    let fails = 0;
    const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  [${T}] ${m}`); if (!c) fails++; };

    const c = TurboKV.createPrimary(ARENA, 32 << 20, 1 << 16,
        { storage: 'bytes', transport: T, submitRingBytes: 1 << 16 });
    TurboKV.install(cluster);
    TurboKV.install(cluster);          // idempotent: a second install must not double-apply

    c.set('coh', 'PRIMARY');
    c.get('coh');                          // resident in the primary's own L1
    c.set('own', 'PRIMARY-OWN');
    c.get('own');
    for (let i = 0; i < 40; i++) c.set('bulk' + i, 'x');

    const w = cluster.fork({ TC_CHILD: '1', TC_T: T, TC_ARENA: ARENA });
    w.on('message', (m) => {
        if (!m || m.t !== 'phase') return;
        setTimeout(() => {
            let guard = 0;
            while (TurboKV.drainSubmissions(8192) > 0 && ++guard < 64);

            ok(c.get('coh') === 'WORKER', 'primary L1 follows a worker overwrite');
            ok(c.get('del') === undefined && native.get('del') === undefined,
               'primary L1 follows a worker delete');
            ok(c.get('own') === 'PRIMARY-OWN', "primary keeps its OWN L1 entries (no self-invalidation)");
            ok(m.emptySet === false, "worker set('') is rejected, not queued");
            ok(c.get('after-empty') === 'LANDED',
               "a write after an empty-key attempt still lands (ring not poisoned)");
            // Only meaningful on shm: with no ring, a 200KB value legitimately fits
            // the arena and must be accepted.
            ok(T === 'shm' ? m.bigSet === false : m.bigSet === true,
               T === 'shm' ? 'a value too large for a ring record is rejected, not silently shed'
                           : 'without a ring, a large value that fits the arena is accepted');
            ok(m.delThenGet === undefined, 'worker read-your-writes: delete then get is a miss');
            ok(m.delThenHas === false, 'worker read-your-writes: delete then has is false');
            ok(m.transport === T, `worker negotiated the ${T} transport`);

            console.log(fails ? `\n[${T}] ${fails} FAILED` : `\n[${T}] all passed`);
            for (const id in cluster.workers) cluster.workers[id].kill();
            process.exit(fails ? 1 : 0);
        }, 500);
    });
    setTimeout(() => { console.log(`[${T}] TIMEOUT`); process.exit(1); }, 20000);
} else {
    const c = TurboKV.attachWorker(ARENA, cluster.worker ? cluster.worker.id : 1,
        { storage: 'bytes', transport: T, l1MaxBytes: 1 << 20 });
    c.set('coh', 'WORKER');
    c.set('del', 'to-be-deleted');
    c.delete('del');
    const emptySet = c.set('', 'should-be-rejected');
    c.set('after-empty', 'LANDED');
    const bigSet = c.set('big', 'B'.repeat(200000));   // fits the arena, not a 64KB ring record
    c.set('rd', 'v'); c.delete('rd');
    const delThenGet = c.get('rd');
    const delThenHas = c.has('rd');
    c.flush();
    setTimeout(() => process.send({
        t: 'phase', transport: c.transport, emptySet, bigSet, delThenGet, delThenHas,
    }), 300);
}
