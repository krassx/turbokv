'use strict';
// REMOVALS, AND THE RECORD THAT SAYS THEY HAPPENED.
//
// A worker cannot write the arena, so its removals -- a `delete`, and the
// eviction a `minLevel: L3` write performs -- are applied by the primary a tick
// later. Until then the worker holds the key in #pendingDel so its own reads
// miss rather than serve what the primary has not removed yet, and the mark is
// cleared by the invalidation record the primary publishes when it applies the
// removal.
//
// That record used to exist only when the key was actually present in the
// arena. A removal of an absent key -- every `minLevel: L3` write of a new key,
// every delete of a key that lives only in L3 -- published nothing, so the mark
// was never cleared: the key became permanently unreadable in that worker, L3
// included, because the guard refuses to even ask for a key this process
// removed. Each one also leaked an entry toward the 4096-entry wholesale flush,
// which then drops the REAL pending deletes.
//
// The same missing record left the PRIMARY's promotion guard blind: a delete
// whose L3 round trip finished before an overlapping getAsync returned left no
// trace at all -- the queue had let go of it and the arena had published
// nothing -- so an L3-only key was written straight back into the shared arena.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCR_ARENA || ('/tcl3rem' + process.pid);

// --------------------------------------------------------------- worker ----
if (process.env.TCR_ROLE === 'worker') {
    (async () => {
        const f = makeFake();
        const w = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l3: f.adapter });
        ok(w.transport === 'shm', `the worker negotiated the shm transport (${w.transport})`);

        // 1. A `minLevel: L3` write of a key the arena has never held. The
        //    write evicts L2 -- of a key that is not there -- and that eviction
        //    is what clears the mark.
        ok(await w.setAsync('only3', 'PAYLOAD', { minLevel: TurboKV.L3 }) === true,
           'the minLevel L3 write is accepted');
        ok(f.store.get('only3').value === 'PAYLOAD', 'and reaches L3');
        await sleep(60);
        w.get('poke');                                  // drain the invalidation ring
        const g1 = await w.getAsync('only3');
        ok(g1 === 'PAYLOAD', `the key this worker just wrote is readable from L3 (${g1})`);
        ok(await w.hasAsync('only3') === true, 'hasAsync agrees');
        ok(f.calls.some(x => x[0] === 'get' && x[1] === 'only3'),
           'and the adapter was actually asked, rather than refused locally');

        // 2. A delete of a key the arena does not hold. Same route, and the
        //    delete is what the caller is told succeeded.
        f.store.set('remote', { value: 'R1', expiresAt: 0 });
        await w.deleteAsync('remote');
        f.store.set('remote', { value: 'R2', expiresAt: 0 });   // written again elsewhere
        await sleep(60);
        w.get('poke');
        const g2 = await w.getAsync('remote');
        ok(g2 === 'R2', `a key deleted while absent from L2 is readable again (${g2})`);
        ok(await w.hasAsync('remote') === true, 'hasAsync agrees for it too');

        // 3. The marks do not accumulate. Every one of these is an absent-key
        //    removal; if the record that clears them is missing they all pile
        //    up, and the 1000th is as unreadable as the first.
        for (let i = 0; i < 64; i++) w.set('mass' + i, 'v', { minLevel: TurboKV.L3 });
        await sleep(80);
        w.get('poke');
        let readable = 0;
        for (let i = 0; i < 64; i++) if (await w.getAsync('mass' + i) === 'v') readable++;
        ok(readable === 64, `all 64 absent-key evictions cleared their marks (${readable}/64)`);

        // 4. A DEGRADED worker has no primary to apply anything, so nothing can
        //    ever publish a record for it. Taking the mark there is a mark that
        //    survives until recovery -- and nothing local can serve a stale
        //    value anyway, because a degraded worker answers from L1 only and
        //    the write evicted L1 itself.
        const d = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l3: f.adapter });
        d.__unsafeForcePrimaryDead();
        ok(d.set('degraded', 'DV', { minLevel: TurboKV.L3 }) === true,
           'a degraded worker still accepts a minLevel L3 write');
        await sleep(30);
        const g4 = await d.getAsync('degraded');
        ok(g4 === 'DV', `and can still read it back from L3 (${g4})`);
        d.close();

        w.close();
        console.log(fail ? `  ${fail} failed (worker)` : '  [l3-removal] worker cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// ------------------------------------------------ worker, tiny ring slot ----
// A ring too small to take the record is the other way a removal is never
// submitted. Nothing will ever publish it, so a mark taken for it is a mark
// nothing can clear -- which is strictly worse than the contract a shed write
// already has, where the failure is counted and reported rather than hidden
// behind a local miss with no end.
if (process.env.TCR_ROLE === 'shed') {
    (async () => {
        const f = makeFake();
        const w = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l3: f.adapter });
        // Fill the ring: the primary was started with maintenance off, so
        // nothing drains it.
        // Small records, so the ring is full for a DELETE record too -- a ring
        // with room left for a 40-byte record would take the delete and prove
        // nothing.
        for (let i = 0; i < 2000 && !(w.stats.writesShed > 0); i++) w.set('fill' + i, 'v');
        ok(w.stats.writesShed > 0, `the submission ring is full (shed ${w.stats.writesShed})`);
        f.store.set('shedkey', { value: 'S1', expiresAt: 0 });
        const before = w.stats.writesShed;
        w.delete('shedkey');
        ok(w.stats.writesShed > before, 'the delete itself is shed');
        await w.drainL3();                              // let the L3 DEL land
        f.store.set('shedkey', { value: 'S2', expiresAt: 0 });
        const g = await w.getAsync('shedkey');
        ok(g === 'S2', `a shed removal does not make its key permanently unreadable (${g})`);
        w.close();
        console.log(fail ? `  ${fail} failed (shed)` : '  [l3-removal] shed cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// -------------------------------------------------------------- primary ----
(async () => {
    // A. The arena publishes the removal of a key it does not hold.
    {
        const primary = TurboKV.createPrimary(ARENA + 'a', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false });
        const head0 = native.stats().ringHead;
        ok(primary.delete('never-here') === false, 'deleting an absent key still reports false');
        const moved = native.stats().ringHead - head0;
        ok(moved === 1, `and still publishes exactly one invalidation record (${moved})`);
        primary.close();
    }

    // B. THE RESIDUAL OF THE ORIGINAL CRITICAL.
    //
    // The DEL settles BEFORE the overlapping read returns, and the key lives
    // only in L3. The queue has already let go of the operation, so the
    // "what do I still owe L3" check is clear; #pendingDel does not exist on a
    // primary. The only thing left that can say the key was removed is the
    // arena's own record -- which is why it has to be published for an absent
    // key too.
    {
        const store = new Map([['only3', 'L3ONLY']]);
        // Reads the store AT CALL TIME and then takes 120ms to answer: a real
        // round trip that observed the value before the delete was issued.
        const adapter = {
            async get(k) { const v = store.get(k); await sleep(120); return v === undefined ? undefined : { value: v }; },
            async set(k, v) { store.set(k, v); },
            async delete(k) { store.delete(k); },
            async clear() { store.clear(); },
        };
        const primary = TurboKV.createPrimary(ARENA + 'b', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false, l3: adapter });
        ok(native.get('only3') === undefined, 'the key is not in the arena to begin with');
        const read = primary.getAsync('only3');
        await sleep(10);
        await primary.deleteAsync('only3');             // settles well before the read
        ok(store.has('only3') === false, 'the L3 delete landed while the read was still out');
        const got = await read;
        ok(native.get('only3') === undefined,
           `the removed value is NOT written back into the shared arena (${native.get('only3')})`);
        ok(got === undefined,
           `and the caller is not handed the value it just deleted (${JSON.stringify(got)})`);
        ok(primary.get('only3') === undefined, `sync get agrees (${primary.get('only3')})`);
        ok(primary.stats.l3DeletedWhileReading === 1,
           `counted as a prevented resurrection, not as contention (${primary.stats.l3DeletedWhileReading})`);
        await primary.close();
    }

    // C. A read that started AFTER the delete must still be free to promote
    //    whatever L3 holds now -- the guard remembers where the removal landed,
    //    not merely that it happened, so it cannot become a permanent refusal.
    {
        const f = makeFake();
        const primary = TurboKV.createPrimary(ARENA + 'c', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false, l3: f.adapter });
        await primary.setAsync('cycle', 'A');
        await primary.deleteAsync('cycle');
        f.store.set('cycle', { value: 'B', expiresAt: 0 });   // written by someone else
        const got = await primary.getAsync('cycle');
        ok(got === 'B', `a later read still promotes (${got})`);
        ok(native.get('cycle') === 'B', `into the shared arena (${native.get('cycle')})`);
        await primary.close();
    }

    // D. WITH NO ADAPTER nothing about a delete changes: no map is populated,
    //    and the arena still answers exactly as before.
    {
        const primary = TurboKV.createPrimary(ARENA + 'd', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false });
        primary.set('k', 'v');
        ok(primary.delete('k') === true, 'delete of a present key reports true');
        ok(primary.delete('k') === false, 'and of an absent key reports false');
        ok(primary.get('k') === undefined, 'the key is gone');
        primary.set('k', 'again');
        ok(await primary.getAsync('k') === 'again', 'a rewritten key reads back');
        primary.close();
    }

    // E. The worker cases, against a real primary.
    {
        // A short maintenance interval, because that timer is what drains the
        // submission ring here: this is a plain fork, not a cluster, so there is
        // no channel for the worker's doorbell to arrive on.
        const primary = TurboKV.createPrimary(ARENA, 16 << 20, 1 << 14, { storage: 'bytes', maintenanceMs: 25 });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCR_ROLE: 'worker', TCR_ARENA: ARENA }, stdio: 'inherit',
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the worker cases passed (child exited ${code})`);
        await primary.close();
    }

    // F. The shed case needs a ring too small to take a record and a primary
    //    that never drains it.
    {
        const primary = TurboKV.createPrimary(ARENA + 'f', 16 << 20, 1 << 14,
            { storage: 'bytes', maintenance: false, submitRings: 2, submitRingBytes: 4096 });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCR_ROLE: 'shed', TCR_ARENA: ARENA + 'f' }, stdio: 'inherit',
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the shed cases passed (child exited ${code})`);
        await primary.close();
    }

    // G. WHAT deleteAsync RESOLVES.
    //
    // There is no local tombstone once a delete has been applied, so this
    // promise is the only signal the design offers that the REMOTE half failed
    // -- spec 9, spec 5.4 and decision 70 all rest on "the caller resolved
    // false and knows it". Resolving on local presence made that guarantee
    // false in both directions.
    {
        const f = makeFake();
        const primary = TurboKV.createPrimary(ARENA + 'h', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 30 });
        // (i) present locally, but the L3 delete fails.
        await primary.setAsync('present', 'v');
        f.fail.set('delete', new Error('l3 down'));
        const r1 = await primary.deleteAsync('present');
        ok(r1 === false, `a failed L3 delete resolves false (${r1})`);
        ok(f.store.has('present') === true, 'L3 still holds the key, which is what false means');
        ok(primary.stats.l3DeleteFailed === 1, `and the failure is counted (${primary.stats.l3DeleteFailed})`);
        ok(primary.delete('present') === false,
           'the synchronous form still answers local presence, for a caller that wants it');
        // (ii) absent locally, and the L3 delete succeeds.
        f.fail.delete('delete');
        f.store.set('l3only', { value: 'v', expiresAt: 0 });
        const r2 = await primary.deleteAsync('l3only');
        ok(r2 === true, `an L3-only key whose delete landed resolves true (${r2})`);
        ok(f.store.has('l3only') === false, 'and it really is gone from L3');
        // (iii) a shed delete: the queue refused it, so it never reached L3.
        await primary.close();
    }

    // H. A shed L3 delete resolves false too: the byte bound refused it, so L3
    //    still holds the key and the caller has to know.
    {
        const f = makeFake();
        f.latency.set('delete', 200);
        const primary = TurboKV.createPrimary(ARENA + 'i', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3QueueMaxBytes: 1 });
        f.store.set('shed', { value: 'v', expiresAt: 0 });
        const r = await primary.deleteAsync('shed');
        ok(r === false, `a shed L3 delete resolves false (${r})`);
        ok(f.store.has('shed') === true, 'L3 was never asked, so it still holds the key');
        await primary.close();
    }

    // I. WITH NO ADAPTER the answer is the local one, exactly as before.
    {
        const primary = TurboKV.createPrimary(ARENA + 'j', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false });
        primary.set('k', 'v');
        ok(await primary.deleteAsync('k') === true, 'deleteAsync reports a key that was present');
        ok(await primary.deleteAsync('k') === false, 'and one that was not');
        ok(await primary.deleteAsync(42) === false, 'a non-string key is still refused');
        primary.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-removal] all passed');
    process.exit(fail ? 1 : 0);
})();
