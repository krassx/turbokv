'use strict';
// THE PROMOTION GUARD.
//
// A read that started before a write can return the older value after it. If
// that value is promoted, it overwrites the newer one in L2 -- not stale,
// WRONG, and it stays wrong until the next write to that key. The guard marks
// the invalidation ring before the await and refuses to promote if the key was
// invalidated while the read was in flight.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
// One name for both processes: deriving it from process.pid would give the
// primary and the forked worker different arenas, and the worker could not
// attach at all.
const ARENA = process.env.TCG_ARENA || ('/tcl3guard' + process.pid);

// --- the worker half, run in a forked child against the parent's arena ------
//
// A DELETE THIS PROCESS ISSUED MUST NOT BE UNDONE BY L3. On a worker the
// removal is applied by the primary a tick later, so until then the key is gone
// locally, still present in L2 -- and still present in L3, which has not seen
// the delete either. Reading through and promoting that value reinstates data
// this process removed after being told the delete succeeded. `get` already
// answers undefined for such a key; the async form must agree, or the two
// disagree about a delete the caller made itself.
if (process.env.TCG_ROLE === 'worker') {
    (async () => {
        const f = makeFake();
        f.store.set('gone', { value: 'L3VALUE', expiresAt: 0 });
        f.store.set('never-local', { value: 'L3VALUE', expiresAt: 0 });
        const w = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l3: f.adapter });

        // 0. the ordinary worker promotion, first: a worker cannot write L2
        //    directly, so its promotion goes through the submission ring like
        //    any other write. The parent checks L2 afterwards.
        f.store.set('promote', { value: 'L3VALUE', expiresAt: 0 });
        const p = await w.getAsync('promote');
        ok(p === 'L3VALUE', `the worker reads through to L3 (${p})`);
        ok(w.get('promote') === 'L3VALUE', `and has the value locally (${w.get('promote')})`);

        // 1. deleted, then read: the delete is known before the read starts.
        ok(w.get('gone') === 'L2VALUE', `the worker reads the key from L2 first (${w.get('gone')})`);
        // Held in flight so L3 still holds 'gone' while the assertions below
        // run -- otherwise the background delete could win the race and land
        // before hasAsync/getAsync even ask, which would pass even with the
        // #pendingDel guard missing and prove nothing.
        f.latency.set('delete', 5000);
        ok(w.delete('gone') === true, 'the worker deletes it');
        ok(w.get('gone') === undefined, `the sync form reports it gone (${w.get('gone')})`);
        const got = await w.getAsync('gone');
        ok(got === undefined, `the async form agrees rather than resurrecting it (${got})`);
        ok(w.get('gone') === undefined, `nothing was promoted back into L1 (${w.get('gone')})`);
        ok(f.calls.filter(x => x[0] === 'get' && x[1] === 'gone').length === 0,
           'and L3 was never even asked for a key this process had deleted');
        // hasAsync must agree with the same guard: L3 still holds 'gone' (the
        // queued delete has not necessarily landed there yet), so without the
        // #pendingDel check hasAsync would report a key this caller was just
        // told is gone as still present.
        const hasGot = await w.hasAsync('gone');
        ok(hasGot === false, `hasAsync agrees too, rather than reporting a deleted key present (${hasGot})`);
        ok(f.calls.filter(x => x[0] === 'has' && x[1] === 'gone').length === 0,
           'and L3 was never even asked has() for a key this process had deleted');

        // 2. the delete lands WHILE the read is in flight, so the early-out
        //    above cannot help and there may be no ring record yet either.
        f.latency.set('get', 60);
        // The delete is pushed to L3 in the background too, and the fake samples
        // its store when the read completes. Held in flight so L3 still answers
        // with the value -- otherwise the read is an ordinary miss and proves
        // nothing about the guard.
        f.latency.set('delete', 5000);
        const read = w.getAsync('never-local');
        await sleep(10);
        w.delete('never-local');
        const got2 = await read;
        ok(got2 === undefined, `a delete during the read wins over L3's value (${got2})`);
        ok(w.get('never-local') === undefined, `and nothing was promoted (${w.get('never-local')})`);
        ok(w.stats.l3DeletedWhileReading === 1,
           `the prevented resurrection is counted (${w.stats.l3DeletedWhileReading})`);

        w.close();
        console.log(fail ? `  ${fail} failed (worker)` : '  [l3-guard] worker cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

(async () => {
    // A slow L3 read, with a local write landing while it is in flight.
    {
        const f = makeFake();
        f.store.set('k', { value: 'OLD', expiresAt: 0 });
        f.latency.set('get', 60);
        // The local write below is ALSO pushed to L3 in the background, and the
        // fake samples its store when the read completes rather than when it
        // started -- so without this the write-through lands first and L3
        // answers NEW, testing nothing. Holding the L3 write in flight is also
        // what the real race looks like: the write is local-first, and L3 is
        // still answering with the older value while it is on its way there.
        f.latency.set('set', 5000);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });

        const read = c.getAsync('k');                 // starts, will return OLD
        await new Promise(r => setTimeout(r, 10));
        c.set('k', 'NEW');                            // lands while the read is in flight

        const got = await read;
        ok(got === 'OLD', `the caller still receives what L3 said (${got})`);
        ok(c.get('k') === 'NEW', `the newer local value is NOT overwritten (${c.get('k')})`);
        ok(c.stats.l3PromotionsBlocked === 1, `the blocked promotion is counted (${c.stats.l3PromotionsBlocked})`);
        c.close();
    }

    // A clear while a read is in flight must block the promotion too: the ring
    // records a flush marker rather than the key's own hash, so a guard that
    // only compares hashes would let the cleared value straight back in.
    {
        const f = makeFake();
        f.store.set('c', { value: 'OLD', expiresAt: 0 });
        f.latency.set('get', 60);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const read = c.getAsync('c');
        await new Promise(r => setTimeout(r, 10));
        c.clearAll();
        await read;
        ok(c.get('c') === undefined, `a clear during the read blocks the promotion (${c.get('c')})`);
        c.close();
    }

    // The ring is read in BATCHES, not truncated. ringRead returns at most the
    // records asked for, so a guard that made one call for 1024 of the 8192 a
    // ring holds would miss an invalidation sitting past that batch and promote
    // straight over it -- the defect this guard exists to prevent, merely
    // harder to reproduce. The control key proves the ring did not simply wrap
    // (which blocks everything, and would pass the first assertion for the
    // wrong reason).
    {
        const f = makeFake();
        f.store.set('far', { value: 'OLD', expiresAt: 0 });
        f.store.set('ctl', { value: 'CTL', expiresAt: 0 });
        f.latency.set('get', 60);
        f.latency.set('set', 5000);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const far = c.getAsync('far');
        const ctl = c.getAsync('ctl');
        await new Promise(r => setTimeout(r, 10));
        for (let i = 0; i < 1100; i++) c.set('noise' + i, 'x');   // more than one batch
        c.set('far', 'NEW');                                      // past the first 1024
        await far; await ctl;
        ok(c.get('far') === 'NEW', `an invalidation past the first batch still blocks (${c.get('far')})`);
        ok(c.get('ctl') === 'CTL', `a key nobody touched is still promoted (${c.get('ctl')})`);
        c.close();
    }

    // A key the arena cannot hash is returned but never promoted. `get` already
    // treats a lone surrogate as never stored, so an L1 entry under it is
    // unreachable -- and it would be filed under the hash `undefined`, which
    // every such key would then share.
    {
        const f = makeFake();
        const K = 'a\uD800b';
        f.store.set(K, { value: 'v', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync(K) === 'v', 'an unhashable key still reads through to L3');
        ok(c.l1Size === 0, `and is not promoted (l1Size ${c.l1Size})`);
        // Counted apart from a blocked promotion on purpose. "This key changed
        // while I was reading it" is ordinary contention an operator expects to
        // see; "this key can never live in L1 or L2" is a different event
        // entirely, and mixing them makes the first number unreadable.
        ok(c.stats.l3UnhashableKeys === 1, `the unhashable key is counted separately (${c.stats.l3UnhashableKeys})`);
        ok(c.stats.l3PromotionsBlocked === undefined,
           `and NOT as a blocked promotion (${c.stats.l3PromotionsBlocked})`);
        c.close();
    }

    // The worker cases need a real arena with a real primary, because
    // #pendingDel only exists on a worker: its delete is applied by the primary
    // a tick later, so until the invalidation comes back the key is gone
    // locally but still present in L2 -- and in L3.
    {
        const primary = TurboKV.createPrimary(ARENA, 16 << 20, 1 << 14, { storage: 'bytes' });
        primary.set('gone', 'L2VALUE');
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCG_ROLE: 'worker', TCG_ARENA: ARENA }, stdio: 'inherit',
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the worker cases passed (child exited ${code})`);
        // Checked from the PRIMARY, because that is where the damage would be
        // visible: a promotion from a worker travels through the submission
        // ring, so a resurrected value lands in L2 after the delete and every
        // process in the cluster then sees the key the worker removed. The
        // worker's own `get` cannot show this -- it answers undefined for a
        // pending delete whatever L2 holds.
        await sleep(50);
        TurboKV.drainSubmissions(8192);
        ok(primary.get('gone') === undefined,
           `the worker's delete still stands in L2 afterwards (${primary.get('gone')})`);
        ok(primary.get('promote') === 'L3VALUE',
           `a worker's legitimate promotion did reach L2 through the ring (${primary.get('promote')})`);
        primary.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-guard] all passed');
    process.exit(fail ? 1 : 0);
})();
