'use strict';
// THE PROMOTION GUARD.
//
// A read that started before a write can return the older value after it. If
// that value is promoted, it overwrites the newer one in L2 -- not stale,
// WRONG, and it stays wrong until the next write to that key. The guard marks
// the invalidation ring before the await and refuses to promote if the key was
// invalidated while the read was in flight.
const { TurboKV } = require('../src/turbokv');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

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
        ok(c.stats.l3PromotionsBlocked === 1, `the block is counted (${c.stats.l3PromotionsBlocked})`);
        c.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-guard] all passed');
    process.exit(fail ? 1 : 0);
})();
