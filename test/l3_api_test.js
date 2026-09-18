'use strict';
// The async API against a fake L3. The rule under test throughout: a sync
// method and its async twin have IDENTICAL effects; the async one only lets
// the caller wait for the L3 half.
const { TurboKV } = require('../src/turbokv');
const { makeFake, delay } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

(async () => {
    // 1. setAsync writes locally AND to L3, and resolves on the L3 outcome
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.setAsync('k', 'v') === true, 'setAsync resolves true');
        ok(c.get('k') === 'v', 'the value is in the local tiers immediately');
        ok(f.store.get('k').value === 'v', 'the value reached L3');
        c.close();
    }

    // 2. sync set reaches L3 too, just without waiting
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(c.set('k2', 'v2') === true, 'sync set still returns a boolean');
        ok(f.store.get('k2') === undefined, 'the L3 write has not happened yet');
        await c.drainL3();
        ok(f.store.get('k2').value === 'v2', 'the L3 write happens in the background');
        c.close();
    }

    // 3. a failed L3 write keeps the local value, resolves false, and is counted
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30, l3FailTtlMs: 5000 });
        ok(await c.setAsync('k3', 'v3') === false, 'a failed L3 write resolves false');
        ok(c.get('k3') === 'v3', 'the local cache keeps working during an L3 outage');
        ok(c.stats.l3SetFailed === 1, `the failure is counted (${c.stats.l3SetFailed})`);
        c.close();
    }

    // 4. background failures do NOT write lastError
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const errs = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 20,
                                onL3Error: (e) => errs.push(e.message) });
        c.set('k4', 'v4');                       // sync: returns before the failure
        c.set('this key is way too long'.repeat(200), 'x');   // a real, synchronous rejection
        const afterSync = c.lastError;
        await delay(120);
        ok(c.lastError === afterSync,
           `a background L3 failure does not overwrite lastError (${c.lastError})`);
        ok(errs.length > 0, 'the error listener hears about it instead');
        c.close();
    }

    // 5. minLevel L3 does not store locally, and a failed L3 write stores nothing
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.setAsync('k5', 'v5', { minLevel: TurboKV.L3 }) === true, 'minLevel L3 write succeeds');
        ok(c.l1Size === 0, 'nothing was put in L1');
        ok(f.store.get('k5').value === 'v5', 'the value is in L3');
        ok(f.calls.some(x => x[0] === 'set' && x[1] === 'k5' && x[2] === false),
           'the adapter is told willCache:false');
        c.close();
    }

    // 6. without an adapter the async API still works, stopping at L2
    {
        const c = TurboKV.open({ storage: 'bytes' });
        ok(await c.setAsync('k6', 'v6') === true, 'setAsync works with no adapter');
        ok(c.get('k6') === 'v6', 'and stores locally');
        c.close();
    }

    // 7. getAsync reads through to L3 and fills the local tiers
    {
        const f = makeFake();
        f.store.set('r1', { value: 'from-l3', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('r1') === 'from-l3', 'getAsync reads through to L3');
        ok(c.get('r1') === 'from-l3', 'and the value is now local');
        ok(c.stats.l3Hits === 1, `L3 hits are counted (${c.stats.l3Hits})`);
        c.close();
    }

    // 8. concurrent misses on one key share a single L3 request
    {
        const f = makeFake();
        f.store.set('r2', { value: 'v', expiresAt: 0 });
        f.latency.set('get', 30);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const all = await Promise.all([c.getAsync('r2'), c.getAsync('r2'), c.getAsync('r2')]);
        ok(all.every(v => v === 'v'), 'every caller gets the value');
        const gets = f.calls.filter(x => x[0] === 'get').length;
        ok(gets === 1, `concurrent misses share one request (made ${gets})`);
        c.close();
    }

    // 9. minLevel L3 returns the value without storing it anywhere local
    {
        const f = makeFake();
        f.store.set('r3', { value: 'v', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('r3', { minLevel: TurboKV.L3 }) === 'v', 'the value is returned');
        ok(c.l1Size === 0 && c.get('r3') === undefined, 'nothing was stored locally');
        c.close();
    }

    // 10. a miss and a failed read are both misses, and the read failure is
    //     reported through the ONE listener path -- exactly once
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('absent') === undefined, 'a key L3 does not have is undefined');
        ok(c.stats.l3Misses === 1, `the miss is counted (${c.stats.l3Misses})`);
        c.close();
    }
    {
        const f = makeFake();
        f.fail.set('get', new Error('L3 read down'));
        const errs = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter,
                                onL3Error: (e, op) => errs.push(op.kind) });
        ok(await c.getAsync('r4') === undefined, 'a failed L3 read is a miss, not a throw');
        ok(c.stats.l3Misses === 1, `the failed read is counted as a miss (${c.stats.l3Misses})`);
        ok(errs.length === 1 && errs[0] === 'get',
           `the read failure is reported once, as a get (${errs.join(',')})`);
        ok(c.lastError === null, `a background L3 failure still does not touch lastError (${c.lastError})`);
        c.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-api] all passed');
    process.exit(fail ? 1 : 0);
})();
