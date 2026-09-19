'use strict';
// THE HERD MAP IS NOT A WAY AROUND THE CLEAR GUARD.
//
// getAsync shares one L3 request among simultaneous misses on a key, so the
// herd is bounded by the number of processes rather than the request rate. That
// sharing happened BEFORE the "is a clear still landing" check, so a read
// issued AFTER clearAll() was handed a promise created for a read issued before
// it -- a promise whose own guard ran when there was no clear to see. The
// caller got the pre-clear value while `get` for the same key answered
// undefined, which is decision 70's guarantee undone by nothing more than
// arriving second.
//
// The same guard had a second hole behind it. On a DEGRADED worker the
// promotion guard bailed out with 'l3PromotionsBlocked' -- which blocks where
// the value is stored and still returns it -- before it ever consulted the
// process-wide clear counter, even though that counter is the only clear guard
// a degraded worker has left. So the read the herd was sharing returned the
// cleared value in the first place.
const { TurboKV } = require('../src/turbokv');
const { makeFake, delay } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

(async () => {
    // 1. A degraded worker: its own clearAll is the only thing that can say a
    //    clear is outstanding, and the read in flight must obey it.
    {
        const f = makeFake();
        f.store.set('k', { value: 'v', expiresAt: 0 });
        f.latency.set('get', 60); f.latency.set('clear', 400);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.__unsafeForcePrimaryDead();
        const first = c.getAsync('k');               // in flight when the clear arrives
        await delay(10);
        c.clearAll();
        const joiner = c.getAsync('k');              // same level: joins `first`
        // minLevel L2 collapses to L1 on a degraded handle, so this joins too
        // -- which is exactly how the finding was reached.
        const other = c.getAsync('k', { minLevel: TurboKV.L2 });
        const a = await first, b = await joiner, d = await other;
        ok(a === undefined, `the read in flight when the clear was issued misses (${a})`);
        ok(b === undefined, `a read issued after clearAll misses rather than joining a pre-clear answer (${b})`);
        ok(d === undefined, `and so does one at another level (${d})`);
        ok(c.get('k') === undefined, `the sync form agrees throughout (${c.get('k')})`);
        ok(c.stats.l3ClearedWhileReading === 1,
           `the in-flight read is counted as blocked by a clear (${c.stats.l3ClearedWhileReading})`);
        await c.drainL3();
        await c.close();
    }

    // 2. The same on a healthy primary, where the arena's generation is also
    //    watching: the joiner must miss, and it must not be answered from the
    //    shared promise at all.
    {
        const f = makeFake();
        f.store.set('k', { value: 'v', expiresAt: 0 });
        f.latency.set('get', 60); f.latency.set('clear', 400);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const order = [];
        const first = c.getAsync('k').then((v) => { order.push('first'); return v; });
        await delay(10);
        c.clearAll();
        const joiner = c.getAsync('k').then((v) => { order.push('joiner'); return v; });
        ok(await joiner === undefined, 'the post-clear read misses');
        // AND IT MISSES AT ONCE. Joining made the post-clear caller wait for a
        // request issued before the clear, and take that request's answer --
        // which is the coupling this guard exists to break, whatever the two
        // happen to agree on. `first` is 60ms of adapter latency away.
        ok(order[0] === 'joiner',
           `refused immediately rather than joined to the pre-clear request (${JSON.stringify(order)})`);
        ok(c.stats.l3Misses === 1, `and counted as a miss in its own right (${c.stats.l3Misses})`);
        ok(await first === undefined, 'the pre-clear read misses too');
        // ONE adapter get, not two: the post-clear read is refused outright, so
        // it neither joins nor starts a second request.
        const gets = f.calls.filter(x => x[0] === 'get' && x[1] === 'k').length;
        ok(gets === 1, `the refused read never reaches the adapter (${gets} get calls)`);
        await c.drainL3();
        await c.close();
    }

    // 3. ONCE THE CLEAR HAS LANDED, sharing works exactly as before -- the
    //    guard must not become a permanent refusal, and the herd must still be
    //    one request for many callers.
    {
        const f = makeFake();
        f.latency.set('get', 40);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        await c.clearAsync();
        f.store.set('after', { value: 'A', expiresAt: 0 });
        const many = await Promise.all([c.getAsync('after'), c.getAsync('after'), c.getAsync('after')]);
        ok(many.every(v => v === 'A'), `every caller is served (${JSON.stringify(many)})`);
        const gets = f.calls.filter(x => x[0] === 'get' && x[1] === 'after').length;
        ok(gets === 1, `from a single shared request (${gets} get calls)`);
        await c.close();
    }

    // 4. hasAsync already asked first and must keep doing so.
    {
        const f = makeFake();
        f.store.set('h', { value: 'v', expiresAt: 0 });
        f.latency.set('clear', 300);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.clearAll();
        ok(await c.hasAsync('h') === false, 'hasAsync misses while a clear is landing');
        await c.drainL3();
        await c.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-joiner] all passed');
    process.exit(fail ? 1 : 0);
})();
