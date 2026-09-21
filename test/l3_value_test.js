'use strict';
// WHAT A READ-THROUGH HANDS BACK, AND WHAT IT STORES.
//
// Three rules meet on this path and getAsync obeyed none of them:
//
//   - decision 7: binary values are COPIED, because they are mutable and the
//     cache hands the same entry to every caller in the process. getAsync
//     returned the object it had just put in L1 -- which was also the adapter's
//     own stored object, so one caller's mutation corrupted the cache AND the
//     fake L3 behind it.
//   - decision 26: a codec-mode value the cache owns is FROZEN, so a mutation
//     raises instead of silently corrupting a shared object. get() froze its L2
//     return; getAsync did not, so the same value was mutable or not depending
//     on which form fetched it.
//   - an adapter's value is untrusted input. It was written into the SHARED
//     arena before the decode that would have rejected it, so one bad reply
//     poisoned a key for every process on the box: sync get() threw the decode
//     error on every read until the entry was gone.
//
// And decision 64's family: a promotion on the primary writes L2 with writer id
// 0, which the primary's own invalidation pass skips, so a sibling cache
// instance in the same process kept serving its stale L1 copy forever.
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

(async () => {
    // 1. BYTES MODE, BINARY VALUE: copied on the way in and on the way out.
    {
        const f = makeFake();
        const buf = Buffer.from('abc');
        f.store.set('b', { value: buf, expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const v = await c.getAsync('b');
        ok(Buffer.isBuffer(v), 'a binary L3 value comes back as a Buffer');
        ok(v !== buf, 'and is NOT the adapter\'s own object');
        v[0] = 0x58;                                     // the caller mutates its result
        ok(c.get('b').toString() === 'abc', `mutating it does not corrupt L1 (${c.get('b')})`);
        ok(native.get('b').toString() === 'abc', `nor the shared arena (${native.get('b')})`);
        ok(buf.toString() === 'abc', `nor the adapter's store (${buf})`);
        // And the second caller does not get the first caller's object either.
        const w = await c.getAsync('b');
        const x = c.get('b');
        ok(w !== x, 'two reads of the same key never share one Buffer');
        await c.close();
    }

    // 2. DIRECT MODE: the async form freezes exactly as the sync form does.
    {
        const f = makeFake();
        const enc = TurboKV.V8_CODEC.encode({ n: 1, inner: { m: 2 } });
        f.store.set('o', { value: enc, expiresAt: 0 });
        const c = TurboKV.open({ storage: 'direct', l3: f.adapter });
        const v = await c.getAsync('o');
        ok(Object.isFrozen(v), 'the getAsync result is frozen');
        ok(Object.isFrozen(v.inner), 'deeply');
        ok(Object.isFrozen(c.get('o')), 'and so is the copy L1 now holds');
        let threw = false;
        try { v.n = 99; } catch { threw = true; }
        ok(threw, 'mutating it raises rather than corrupting the cached object');
        ok(c.get('o').n === 1, `the cached value is unchanged (${c.get('o').n})`);
        await c.close();
    }

    // 3. SAFE MODE decodes per read, so each caller gets a fresh mutable object
    //    -- from getAsync exactly as from get.
    {
        const f = makeFake();
        f.store.set('s', { value: JSON.stringify({ n: 1 }), expiresAt: 0 });
        const c = TurboKV.open({ storage: 'safe', l3: f.adapter });
        const a = await c.getAsync('s');
        const b = c.get('s');
        ok(a.n === 1 && b.n === 1, 'both forms decode the value');
        ok(a !== b, 'safe mode still hands out a fresh object per read');
        ok(!Object.isFrozen(a), 'and does not freeze it');
        a.n = 99;
        ok(c.get('s').n === 1, `so a mutation cannot reach the cache (${c.get('s').n})`);
        await c.close();
    }

    // 4. A VALUE THE CACHE CANNOT STORE never reaches the arena.
    {
        const f = makeFake();
        f.store.set('p', { value: Buffer.from('not a v8 blob'), expiresAt: 0 });
        let reported = null;
        const c = TurboKV.open({ storage: 'direct', l3: f.adapter, onL3Error: (e, op) => { reported = op.kind; } });
        let threw = null, got;
        try { got = await c.getAsync('p'); } catch (e) { threw = e.message; }
        ok(threw === null, `getAsync does not throw at the caller (${threw})`);
        ok(got === undefined, `it is a miss, like any other adapter failure (${got})`);
        ok(native.get('p') === undefined, `and NOTHING was written into the shared arena (${native.get('p')})`);
        ok(c.l1Size === 0, `nor into L1 (${c.l1Size})`);
        ok(reported === 'get', `the failure is reported through onL3Error (${reported})`);
        ok(c.stats.l3BadValues === 1, `and counted apart from an ordinary miss (${c.stats.l3BadValues})`);
        // The whole point: the key is not poisoned for anyone.
        let syncThrew = null;
        try { c.get('p'); } catch (e) { syncThrew = e.message; }
        ok(syncThrew === null, `sync get of that key does not throw (${syncThrew})`);
        await c.close();
    }

    // 5. The same in bytes mode, where the arena silently refused the value and
    //    L1 kept an object `set()` would have rejected outright.
    {
        const f = makeFake();
        f.store.set('q', { value: { a: 1 }, expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const got = await c.getAsync('q');
        ok(got === undefined, `an object in bytes mode is a miss (${JSON.stringify(got)})`);
        ok(c.l1Size === 0, `and does not land in L1 either (${c.l1Size})`);
        ok(c.get('q') === undefined, 'so the sync form agrees');
        await c.close();
    }

    // 6. ttlMs FROM THE ADAPTER IS UNTRUSTED. A naive PTTL passthrough answers
    //    -1 for "no expiry", which used to be taken as a TTL, min'd to -1, and
    //    stored as NO EXPIRY AT ALL -- so the l3TtlMs bound was bypassed by the
    //    one reply it exists to bound.
    {
        for (const bad of [-1, -2, 0, NaN, Infinity, '500', null, undefined]) {
            const f = makeFake();
            f.adapter.get = async () => ({ value: 'v', ttlMs: bad });
            const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3TtlMs: 60000 });
            ok(await c.getAsync('t') === 'v', `ttlMs ${String(bad)}: the value still reads through`);
            native.get('t');
            const rem = native.lastTtlRemainingMs();
            ok(rem > 55000 && rem <= 60000,
               `ttlMs ${String(bad)}: the arena entry carries l3TtlMs, not no-expiry (${rem})`);
            await c.close();
        }
    }

    // 7. A REAL ttl is still honoured, and still capped by l3TtlMs.
    {
        const f = makeFake();
        f.adapter.get = async () => ({ value: 'v', ttlMs: 3000 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3TtlMs: 60000 });
        await c.getAsync('short');
        native.get('short');
        const rem = native.lastTtlRemainingMs();
        ok(rem > 2000 && rem <= 3000, `a shorter ttl from L3 wins (${rem})`);
        await c.close();
    }
    {
        const f = makeFake();
        f.adapter.get = async () => ({ value: 'v', ttlMs: 999999 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3TtlMs: 5000 });
        await c.getAsync('long');
        native.get('long');
        const rem = native.lastTtlRemainingMs();
        ok(rem > 4000 && rem <= 5000, `a longer ttl from L3 is capped by l3TtlMs (${rem})`);
        await c.close();
    }

    // 8. A SIBLING INSTANCE IN THE SAME PROCESS. decision 64's third call site.
    //
    //    B reads the key, so it holds it in L1. The key then leaves L2 the way
    //    an eviction or an expiry removes it -- a primary-side removal, whose
    //    ring record the primary's own invalidation pass skips, so neither
    //    instance drops anything. A reads through to L3 and promotes a NEWER
    //    value into the shared arena. B must not go on answering with the value
    //    it cached before.
    {
        const f = makeFake();
        const a = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const b = TurboKV.open({ storage: 'bytes' });
        await a.setAsync('sib', 'OLD');
        f.store.set('sib', { value: 'NEW', expiresAt: 0 });   // someone else moved it on
        ok(b.get('sib') === 'OLD', `the sibling caches the old value (${b.get('sib')})`);
        native.del('sib', 0);                  // gone from L2; both L1s still hold it
        a.clearLocal();                        // only A forgets, so A's read reaches L3
        ok(b.get('sib') === 'OLD', `and still serves it from L1 (${b.get('sib')})`);
        const got = await a.getAsync('sib');
        ok(got === 'NEW', `the promotion reads NEW from L3 (${got})`);
        ok(native.get('sib') === 'NEW', `and writes it to the shared arena (${native.get('sib')})`);
        ok(b.get('sib') === 'NEW',
           `the sibling follows the promotion rather than serving its stale copy (${b.get('sib')})`);
        await a.close(); await b.close();
    }

    // 9. The same for the l3FailTtlMs cap, which is the fourth call site: it
    //    SHORTENS an entry's life in L2, and a sibling holding the entry with
    //    its original (absent) expiry would serve it straight past the cap.
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const a = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30, l3FailTtlMs: 200 });
        const b = TurboKV.open({ storage: 'bytes' });
        // NOT awaited: the sibling has to take its copy while the arena entry
        // still has no expiry, which is the whole window the cap closes. By the
        // time setAsync's promise resolves the cap has already been applied.
        a.set('cap', 'V');
        ok(b.get('cap') === 'V', `the sibling caches it (${b.get('cap')})`);
        await new Promise(r => setTimeout(r, 120));      // let the write be abandoned
        ok(a.stats.l3FailTtlApplied === 1, `the cap was applied (${a.stats.l3FailTtlApplied})`);
        native.get('cap');
        ok(native.lastTtlRemainingMs() > 0, 'the arena entry now expires');
        await new Promise(r => setTimeout(r, 250));
        ok(b.get('cap') === undefined,
           `and the sibling converges with it rather than serving past the cap (${b.get('cap')})`);
        await a.close(); await b.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-value] all passed');
    process.exit(fail ? 1 : 0);
})();
