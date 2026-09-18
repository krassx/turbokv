// Regressions for the second adversarial review (three agents: never-reviewed
// code, native races/memory, API/packaging/tests).
//
// Every case here drives the PUBLIC path in the configuration that actually
// ships, because the recurring failure in this repo has been tests that
// exercise a helper or a non-default mode and so keep passing while the shipped
// path regresses.
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
let fails = 0, n = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fails++; };
const mk = (o) => TurboKV.createPrimary('/tcr2_' + process.pid + '_' + (n++), 16 << 20, 1 << 14,
    { storage: 'bytes', l1MaxBytes: 1 << 18, ...o });

// A degraded handle must be observable. `lastError` was the only signal and the
// next write overwrote it with "submission ring full", blaming backpressure for
// a dead primary.
{
    const c = mk({});
    ok(c.primaryDead === false, 'primaryDead is exposed and false while healthy');
    c.close();
}

// SweepExpired was the one expiry comparison left non-wrap-aware; it deleted
// live entries whose expiry crossed the uint32 wrap. Covered natively in
// native_regression_test.cc; here we only pin that a normal TTL still survives
// a sweep, which is the property that fix must not break.
{
    const c = mk({});
    c.set('keep', 'v', { ttlMs: 60000 });
    c.set('plain', 'v');
    native.sweepExpired(0, 1 << 16);
    c.clearLocal();
    ok(c.get('keep') === 'v', 'a sweep does not evict an unexpired TTL entry');
    ok(c.get('plain') === 'v', 'a sweep does not evict an entry with no TTL');
    c.close();
}

// The API promises set() never throws and returns a boolean. It threw for a
// Symbol key (`this.#ns + key`), for an object whose toString throws, and for an
// options object whose ttlMs getter throws -- all caller-supplied code running
// inside a method documented not to throw. Non-string keys were also coerced
// silently, so set(undefined, v) stored under 'undefined' and every plain object
// aliased to '[object Object]'.
{
    const c = mk({});
    const noThrow = (fn) => { try { return { v: fn() }; } catch (e) { return { threw: e.constructor.name }; } };
    for (const [label, fn] of [
        ['Symbol key', () => c.set(Symbol('s'), 'v')],
        ['key whose toString throws', () => c.set({ toString() { throw new Error('x'); } }, 'v')],
        ['options whose ttlMs getter throws', () => c.set('k', 'v', { get ttlMs() { throw new Error('x'); } })],
        ['undefined key', () => c.set(undefined, 'v')],
        ['plain object key', () => c.set({}, 'v')],
    ]) {
        const r = noThrow(fn);
        ok(r.threw === undefined && r.v === false, `set: ${label} -> false, not a throw (${JSON.stringify(r)})`);
    }
    ok(noThrow(() => c.get(Symbol('s'))).v === undefined, 'get: a Symbol key is a miss, not a throw');
    ok(noThrow(() => c.has(Symbol('s'))).v === false, 'has: a Symbol key is false, not a throw');
    ok(noThrow(() => c.delete(Symbol('s'))).v === false, 'delete: a Symbol key is false, not a throw');
    c.set('real', 'v');
    ok(c.get('real') === 'v', 'a normal string key still works');
    c.close();
}

// Arena capacity must not be quantised to powers of two. The log used to mask
// offsets with (dataBytes-1), so the data region was rounded DOWN to a power of
// two and a 24MB, 26MB, 28MB or 32MB request all yielded exactly 16MB of data -
// capacity could be doubled but never tuned, and DESIGN 7's formulas named
// numbers nobody actually got.
{
    const seen = [];
    for (const mb of [24, 26, 28, 32]) {
        const c = TurboKV.createPrimary('/tcquant' + process.pid + '_' + mb, mb << 20, 1 << 14, { storage: 'bytes' });
        const data = native.stats().dataBytes;
        seen.push({ mb, data, frac: data / (mb << 20) });
        c.close();
    }
    for (const r of seen) {
        ok(r.frac > 0.9, `a ${r.mb}MB arena yields ${(r.data / 1048576).toFixed(1)}MB of data (${(100 * r.frac).toFixed(0)}% of it)`);
    }
    const distinct = new Set(seen.map(r => r.data)).size;
    ok(distinct === seen.length, `each requested size gives a distinct capacity (${distinct}/${seen.length} distinct)`);
}

// And the log must still be correct when dataBytes is not a power of two.
{
    const c = TurboKV.createPrimary('/tcquant2' + process.pid, 26 << 20, 1 << 15, { storage: 'bytes' });
    const N = 40000, val = (i) => 'v'.repeat(180) + i;
    for (let i = 0; i < N; i++) c.set('k' + i, val(i));
    c.clearLocal();
    let wrong = 0, found = 0;
    for (let i = 0; i < N; i++) { const v = c.get('k' + i); if (v === undefined) continue; found++; if (v !== val(i)) wrong++; }
    ok(wrong === 0, `no wrong values across a non-power-of-two log with wrapping (${found} readable, ${wrong} wrong)`);
    ok(native.stats().evictions > 0, 'the run actually wrapped the log');
    c.close();
}

console.log(fails ? `\n  ${fails} FAILED` : '\n  all passed');
process.exit(fails ? 1 : 0);
