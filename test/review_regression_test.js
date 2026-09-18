// Regression tests for every defect the adversarial review found.
const { Cache, TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const __native = native;
let fail = 0, n = 0;
const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const mk = o => TurboKV.createPrimary('/tcrr' + process.pid + '_' + (n++), 8 << 20, 1 << 16,
    { storage: 'bytes', l1MaxBytes: 32 * 1024, ...o });

// 1. long / non-latin1 keys must never collide or truncate
{
    const c = mk({});
    const A = 'K'.repeat(511) + 'A', B = 'K'.repeat(511) + 'B';
    c.set(A, 'value-A'); c.set(B, 'value-B'); c.clearLocal();
    ok(c.get(A) === 'value-A' && c.get(B) === 'value-B', '511+ byte keys stay distinct');
    ok(c.get('K'.repeat(511) + 'ZZZ') === undefined, 'never-set long key is a miss');
    c.set('ā', 'a-macron'); c.set('ȁ', 'a-double-grave'); c.clearLocal();
    ok(c.get('ā') === 'a-macron' && c.get('ȁ') === 'a-double-grave', 'non-latin1 keys distinct');
    c.set('中', 'cjk'); c.clearLocal();
    ok(c.get('-') === undefined, 'CJK key does not alias an ASCII key');
    ok(c.set('x'.repeat(2000), 'v') === false, 'over-long key rejected, not truncated');
    __native.destroy();
}
// 2. index saturation must not brick the arena
{
    const c = mk({});
    const val = 'v'.repeat(200);
    for (let i = 0; i < 60000; i++) c.set('hot' + (i % 3), val);
    for (let i = 0; i < 40; i++) c.set('new' + i, val);
    let sets = 0; for (let i = 0; i < 50; i++) if (c.set('fresh' + i, val)) sets++;
    c.clearLocal();
    let readable = 0; for (let i = 0; i < 50; i++) if (c.get('fresh' + i) !== undefined) readable++;
    const s = native.stats();
    ok(sets === 50 && readable === 50, `arena still usable after churn (sets=${sets} readable=${readable})`);
    ok(s.logTail <= s.logHead, 'tail never overshoots head');
    __native.destroy();
}
// 3. TTL must survive an L1 refill from L2
{
    const c = mk({});
    c.set('t', 'v', { ttlMs: 50 });
    let u = Date.now() + 120; while (Date.now() < u);
    ok(c.get('t') === undefined && c.has('t') === false, 'TTL enforced after L1 refill from L2');
    __native.destroy();
}
// 4/5. the primary's L1 must follow applied worker batches
{
    const c = mk({});
    c.set('k', 'from-primary');
    TurboKV.applyBatch({ t: 'tc', id: 1, b: ['s', 'k', 'from-worker', 0] });
    ok(c.get('k') === 'from-worker', 'primary L1 sees a worker set');
    TurboKV.applyBatch({ t: 'tc', id: 1, b: ['d', 'k', null, 0] });
    ok(c.get('k') === undefined, 'primary L1 sees a worker delete');
    __native.destroy();
}
// 8. a failed set must not destroy the previous value
{
    const c = mk({});
    c.set('keep', 'original');
    ok(c.set('keep', 'y'.repeat(200 * 1024 * 1024)) === false, 'oversized value rejected');
    ok(c.get('keep') === 'original', 'failed set left the previous value intact');
    __native.destroy();
}
// 10. no crash before an arena exists or after close
{
    const c = mk({}); c.close();
    ok(native.get('anything') === undefined, 'native get after destroy does not crash');
}
// sev3: ttl clamp
{
    const c = mk({});
    c.set('big', 'v', { ttlMs: 2147483600 });
    ok(c.has('big') === true, 'huge ttlMs does not overflow into instant expiry');
    __native.destroy();
}
ok(typeof Cache === 'function', 'Cache alias is exported as the docs describe');
// worker and primary must agree on delete() of an absent key
{
    const c = mk({});
    ok(c.delete('never-existed') === false, 'delete of an absent key reports false');
    __native.destroy();
}
// worker id 0 must be rejected, not silently wedge the process.
// `#id === 0` is how every method recognises the primary, so a worker attached
// as 0 takes the primary's write path against a read-only mapping and blocks
// the event loop forever on its first set() -- no throw, no crash, no log.
{
    // Attach against a segment that EXISTS, so the only thing that can reject is
    // the id itself. Pointing at a nonexistent segment made this vacuous:
    // native.attach throws for any id, so `threw === 6` held even with the
    // validation deleted -- verified by deleting it, and the test still passed.
    const live = '/tcwid' + process.pid;
    const primary = TurboKV.createPrimary(live, 8 << 20, 1 << 14, { storage: 'bytes' });
    let threw = 0, messages = 0;
    for (const bad of [0, -1, 1.5, null, undefined, 'x']) {
        try { TurboKV.attachWorker(live, bad); }
        catch (e) { threw++; if (/workerId must be/.test(e.message)) messages++; }
    }
    ok(threw === 6, 'attachWorker rejects every non-positive-integer workerId');
    ok(messages === 6, 'each rejection is the workerId check, not an attach failure');
    primary.close();
    let coerced = false;
    try { TurboKV.attachWorker('/tcnope' + process.pid, '3'); } catch (e) { coerced = !/workerId must be/.test(e.message); }
    ok(coerced, "numeric string workerId '3' is coerced, not rejected");
}

// Native-layer defects found by the second adversarial review.
{
    const c = mk({});

    // Unpaired surrogates all encode to U+FFFD, so distinct keys aliased and
    // returned each other's values.
    c.set('\uFFFD', 'real-replacement-char');
    ok(c.set('\uD800', 'x') === false, 'lone high surrogate key is rejected');
    ok(c.set('\uDC00', 'x') === false, 'lone low surrogate key is rejected');
    ok(c.get('\uD800') === undefined, 'lone surrogate key does not alias U+FFFD');
    ok(c.get('\uFFFD') === 'real-replacement-char', 'a genuine U+FFFD key still works');

    // These dereferenced the store before an arena existed.
    ok(typeof native.ringRead === 'function', 'ringRead is exported');
    c.close();
}

console.log(fail ? `  ${fail} FAILURES` : '  all passed');
process.exit(fail ? 1 : 0);
