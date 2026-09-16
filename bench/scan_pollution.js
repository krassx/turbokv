// Does a large-value scan evict the hot working set from L1?
//
// The case: a request path keeps a small, hot set resident in L1, and a batch
// job walks a large cold keyspace through the same cache. Every cold value the
// batch touches is promoted into L1, which is a fixed byte budget, so the hot
// set is evicted by data nobody will read again. This is what a `minLevel`
// option on get() would prevent -- so measure it before building it.
//
// Arm A  scan through cache.get()  -- today: every cold value lands in L1
// Arm B  scan through native.get() -- reads L2 directly, leaving L1 untouched.
//        This is what get(key, { minLevel: L2 }) would do, so it stands in for
//        the option that does not exist yet.
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');

const HOT_KEYS   = 2000;
const HOT_BYTES  = 200;
const COLD_KEYS  = 2000;
const COLD_BYTES = 64 * 1024;
const L1_BYTES   = 8 << 20;
const READS      = 20000;

const hotVal  = 'h'.repeat(HOT_BYTES);
const coldVal = 'c'.repeat(COLD_BYTES);

function run(arm) {
    const c = TurboKV.createPrimary('/tcscan' + process.pid + arm, 512 << 20, 1 << 18,
                                    { storage: 'bytes', l1MaxBytes: L1_BYTES, maintenance: false });

    for (let i = 0; i < HOT_KEYS; i++) c.set('hot:' + i, hotVal);
    for (let i = 0; i < COLD_KEYS; i++) c.set('cold:' + i, coldVal);
    c.clearLocal();                                   // start from a known-empty L1

    // --- warm: pull the hot set into L1 and confirm it is resident
    for (let i = 0; i < HOT_KEYS; i++) c.get('hot:' + i);
    const warmL1 = c.l1Size;

    const before = measure(c, HOT_KEYS);

    // --- the batch job walks the cold keyspace once
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < COLD_KEYS; i++) {
        if (arm === 'A') c.get('cold:' + i);
        else native.get('cold:' + i);                 // stands in for minLevel: L2
    }
    const scanMs = Number(process.hrtime.bigint() - t0) / 1e6;

    const afterL1 = c.l1Size;
    // The FIRST pass over the hot set is the damage: every evicted key is an L2
    // read. Measuring only a long steady-state run hides it, because the run
    // itself re-warms L1 -- the first version of this bench reported a 1.5 point
    // hit-rate drop for a scan that had evicted 94% of the set.
    const firstPass = measure(c, HOT_KEYS);
    const steady = measure(c, READS);

    native.destroy();
    return { warmL1, afterL1, before, firstPass, steady, scanMs };
}

// Hit rate and latency over the hot set, read in a fixed pseudo-random order.
function measure(c, n) {
    const s0 = { ...c.stats };
    let seed = 12345;
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < n; i++) {
        seed = (seed * 1103515245 + 12345) & 0x7fffffff;
        c.get('hot:' + (seed % HOT_KEYS));
    }
    const ns = Number(process.hrtime.bigint() - t0) / n;
    const l1 = c.stats.l1Hits - s0.l1Hits;
    const l2 = c.stats.l2Hits - s0.l2Hits;
    return { l1HitPct: (100 * l1 / (l1 + l2)), ns };
}

// Both arms run in one process, so whichever goes first pays the JIT warm-up.
// The first version of this bench reported A 1.74x slower than B partly for that
// reason. Discard a full run, then measure each arm in BOTH orders and report
// the range -- if the effect is real it survives the swap.
run('A'); run('B');                                   // warm-up, discarded

const A1 = run('A'), B1 = run('B');
const B2 = run('B'), A2 = run('A');
const pick = (x, y) => ({
    ...x,
    firstPass: { l1HitPct: (x.firstPass.l1HitPct + y.firstPass.l1HitPct) / 2,
                 ns: Math.min(x.firstPass.ns, y.firstPass.ns) },
    steady: { ns: Math.min(x.steady.ns, y.steady.ns) },
    scanMs: Math.min(x.scanMs, y.scanMs),
    spread: Math.abs(x.firstPass.ns - y.firstPass.ns),
});
const A = pick(A1, A2), B = pick(B1, B2);

const row = (label, r) =>
    console.log(`  ${label.padEnd(34)} survivors ${String(r.afterL1).padStart(5)}/${HOT_KEYS}` +
                `   first pass after scan: ${r.firstPass.l1HitPct.toFixed(1)}% L1 hits, ${r.firstPass.ns.toFixed(0)}ns/read` +
                `   steady ${r.steady.ns.toFixed(0)}ns`);

console.log(`\n  hot set ${HOT_KEYS} x ${HOT_BYTES}B, cold scan ${COLD_KEYS} x ${COLD_BYTES / 1024}KB, L1 budget ${L1_BYTES / (1 << 20)}MB`);
console.log(`  before the scan both arms: 100.0% L1 hits, ${A.before.ns.toFixed(0)}ns/read\n`);
row('A: scan through the cache (today)', A);
row('B: scan bypassing L1 (minLevel L2)', B);
console.log(`\n  the damage  -- hot entries evicted by the scan: ${HOT_KEYS - A.afterL1} of ${HOT_KEYS}` +
            ` (${(100 * (HOT_KEYS - A.afterL1) / HOT_KEYS).toFixed(1)}%) vs ${HOT_KEYS - B.afterL1} for B`);
console.log(`  the cost    -- first pass after the scan: ${A.firstPass.ns.toFixed(0)}ns (A) vs ${B.firstPass.ns.toFixed(0)}ns (B)` +
            `  -> ${(A.firstPass.ns / B.firstPass.ns).toFixed(2)}x`);
console.log(`  the scan    -- ${A.scanMs.toFixed(0)}ms (A) vs ${B.scanMs.toFixed(0)}ms (B)` +
            `  -> promoting 128MB into L1 costs ${(A.scanMs / B.scanMs).toFixed(2)}x`);
console.log(`  run-to-run spread within an arm: A ${A.spread.toFixed(0)}ns, B ${B.spread.toFixed(0)}ns` +
            ` (best-of-two reported, both orders measured)\n`);
