'use strict';
// "SERVE THROUGH THE OUTAGE, THEN CONVERGE" -- ON A WORKER.
//
// Decision 68 keeps a write L3 rejected in the local tiers under a short TTL,
// so the box goes on serving while L3 is unreachable and then converges. On the
// PRIMARY the cap is applied to L2 on the spot. A WORKER cannot write L2: its
// write is still sitting in the submission ring when the L3 outcome arrives a
// microtask later, so the compare that keeps the cap safe -- "is the arena
// still holding exactly the value I wrote" -- looked at the PREVIOUS value, or
// at nothing, and refused.
//
// The compare is right and was asked too early, so it is now retried until it
// is meaningful. Two things have to hold afterwards:
//
//   - the value L3 refused no longer sits in the SHARED arena with no expiry,
//     where every process on the box reads it forever;
//   - and this worker's own reads stop serving it AT the deadline, rather than
//     whenever the primary gets around to applying the cap -- otherwise the
//     capped L1 entry expires and the read falls straight through to the
//     uncapped L2 copy.
//
// The compare must still refuse when it should: a value another writer
// superseded must never be put back by a cap.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCC_ARENA || ('/tcl3cap' + process.pid);

if (process.env.TCC_ROLE === 'worker') {
    (async () => {
        const f = makeFake();
        // A queue that sheds everything: the L3 write is abandoned on the spot,
        // so the cap runs in a microtask, before the primary has drained
        // anything. That is the exact ordering the finding is about.
        const w = TurboKV.attachWorker(ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 250 });
        ok(w.transport === 'shm', `the worker negotiated the shm transport (${w.transport})`);

        const r = await w.setAsync('shed', 'V');
        ok(r === false, `the L3 write was shed and the caller is told (${r})`);
        ok(w.get('shed') === 'V', `the value is served locally through the outage (${w.get('shed')})`);

        // Past the cap: this worker must not serve it, whatever L2 still holds.
        await sleep(400);
        ok(w.get('shed') === undefined,
           `the worker converges at the cap rather than falling through to L2 (${JSON.stringify(w.get('shed'))})`);
        ok(await w.hasAsync('shed') === false,
           'hasAsync agrees, rather than reporting a value get() calls gone');

        // And the SHARED arena gets the deadline, which is the half other
        // processes depend on. The primary drains on its maintenance timer.
        let capped = false;
        for (let i = 0; i < 60 && !capped; i++) {
            await sleep(25);
            if (native.get('shed') !== undefined && native.lastTtlRemainingMs() > 0) capped = true;
            if (native.get('shed') === undefined) capped = true;   // already expired: also converged
        }
        ok(capped, 'the arena entry ends up with a deadline instead of living forever');
        ok(w.stats.l3FailTtlApplied >= 2,
           `both tiers were capped, not just L1 (${w.stats.l3FailTtlApplied})`);

        // A SUPERSEDED VALUE IS NEVER PUT BACK. The cap is a rewrite, so if
        // anything replaced the value in the meantime the compare has to
        // refuse -- a resurrection is the one failure this system does not
        // accept, and a deferred cap widens the window in which one is
        // possible.
        f.fail.set('set', new Error('l3 down'));
        const w2 = TurboKV.attachWorker(ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 20, l3FailTtlMs: 5000 });
        w2.set('race', 'OLD');
        await sleep(60);                       // the L3 write is abandoned; the cap is deferred
        w2.set('race', 'NEW');                 // superseded before the cap can land
        await sleep(300);
        ok(native.get('race') === 'NEW',
           `the newer value stands; the cap did not resurrect the older one (${native.get('race')})`);
        ok(w2.get('race') === 'NEW', `and the worker reads it (${w2.get('race')})`);
        w2.close();

        w.close();
        console.log(fail ? `  ${fail} failed (worker)` : '  [l3-cap] worker cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// ---------------------------------------------------- worker, bulk poll ----
// THE POLL IS BOUNDED. Deciding whether a cap can be applied means asking the
// arena, and that copies a value out of it: a full pass over the 4096-entry
// bound was measured at 1.69ms for 8KB values -- an event-loop stall every 20ms
// for the length of an outage. So a tick reaches the arena for at most 64 caps.
//
// Driven a tick at a time against a primary that drains only on request, rather
// than timed: the bound is a property of ONE tick, and a count sampled from a
// loaded event loop is a count of however many ticks the sample spanned, which
// made the obvious timing version flaky in both directions.
if (process.env.TCC_ROLE === 'bulk') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 5, l3FailTtlMs: 60000 });
        const step = (want) => new Promise((r) => {
            const h = (m) => { if (m && m.step === want) { process.off('message', h); r(); } };
            process.on('message', h);
        });
        const N = 640;                         // ten slices
        for (let i = 0; i < N; i++) w.set('bulk' + i, 'V');
        await w.drainL3();
        await sleep(20);                       // the cap handlers run off the settled promises
        ok(w.__unsafeCapState().caps === N,
           `every failed write deferred a cap (${w.__unsafeCapState().caps} of ${N})`);
        // Nothing has landed in L2 yet, so every tick so far has correctly done
        // nothing -- which is what makes the first tick after the drain the one
        // the bound is measured on.
        ok(w.__unsafeCapState().sent === 0, `and none could be applied yet (${w.__unsafeCapState().sent})`);
        process.send({ step: 'written' });
        await step('drained');

        // A LOCAL WRITE CANCELS ITS CAP, and this is where that matters. The
        // cap decides by comparing against the ARENA, but a write goes into the
        // submission RING, so between the two there is a window where the arena
        // still holds the old value and the ring already holds the new one. A
        // cap submitted in that window is ordered BEHIND the new write and
        // re-applies the old value on top of it. Deterministic here: the parent
        // has drained exactly once, so 'bulk0' is in L2 while the write below
        // is only in the ring.
        w.set('bulk0', 'NEWER');
        ok(w.__unsafeCapState().caps === N - 1,
           `a local write drops that key's cap (${w.__unsafeCapState().caps} of ${N})`);

        // AND A SIBLING'S WRITE DOES TOO. The caps are per-instance and the
        // submission ring is per-PROCESS, so instance A's cap for a key is
        // ordered behind instance B's write to it exactly as it would be
        // behind A's own -- decision 64's family, and the same sweep across
        // `instances` that #dropOthers does. Cancelling only on the writing
        // instance left the arena holding the capped old value for a full
        // l3FailTtlMs.
        const sib = TurboKV.attachWorker(process.env.TCC_ARENA, 1, { storage: 'bytes' });
        sib.set('bulk1', 'SIBLING');
        ok(w.__unsafeCapState().caps === N - 2,
           `a sibling instance's write drops it too (${w.__unsafeCapState().caps} of ${N})`);

        w.__unsafeRunCaps();
        const one = w.__unsafeCapState().sent;
        ok(one > 0, `one tick makes progress (${one})`);
        // 2x the slice, so a real interval tick slipping into the message turn
        // cannot fail this. Either way it is nowhere near ${N}.
        ok(one <= 128, `and reaches the arena for a bounded slice, not all ${N} (${one})`);
        // The bound must not cost convergence: ten slices, so eleven more ticks.
        for (let i = 0; i < 12; i++) w.__unsafeRunCaps();
        // N - 2, because the two writes above cancelled one cap each.
        ok(w.__unsafeCapState().sent === N - 2,
           `every remaining cap is applied within ceil(N/64) ticks (${w.__unsafeCapState().sent} of ${N - 2})`);
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `and none is abandoned (${w.stats.l3FailTtlUnapplied || 0})`);
        process.send({ step: 'capped' });
        await step('drained2');
        ok(native.get('bulk0') === 'NEWER',
           `and no cap was ordered behind the newer write (${native.get('bulk0')})`);
        ok(w.get('bulk0') === 'NEWER', `the worker reads it (${w.get('bulk0')})`);
        ok(native.get('bulk1') === 'SIBLING',
           `nor behind a sibling instance's write (${native.get('bulk1')})`);
        sib.close();

        // A PROMOTION IS A WRITE INTO THE RING, so it cancels an outstanding
        // cap exactly as set() does -- same mechanism, same consequence if it
        // does not (the cap lands behind the promotion and puts the value L3
        // refused back over the value L3 holds). The route into it is this
        // very design: the read guard is what makes a key with an outstanding
        // cap miss locally, which is what sends the read to L3 to be promoted.
        const f2 = makeFake();
        f2.fail.set('set', new Error('l3 down'));
        f2.store.set('promo', { value: 'L3VAL', expiresAt: 0 });
        const w2 = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f2.adapter, l3RetryMs: 5, l3FailTtlMs: 60 });
        w2.set('promo', 'OURS');
        await w2.drainL3();
        await sleep(20);
        ok(w2.__unsafeCapState().caps === 1, `the failed write defers a cap (${w2.__unsafeCapState().caps})`);
        // Past the cap BEFORE the arena is told about the write: while the
        // arena holds nothing for this key the poll cannot submit anything, so
        // the cap is still outstanding when the drain below lands.
        await sleep(80);
        ok(w2.__unsafeCapState().sent === 0,
           `which cannot be submitted while the write is still in the ring (${w2.__unsafeCapState().sent})`);
        process.send({ step: 'promo-written' });
        await step('drained3');                   // arena now holds OURS, uncapped

        // From here to the tick below there is no macrotask boundary: the fake
        // adapter answers through microtasks, so the automatic poll cannot run
        // in between. It MAY have run in the message turn above, which is why
        // `sent` is reported rather than asserted -- the arena assertion at the
        // end holds either way, and is the discriminating one when it is 0.
        const raced = w2.__unsafeCapState().sent;
        ok(w2.get('promo') === undefined,
           `the read guard makes the key miss locally (${JSON.stringify(w2.get('promo'))})`);
        ok(await w2.getAsync('promo') === 'L3VAL', 'so the read goes through to L3');
        ok(w2.__unsafeCapState().caps === 0,
           `and the promotion cancelled the outstanding cap (${w2.__unsafeCapState().caps})`);
        w2.__unsafeRunCaps();
        process.send({ step: 'promoted' });
        await step('drained4');
        ok(native.get('promo') === 'L3VAL',
           `the arena holds what L3 holds, not the value it refused ` +
           `(${native.get('promo')}; poll had claimed ${raced} before the promotion)`);
        ok(w2.get('promo') === 'L3VAL', `and the worker reads it (${w2.get('promo')})`);
        w2.close();

        w.close();
        console.log(fail ? `  ${fail} failed (bulk)` : '  [l3-cap] bulk-poll cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// ----------------------------------------------------- worker, one drain ----
// The deferred cap closes the SHARED arena's half. This half is the worker's
// own: between the moment the cap falls due and the moment the primary applies
// it, the capped L1 entry has expired and a read falls straight through to the
// uncapped L2 copy -- so the worker goes on serving, past its own deadline, the
// value L3 refused. The parent drains the submission ring EXACTLY ONCE here, so
// the window is not a race: L2 provably holds the value with no expiry while
// the assertions run.
if (process.env.TCC_ROLE === 'slow') {
    (async () => {
        const f = makeFake();
        const w = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 120 });
        const step = (want) => new Promise((r) => {
            const h = (m) => { if (m && m.step === want) { process.off('message', h); r(); } };
            process.on('message', h);
        });
        ok(await w.setAsync('slow', 'V') === false, 'the L3 write was shed');
        process.send({ step: 'written' });
        await step('drained');                 // the SET is now in L2, uncapped
        await sleep(200);                      // well past the 120ms cap
        ok(native.get('slow') === 'V' && native.lastTtlRemainingMs() === 0,
           `L2 still holds the refused value with no expiry (${native.get('slow')}/${native.lastTtlRemainingMs()})`);
        ok(w.get('slow') === undefined,
           `and the worker refuses to serve it past the cap (${JSON.stringify(w.get('slow'))})`);
        ok(w.has('slow') === false, `has() agrees (${w.has('slow')})`);
        w.close();
        console.log(fail ? `  ${fail} failed (slow)` : '  [l3-cap] one-drain cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

(async () => {
    // The primary's own behaviour is the control, and must not change.
    {
        const f = makeFake();
        const c = TurboKV.createPrimary(ARENA + 'p', 8 << 20, 1 << 13,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 200 });
        ok(await c.setAsync('shedp', 'V') === false, 'the shed write is reported');
        native.get('shedp');
        const rem = native.lastTtlRemainingMs();
        ok(rem > 0 && rem <= 200, `the primary caps L2 immediately, as it always did (${rem})`);
        await sleep(260);
        ok(c.get('shedp') === undefined, `and converges (${c.get('shedp')})`);
        await c.close();
    }

    // A cache with NO ADAPTER never defers anything: no queue, no failed write,
    // no cap, and the read path must be untouched.
    {
        const c = TurboKV.createPrimary(ARENA + 'n', 4 << 20, 1 << 12, { storage: 'bytes', maintenance: false });
        c.set('k', 'v');
        ok(c.get('k') === 'v', 'a plain cache still reads back');
        ok(c.has('k') === true, 'and has() answers from the arena');
        ok(c.stats.l3FailTtlApplied === undefined,
           `no cap counter is invented for a cache with no L3 (${c.stats.l3FailTtlApplied})`);
        c.close();
    }

    {
        // A short maintenance interval, because that timer is what drains the
        // submission ring here: this is a plain fork, not a cluster, so the
        // worker's doorbell has no channel to arrive on.
        const primary = TurboKV.createPrimary(ARENA, 16 << 20, 1 << 14,
            { storage: 'bytes', maintenanceMs: 25 });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCC_ROLE: 'worker', TCC_ARENA: ARENA }, stdio: 'inherit',
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the worker cases passed (child exited ${code})`);
        TurboKV.drainSubmissions(8192);
        // From the primary's side: the key either expired or carries a
        // deadline. What it must NOT be is present with no expiry at all.
        const still = native.get('shed') !== undefined;
        const rem = still ? native.lastTtlRemainingMs() : 0;
        ok(!still || rem > 0,
           `the arena does not hold the refused value forever (present=${still} ttlRemaining=${rem})`);
        await primary.close();
    }

    // The one-drain case: the primary never drains on its own, so the parent
    // decides exactly when the worker's write reaches L2 -- and never lets the
    // cap that follows it reach L2 at all.
    {
        const arena = ARENA + 's';
        const primary = TurboKV.createPrimary(arena, 8 << 20, 1 << 13, { storage: 'bytes', maintenance: false });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCC_ROLE: 'slow', TCC_ARENA: arena }, stdio: 'inherit',
            });
            kid.on('message', (m) => {
                if (m && m.step === 'written') {
                    TurboKV.drainSubmissions(8192);     // the SET lands; nothing after it does
                    kid.send({ step: 'drained' });
                }
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the one-drain cases passed (child exited ${code})`);
        await primary.close();
    }

    // A failed write's promise can settle AFTER close(): the queue goes on
    // retrying until it is told to stop, and it is told only after a bounded
    // drain. Arming a poll then is low harm -- unref'd, and it terminates on
    // its first tick because a closed cache has no arena -- but a closed cache
    // should own no timer at all.
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const c = TurboKV.createPrimary(ARENA + 'x', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f.adapter,
              l3RetryMs: 120, l3FailTtlMs: 1000, l3CloseTimeoutMs: 1 });
        // A worker handle in this same process, so the cap is DEFERRED rather
        // than applied on the spot -- the primary never defers anything.
        const w = new TurboKV({ storage: 'bytes', l3: f.adapter, workerId: 1,
                                l3RetryMs: 120, l3FailTtlMs: 1000, l3CloseTimeoutMs: 1 });
        w.set('late', 'V');
        ok(w.__unsafeCapState().timer === false, 'no poll is armed before the write fails');
        await w.close();
        await sleep(400);                 // the abandoned write settles in here
        const st = w.__unsafeCapState();
        ok(st.timer === false, `a closed cache arms no poll (timer=${st.timer})`);
        ok(st.caps === 0, `and records no cap (caps=${st.caps})`);
        await c.close();
    }

    // The bulk case: one drain, on request, so the first tick after it is the
    // one the per-tick bound is measured on.
    {
        const arena = ARENA + 'u';
        const primary = TurboKV.createPrimary(arena, 16 << 20, 1 << 14, { storage: 'bytes', maintenance: false });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCC_ROLE: 'bulk', TCC_ARENA: arena }, stdio: 'inherit',
            });
            kid.on('message', (m) => {
                if (m && m.step === 'written') {
                    TurboKV.drainSubmissions(20000);
                    kid.send({ step: 'drained' });
                } else if (m && m.step === 'capped') {
                    TurboKV.drainSubmissions(20000);
                    kid.send({ step: 'drained2' });
                } else if (m && m.step === 'promo-written') {
                    TurboKV.drainSubmissions(20000);
                    kid.send({ step: 'drained3' });
                } else if (m && m.step === 'promoted') {
                    TurboKV.drainSubmissions(20000);
                    kid.send({ step: 'drained4' });
                }
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the bulk-poll cases passed (child exited ${code})`);
        await primary.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-cap] all passed');
    process.exit(fail ? 1 : 0);
})();
