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

    console.log(fail ? `  ${fail} failed` : '  [l3-cap] all passed');
    process.exit(fail ? 1 : 0);
})();
