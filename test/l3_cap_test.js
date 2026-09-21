'use strict';
// "SERVE THROUGH THE OUTAGE, THEN CONVERGE" -- ON A WORKER.
//
// Decision 68 keeps a write L3 rejected in the local tiers under a short TTL,
// so the box goes on serving while L3 is unreachable and then converges. A
// WORKER MAY NOT WRITE L2 AT ALL, so the cap is a CONDITIONAL OPERATION IT
// ASKS THE PRIMARY TO APPLY: the request names the value the cap was taken
// against, and the primary -- the arena's only writer, and synchronous --
// re-times the entry only if that is still what it holds.
//
// The shape before this was a blind re-write submitted by the worker after its
// own compare, with a hop in between: anything the primary applied inside that
// hop was overwritten when the cap landed, resurrecting a deleted key or
// putting an older value back box-wide for l3FailTtlMs. The compare was never
// wrong; it was asked in the wrong process.
//
// Three things have to hold:
//
//   - the value L3 refused no longer sits in the SHARED arena with no expiry,
//     where every process on the box reads it forever;
//   - this worker's own reads stop serving it AT the deadline, rather than
//     whenever the primary gets around to applying the cap -- otherwise the
//     capped L1 entry expires and the read falls straight through to the
//     uncapped L2 copy;
//   - and the cap only ever SHORTENS. A value another writer superseded is
//     never put back, and an entry whose own TTL expires sooner keeps it.
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
        ok(w.stats.l3FailTtlApplied >= 1,
           `the local copy was capped (${w.stats.l3FailTtlApplied})`);

        // AND THE SHARED ARENA GETS THE DEADLINE, which is the half every
        // other process on the box depends on.
        //
        // A SECOND HANDLE with a long cap, deliberately: on the 250ms one the
        // capped entry is usually gone before anything here can sample it, and
        // "the key is absent" was accepted as convergence -- which made this
        // assertion pass for a write that never reached L2 at all. That
        // false positive is exactly how this file failed on one CI platform
        // and nowhere else. Here the entry must be PRESENT and carry a
        // deadline: absent proves nothing and is not accepted.
        const wl = TurboKV.attachWorker(ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 4000 });
        ok(await wl.setAsync('capped', 'V') === false, 'a second shed write, with a long cap');
        let rem = 0;
        for (let i = 0; i < 80 && rem <= 0; i++) {
            await sleep(25);
            if (native.get('capped') !== undefined) rem = native.lastTtlRemainingMs();
        }
        ok(rem > 0, `the arena entry ends up with a deadline instead of living forever (${rem})`);
        ok(rem <= 4000, `and it is the cap, not a life of its own (${rem})`);
        ok((wl.stats.l3FailTtlUnapplied || 0) === 0,
           `nothing was abandoned (${wl.stats.l3FailTtlUnapplied || 0})`);
        wl.close();

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

// ------------------------------------------------ worker, ipc transport ----
// THE RETIME FAMILY NEEDS ONE MEMBER PER TRANSPORT. The publish family has an
// arena route, a ring route and an IPC route; the retime family had only the
// first two, so on a `transport: 'ipc'` worker the cap had nowhere to go and
// l3FailTtlMs simply did not exist -- a value L3 had explicitly refused sat in
// the SHARED arena with no expiry, forever, and the only trace was a counter.
// The same convergence promise decision 68 makes, unkept on one transport.
if (process.env.TCC_ROLE === 'ipc') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', transport: 'ipc', l3: f.adapter, l3RetryMs: 10, l3FailTtlMs: 300 });
        ok(w.transport === 'ipc', `the worker is on the IPC transport (${w.transport})`);
        ok(await w.setAsync('refused', 'REFUSED-BY-L3') === false, 'the L3 write failed');

        // Non-vacuous in both directions: the write must actually reach L2,
        // and it must be there UNCAPPED first -- otherwise "it has a deadline
        // now" would pass for a key that was never written, or for one the
        // original write had already bounded.
        for (let i = 0; i < 80 && native.get('refused') === undefined; i++) { await sleep(25); w.get('poke'); }
        ok(native.get('refused') === 'REFUSED-BY-L3', `the write reached L2 (${native.get('refused')})`);
        ok(native.lastTtlRemainingMs() === 0, 'and is resident with no expiry');

        let ttl = 0, gone = false;
        for (let i = 0; i < 80; i++) {
            await sleep(25); w.get('poke');
            if (native.get('refused') === undefined) { gone = true; break; }
            ttl = native.lastTtlRemainingMs();
            if (ttl > 0) break;
        }
        ok(ttl > 0 || gone,
           `the cap reaches L2 over IPC too (ttlRemaining ${ttl}${gone ? ', already expired' : ''})`);
        ok(ttl <= 300, `and it is the cap, not the value's own life (${ttl})`);
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `nothing is abandoned (${w.stats.l3FailTtlUnapplied || 0})`);

        // And it converges, which is the promise the counter was standing in
        // for.
        for (let i = 0; i < 40 && w.get('refused') !== undefined; i++) await sleep(25);
        ok(w.get('refused') === undefined, `the value L3 refused is gone (${w.get('refused')})`);
        w.close();
        console.log(fail ? `  ${fail} failed (ipc)` : '  [l3-cap] ipc-transport cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// ------------------------------------------------- worker, shed and ipc ----
// A SHED WRITE MUST NOT DROP A CAP'S READ GUARD, and an IPC write MUST. Both
// are one branch inside one publish helper, which is the point of that
// refactor -- but a branch is still a branch, and the source-level test in
// write_sites_test.js cannot see which side of it the cancel sits on.
if (process.env.TCC_ROLE === 'shed') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const a = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 5, l3FailTtlMs: 60000 });
        a.set('capped', 'OURS');
        await a.drainL3();
        await sleep(20);
        a.__unsafePauseCaps();                  // nothing races the assertions below
        ok(a.__unsafeHasCap('capped'), 'a cap is outstanding for the key');

        // Saturate the ring. The primary was started with maintenance off, so
        // nothing drains it.
        for (let i = 0; i < 2000 && !(a.stats.writesShed > 0); i++) a.set('fill' + i, 'v');
        ok(a.stats.writesShed > 0, `the submission ring is full (shed ${a.stats.writesShed})`);
        const before = a.stats.writesShed;
        a.set('capped', 'NEWER');               // shed: nothing reaches the ring
        ok(a.stats.writesShed > before, 'the write to the capped key is shed too');
        ok(a.__unsafeHasCap('capped'), 'a shed write does not cancel the cap');
        // A PROMOTION, actually issued, from a WORKER. It publishes nothing at
        // all now -- L3-derived data reaches the arena through the primary and
        // nowhere else -- so there is nothing for it to cancel, whether or not
        // the ring would have taken it. An assertion that only seeds the fake
        // L3 and never reads claims something it does not test, and stayed
        // green with the old fix reverted, so the read is made here.
        //
        // Its own instance, with a cap short enough that the read guard makes
        // the key miss locally, which is what sends the read to L3.
        const p2 = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 5, l3FailTtlMs: 40 });
        p2.set('shedpromo', 'OURS');
        await p2.drainL3();
        await sleep(20);
        p2.__unsafePauseCaps();
        ok(p2.__unsafeHasCap('shedpromo'), 'a second cap is outstanding');
        await sleep(60);                        // past the 40ms cap
        ok(p2.get('shedpromo') === undefined, `the key misses locally (${p2.get('shedpromo')})`);
        f.store.set('shedpromo', { value: 'L3VAL', expiresAt: 0 });
        const promoted = await p2.getAsync('shedpromo');
        ok(promoted === 'L3VAL', `the read goes through to L3 (${promoted})`);
        ok(p2.__unsafeHasCap('shedpromo'), 'a worker promotion does not cancel the cap either');
        ok(native.get('shedpromo') !== 'L3VAL',
           `and the shared arena never saw it (${JSON.stringify(native.get('shedpromo'))})`);
        p2.close();

        // THE IPC FALLBACK, from a sibling instance. That write really is
        // published, so the guard it supersedes has to go -- otherwise the key
        // goes on missing locally past a deadline belonging to a value nobody
        // will read again.
        const ipc = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', transport: 'ipc' });
        ok(ipc.transport === 'ipc', `the sibling negotiated the ipc transport (${ipc.transport})`);
        ipc.set('capped', 'VIA-IPC');
        ok(!a.__unsafeHasCap('capped'), 'an IPC write cancels the cap');
        ipc.close();

        a.close();
        console.log(fail ? `  ${fail} failed (shed)` : '  [l3-cap] shed/ipc cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// -------------------------------------------------- worker, cap bookkeeping ----
// The caps a worker keeps are a READ GUARD, not a queue of work: the cap
// itself has already been handed to the primary as a conditional 'r'. What is
// left here is "this worker must stop serving the value L3 refused at the
// deadline, rather than whenever the primary gets round to it".
//
// So the bookkeeping has to hold: one guard entry per failed write, dropped
// again the moment anything in this PROCESS publishes a newer value for the
// key -- otherwise the key goes on missing locally past a deadline that no
// longer applies to anything.
//
// Driven against a primary that drains only on request, so the orderings are
// decided by this file rather than by a timer.
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
        const N = 200;
        for (let i = 0; i < N; i++) w.set('bulk' + i, 'V');
        await w.drainL3();
        await sleep(20);                       // the cap handlers run off the settled promises
        ok(w.__unsafeCapState().caps === N,
           `every failed write records a read guard (${w.__unsafeCapState().caps} of ${N})`);
        ok(w.__unsafeCapState().timer === true, 'and the retire poll is armed');
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `and every cap was handed over (${w.stats.l3FailTtlUnapplied || 0})`);
        process.send({ step: 'written' });
        await step('drained');

        // A LOCAL WRITE DROPS ITS GUARD. The guard exists to make this worker
        // MISS on the value L3 refused; a newer value for the key is on its
        // way, so leaving the entry would make the key miss past a deadline
        // that belongs to a value nobody will read again.
        w.set('bulk0', 'NEWER');
        ok(w.__unsafeCapState().caps === N - 1,
           `a local write drops that key's guard (${w.__unsafeCapState().caps} of ${N})`);

        // AND A SIBLING'S WRITE DOES TOO. The guards are per-instance and the
        // submission ring is per-PROCESS -- decision 64's family, and the same
        // sweep across `instances` that #dropOthers does.
        const sib = TurboKV.attachWorker(process.env.TCC_ARENA, 1, { storage: 'bytes' });
        sib.set('bulk1', 'SIBLING');
        ok(w.__unsafeCapState().caps === N - 2,
           `a sibling instance's write drops it too (${w.__unsafeCapState().caps} of ${N})`);

        process.send({ step: 'capped' });
        await step('drained2');
        // THE CAP IS CONDITIONAL, so the newer value stands. The primary
        // compared before it wrote, in the same synchronous step, and refused.
        ok(native.get('bulk0') === 'NEWER',
           `no cap was ordered behind the newer write (${native.get('bulk0')})`);
        ok(w.get('bulk0') === 'NEWER', `the worker reads it (${w.get('bulk0')})`);
        ok(native.get('bulk1') === 'SIBLING',
           `nor behind a sibling instance's write (${native.get('bulk1')})`);
        // ...while a key nothing superseded DID get the deadline, so the
        // refusals above are refusals and not a cap that simply never ran.
        ok(native.get('bulk2') === 'V' && native.lastTtlRemainingMs() > 0,
           `an untouched key was capped (${native.get('bulk2')}/${native.lastTtlRemainingMs()})`);
        sib.close();

        // A PROMOTION FROM A WORKER TOUCHES L2 AT ALL. It fills this worker's
        // own L1 and stops there, so it neither cancels the guard nor writes
        // the arena -- the architectural rule, seen from inside the one path
        // that used to be the exception. The route in is this design's own:
        // the read guard makes a capped key miss locally, which is what sends
        // the read to L3.
        const f2 = makeFake();
        f2.fail.set('set', new Error('l3 down'));
        f2.store.set('promo', { value: 'L3VAL', expiresAt: 0 });
        const w2 = TurboKV.attachWorker(process.env.TCC_ARENA, 1,
            { storage: 'bytes', l3: f2.adapter, l3RetryMs: 5, l3FailTtlMs: 60 });
        w2.set('promo', 'OURS');
        await w2.drainL3();
        await sleep(20);
        ok(w2.__unsafeCapState().caps === 1, `the failed write records a guard (${w2.__unsafeCapState().caps})`);
        await sleep(80);                       // past the 60ms cap
        w2.__unsafePauseCaps();                // nothing retires the guard under the assertions
        process.send({ step: 'promo-written' });
        await step('drained3');                // arena now holds OURS
        ok(w2.get('promo') === undefined,
           `the read guard makes the key miss locally (${JSON.stringify(w2.get('promo'))})`);
        ok(await w2.getAsync('promo') === 'L3VAL', 'so the read goes through to L3');
        ok(w2.__unsafeCapState().caps === 1,
           `and the promotion cancelled nothing, because it published nothing (${w2.__unsafeCapState().caps})`);
        process.send({ step: 'promoted' });
        await step('drained4');
        ok(native.get('promo') !== 'L3VAL',
           `the worker's promotion never reached the shared arena (${native.get('promo')})`);
        w2.close();

        w.close();
        console.log(fail ? `  ${fail} failed (bulk)` : '  [l3-cap] cap-bookkeeping cases passed');
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
            // A worker's cap is a REQUEST to the primary now, and it travels in
            // the ordinary IPC batch -- the same channel clearAll's generation
            // ops already use. This is a plain fork, not a cluster, so the
            // parent routes it by hand exactly as install() would.
            kid.on('message', (m) => { if (TurboKV.isCacheMessage(m)) TurboKV.applyBatch(m); });
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
                // The caps travel over IPC; applyBatch drains every submission
                // ring to empty before it applies them, which is what puts the
                // worker's own write for a key provably ahead of the cap that
                // names it.
                if (TurboKV.isCacheMessage(m)) { TurboKV.applyBatch(m); return; }
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
        ok(code === 0, `the cap-bookkeeping cases passed (child exited ${code})`);
        await primary.close();
    }

    // The shed/IPC case needs a ring too small to take a record and a primary
    // that never drains it.
    {
        const arena = ARENA + 'd';
        const primary = TurboKV.createPrimary(arena, 16 << 20, 1 << 14,
            { storage: 'bytes', maintenance: false, submitRings: 2, submitRingBytes: 4096 });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCC_ROLE: 'shed', TCC_ARENA: arena }, stdio: 'inherit',
            });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the shed/ipc cases passed (child exited ${code})`);
        await primary.close();
    }

    // The IPC-transport case. The parent has to route the batches itself --
    // this is a plain fork, not a cluster, so install() has no channel to wire.
    {
        const arena = ARENA + 'i';
        const primary = TurboKV.createPrimary(arena, 16 << 20, 1 << 14,
            { storage: 'bytes', maintenanceMs: 25 });
        const code = await new Promise((resolve) => {
            const kid = fork(__filename, [], {
                env: { ...process.env, TCC_ROLE: 'ipc', TCC_ARENA: arena }, stdio: 'inherit',
            });
            kid.on('message', (m) => { if (TurboKV.isCacheMessage(m)) TurboKV.applyBatch(m); });
            kid.on('exit', (c) => resolve(c));
        });
        ok(code === 0, `the ipc-transport cases passed (child exited ${code})`);
        await primary.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-cap] all passed');
    process.exit(fail ? 1 : 0);
})();
