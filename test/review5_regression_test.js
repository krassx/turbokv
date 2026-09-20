'use strict';
// Regressions for the fourth adversarial pass.
//
// All three findings are about the same seam the third pass split in two: a
// worker's marks, and what the WRAPPED-RING branch and the promotion guard do
// with them.
//
//   N1  a wrapped ring released every write mark UNCONDITIONALLY, so a
//       `minLevel: 2` write the primary had genuinely not drained went back to
//       reading L2's PREVIOUS value -- a stale value, not a miss, and a
//       persistent one. The stated bound ("one stale read after 65536 records,
//       with the doorbell and the backstop both dead") was wrong three ways:
//       the threshold is `ringCap` (32768 at the 16MB default, 8192 on a small
//       arena), a plain synchronous burst on the primary turns no event loop
//       and needs nothing to be broken, and the residue persists until
//       something else replaces the key.
//
//       The fix asks the PRECISE question instead of guessing: the submission
//       ring is SPSC with this worker as the producer, so the consumer's own
//       index says whether the primary stepped past our record. Released iff
//       it did; kept otherwise -- and a kept mark is re-examined on later
//       drains, so it cannot become F2's permanently unreadable key.
//
//   N2  `#promotionBlock` reporting a pending `minLevel: 2` write as
//       `l3PromotionsBlockedSelf` was claimed "by construction" and pinned by
//       nothing: reverting that one token to `l3DeletedWhileReading` passed
//       every suite. It is not a cosmetic difference -- that reason changes
//       the ANSWER, so the caller is handed `undefined` for a key it has just
//       successfully written.
//
//   N3  `set(k, v, { minLevel: 3 })` on a worker took a DELETE mark for the
//       L2 eviction it submits, so `#deletedHere` refused to ask L3 for a key
//       that by construction lives ONLY in L3. The write was acked and then
//       unreadable through every form.
//
// Every case runs in a forked child against a real primary: all of them are
// about the gap between a worker's submission and the moment the primary
// applies it, which does not exist in one process.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');

let fail = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCR5_ARENA || ('/tcr5' + process.pid);

const step = (want) => new Promise((r) => {
    const h = (m) => { if (m && m.step === want) { process.off('message', h); r(m); } };
    process.on('message', h);
});
// A worker catches up with the invalidation ring 512 records at a time, one
// batch per read (see #drain), so after a lap of 70000 records the record that
// clears a mark is tens of thousands of positions ahead of the cursor. Reading
// in a bounded loop is how a worker reaches it; `undefined` never appears once
// the value is there, so a loop that ends early ends because it succeeded.
const settle = (w, key, want) => {
    for (let i = 0; i < 400; i++) { const v = w.get(key); if (v === want) return v; }
    return w.get(key);
};
const done = (c, label) => {
    console.log(fail ? `  ${fail} failed (${label})` : `  [review5] ${label} cases passed`);
    if (c) c.close();
    process.exit(fail ? 1 : 0);
};

// ------------------------------------ N1: the wrap residue, on both transports
//
// The primary writes a full ring of records SYNCHRONOUSLY and drains nothing.
// That is the whole precondition: a synchronous burst turns no event loop, so
// the doorbell never fires and the 500ms backstop never runs, with nothing
// broken anywhere.
if (process.env.TCR5_ROLE === 'wrapres' || process.env.TCR5_ROLE === 'wrapresipc') {
    (async () => {
        const ipc = process.env.TCR5_ROLE === 'wrapresipc';
        const w = TurboKV.attachWorker(process.env.TCR5_ARENA, 1,
            { storage: 'bytes', ...(ipc ? { transport: 'ipc' } : {}) });
        w.get('poke');
        ok(w.transport === (ipc ? 'ipc' : 'shm'), `the worker is on the ${ipc ? 'ipc' : 'shm'} transport`);
        ok(w.get('k2') === 'OLD', 'the arena holds the value this write is about to replace');
        ok(w.set('k2', 'NEW', { minLevel: TurboKV.L2 }) === true, 'a minLevel-2 write is accepted');
        ok(w.get('k2') === undefined, 'and the worker misses on it while the write is undrained');

        process.send({ step: 'wrap' }); await step('wrapped');   // NOTHING is drained

        const after = w.get('k2');
        if (!ipc) {
            // THE FINDING. The submission is provably unconsumed -- the
            // consumer index has not moved -- so the mark is kept and the read
            // misses. Reading 'OLD' here is the stale value this system does
            // not accept.
            ok(after === undefined,
               `after a wrap with the submission still unconsumed the worker MISSES (${JSON.stringify(after)})`);
            ok(w.has('k2') === false, 'has() agrees');
            ok(await w.getAsync('k2') === undefined, 'and so does the async form');
            ok(w.__unsafeMarkState().writtenKeys.indexOf('k2') >= 0,
               `the write mark survived the wrap (${JSON.stringify(w.__unsafeMarkState().writtenKeys)})`);
        } else {
            // THE DOCUMENTED RESIDUE, pinned so it cannot drift away from what
            // README and the source say. The IPC fallback has no consumer
            // index -- a batch on the cluster channel is acked by nothing --
            // so "did the primary apply my write" is genuinely unanswerable
            // and the mark is released, which is the safe direction between
            // one stale read and a permanently unreadable key. If this ever
            // starts returning undefined, the fix is real and the prose in
            // #drain and in README has to be corrected with it.
            ok(after === 'OLD',
               `on ipc the mark is released and the superseded value is read (${JSON.stringify(after)})`);
        }

        // NOT PERMANENT -- the other half of the ranking, and what F2 was.
        // Once the primary drains, the record comes round and the key is
        // readable again through every form.
        process.send({ step: 'drain' }); await step('drained');
        const back = settle(w, 'k2', 'NEW');
        ok(back === 'NEW', `the key is readable again once the primary catches up (${JSON.stringify(back)})`);
        ok(w.__unsafeMarkState().writtenKeys.indexOf('k2') < 0, 'and the mark is gone');

        if (!ipc) {
            // A CONSUMED SUBMISSION WHOSE RECORD THE WRAP THREW AWAY. This is
            // the lever itself: nothing on the invalidation ring can clear
            // this mark -- the record is inside the discarded region -- so the
            // consumer index is the only thing that answers, and it must.
            ok(w.set('k3', 'NEW3', { minLevel: TurboKV.L2 }) === true, 'a second minLevel-2 write is accepted');
            process.send({ step: 'drainwrap' }); await step('drainwrapped');
            ok(w.get('k3') === 'NEW3',
               `a consumed submission releases its mark even when the record is lost to a wrap (${JSON.stringify(w.get('k3'))})`);
            ok(w.__unsafeMarkState().writtenKeys.indexOf('k3') < 0, 'so nothing is left holding the key down');
        }
        done(w, ipc ? 'wrap-residue [ipc]' : 'wrap-residue [shm]');
    })();
    return;
}

// -------------------- N1b: the drain must MAKE "tail past it" mean "applied"
//
// The wave-6 review's first note, and it is the load-bearing half of N1's
// lever. `storeSet` returns false on a logAlloc or findFreeSlot rejection and
// never reaches `ringAppend` on that path, but SubmitDrain ignored the return
// and advanced `tail` anyway. So the consumer index said "consumed" for a
// record that was consumed and DROPPED, and after a wrap #reconcileWrites
// released the mark for it -- L2 kept the predecessor, and this worker read
// the value its own acked write replaced.
//
// Note the asymmetry, because it is what makes this a wrap-only stale read:
// WITHOUT a wrap the same rejection publishes no record, so the mark is never
// cleared and the worker keeps missing. Safe, but it also means the mark is
// never cleared AT ALL -- a permanent local miss for that key.
//
// The drain now DELETES the key when a set is rejected, which is the answer to
// both: L2 holds nothing rather than a superseded value, and storeDelete
// always appends a record, so the mark clears the ordinary way and every other
// process's L1 copy of the superseded value is invalidated with it.
//
// The rejection is forced with __unsafeRejectDrainSets, because it is
// unreachable through the public API for a worker whose #maxValue came from
// this arena -- see the hook's comment for the one route that does reach it.
if (process.env.TCR5_ROLE === 'rejectset') {
    (async () => {
        const w = TurboKV.attachWorker(process.env.TCR5_ARENA, 1, { storage: 'bytes' });
        w.get('poke');
        ok(w.get('r1') === 'OLD' && w.get('r2') === 'OLD2', 'the arena holds the values these writes will replace');

        // ---- rejected, NO wrap. The mark has to be let go of, or the key is
        // unreadable on this worker for as long as it lives.
        ok(w.set('r1', 'NEW', { minLevel: TurboKV.L2 }) === true, 'a minLevel-2 write is accepted');
        process.send({ step: 'reject-drain' }); await step('reject-drained');
        ok(w.get('r1') === undefined,
           `the rejected write leaves nothing to read, not the superseded value (${JSON.stringify(w.get('r1'))})`);
        ok(w.__unsafeMarkState().writtenKeys.indexOf('r1') < 0,
           `and the mark is released rather than stuck forever (${JSON.stringify(w.__unsafeMarkState().writtenKeys)})`);

        // ---- rejected, THEN a wrap. This is the stale read: the mark is
        // released because the consumer index passed the record, and the
        // record is the one thing that would have said it never landed.
        ok(w.set('r2', 'NEW2', { minLevel: TurboKV.L2 }) === true, 'a second minLevel-2 write is accepted');
        process.send({ step: 'reject-wrap' }); await step('reject-wrapped');
        const after = w.get('r2');
        ok(after === undefined,
           `after a wrap a rejected write still reads as a MISS, never as the value it replaced (${JSON.stringify(after)})`);
        ok(w.has('r2') === false, 'has() agrees');
        // And the shared arena agrees with both of them: this is not a local
        // suppression papering over a value every other process still sees.
        process.send({ step: 'report', arena1: native.get('r1'), arena2: native.get('r2') });
        await step('bye');
        done(w, 'rejected-set');
    })();
    return;
}

// ------------- N1c: what a worker snapshots from an arena it no longer has
//
// The route that made N1b reachable at all, and a lie to the caller in its own
// right. A worker reads `native.maxValueBytes()` ONCE, in its constructor, and
// that number is arena geometry: `dataBytes / 2` less the entry header and the
// key bound. #recovered() re-reads the ring head, re-claims a submission ring
// and refreshes #ringMaxValue with it -- and left this one alone. So a worker
// that recovers onto a SMALLER arena keeps the limit it read from the larger
// one, accepts a value the new arena's log allocator will refuse, and returns
// `true` for a write that can never land.
//
// It is alone in that class, checked rather than assumed: #keyMax comes from
// KEY_MAX, a compile-time constant with no arena behind it (binding.cc's
// KeyMaxBytes does not even take NEED_STORE), and #ringMaxValue is read inside
// #useSubmissionRing, which #recovered calls, and is consulted only while
// #ringIdx >= 0 -- which nothing but that same function sets.
//
// THE HANDSHAKE BELOW IS NOT DECORATION, and getting it wrong is how this test
// first failed on Windows and only on Windows. A replacement primary calls
// shmCreate, and on Windows that is CreateFileMappingA, which returns
// ERROR_ALREADY_EXISTS while ANY process still holds a handle to the name --
// deliberately, because Windows cannot unlink an object others hold, so an
// existing name means a live primary (see platform.h, and decision 41, which
// calls a degraded worker's detach "mandatory, not hygiene" for exactly this).
// POSIX takes the other branch and shm_unlinks first, so it never noticed.
//
// A worker releases its mapping in #degrade -- which unmaps the arena AND
// destroys its submission segment -- but #degrade only runs from
// #checkPrimary, inside #drain, which runs on an OPERATION. A worker sitting
// idle on `await` does none, so it never notices and never lets go. The first
// version of this test waited on a CLOCK (`sleep(2600)`, past primaryStaleMs)
// while the child did nothing at all, and re-created the arena with the child
// still holding it.
//
// So the child drives its own degradation and REPORTS it, and the parent
// creates the replacement only then. The two assertions before that report
// make the platform property a checked invariant on every platform, rather
// than something only a Windows runner can tell us about.
if (process.env.TCR5_ROLE === 'maxvalue') {
    (async () => {
        // 1.5MB: inside the 16MB arena's 4193280B limit and inside the 8MB
        // submission ring's 4193248B one, outside the 2MB arena's 949048B
        // limit. So the ARENA bound is the only thing that can decide it, and
        // a pass here cannot be the ring's doing.
        const BIG = Buffer.alloc(1536 * 1024, 7);
        const w = TurboKV.attachWorker(process.env.TCR5_ARENA, 1,
            { storage: 'bytes', primaryStaleMs: 1000 });
        w.get('poke');
        ok(w.set('big', BIG) === true, 'the large arena accepts a 1.5MB value');

        // The parent closes the old primary and waits for us to LET GO.
        process.send({ step: 'swap' }); await step('closed');
        for (let i = 0; i < 400 && !w.primaryDead; i++) { w.get('probe'); await sleep(50); }
        ok(w.primaryDead === true, 'the worker notices the primary is gone -- on an operation, not a clock');
        // #degrade unmaps the arena and destroys the submission segment, both
        // synchronously, before it returns. Asserted rather than assumed: it is
        // the precondition for the replacement primary being able to create the
        // name at all on Windows.
        ok(native.stats() === undefined || native.stats() === null,
           `and RELEASES the arena mapping with it (${JSON.stringify(native.stats())})`);
        const ss = TurboKV.submitStats();
        ok(ss === null || ss.enabled === 0, `and the submission segment too (${JSON.stringify(ss)})`);
        process.send({ step: 'released' }); await step('swapped');

        for (let i = 0; i < 400 && (w.stats.recoveries || 0) === 0; i++) { w.get('probe'); await sleep(50); }
        ok((w.stats.recoveries || 0) === 1, `the worker recovered (${w.stats.recoveries || 0})`);
        ok(w.stats.lastRecovery && w.stats.lastRecovery.sameArena === false,
           'onto a DIFFERENT arena, which is the whole condition');
        ok(w.transport === 'shm', 'with its submission ring re-claimed');

        const acked = w.set('big', BIG);
        ok(acked === false,
           `a value the new arena cannot hold is REFUSED, not acked (${acked})`);
        ok(/exceeds the \d+B arena limit/.test(w.lastError || ''),
           `and the caller is told which limit (${w.lastError})`);
        ok((w.stats.rejectedSize || 0) >= 1, `counted as a size rejection (${w.stats.rejectedSize})`);
        // NON-VACUITY: the refreshed limit is the NEW arena's, not zero. A
        // refresh that simply clamped everything to nothing would pass the
        // assertion above and break every write the worker makes.
        ok(w.set('ordinary', 'V') === true, 'while an ordinary write still succeeds');
        ok(w.set('mid', Buffer.alloc(400 * 1024, 3)) === true,
           'and so does one that only the OLD limit would have had to allow');
        process.send({ step: 'bye' }); await step('byebye');
        done(w, 'recovered-maxvalue');
    })();
    return;
}

// ------------------------------- N2: a pending write is not a pending delete
//
// The guard at the bottom of #promotionBlock reports THIS worker's own
// undrained `minLevel: 2` write. `l3PromotionsBlockedSelf` blocks the
// PLACEMENT and still hands the caller what L3 returned;
// `l3DeletedWhileReading` answers `undefined` and counts a prevented
// resurrection. Reverting that one token passed all 41 suites.
//
// Reaching it needs the L3 queue to have let go of the set already -- so the
// write is awaited -- and no L1 entry to carry the self-mark, which is exactly
// what `minLevel: 2` produces.
if (process.env.TCR5_ROLE === 'blockself') {
    (async () => {
        const f = makeFake();
        f.store.set('b', { value: 'L3OLD', expiresAt: 0 });
        const w = TurboKV.attachWorker(process.env.TCR5_ARENA, 1, { storage: 'bytes', l3: f.adapter });
        w.get('poke');
        ok(await w.setAsync('b', 'NEW', { minLevel: TurboKV.L2 }) === true,
           'the minLevel-2 write is acked by L3');
        const m = w.__unsafeMarkState();
        ok(m.writtenKeys.indexOf('b') >= 0, `and its WRITE mark is still outstanding (${JSON.stringify(m.writtenKeys)})`);
        ok(m.pendingKeys.indexOf('b') < 0, 'with no removal mark anywhere');
        ok(w.__unsafeOutstandingKind('b') === undefined,
           `the L3 queue has let go of it, so the guard reaches the mark (${w.__unsafeOutstandingKind('b')})`);

        const before = f.calls.filter(c => c[0] === 'get').length;
        const v = await w.getAsync('b');
        ok(f.calls.filter(c => c[0] === 'get').length === before + 1, 'the read did reach L3');
        // The reason blocks the PLACEMENT ONLY.
        ok(v === 'NEW', `and the caller is handed what L3 returned (${JSON.stringify(v)})`);
        ok((w.stats.l3PromotionsBlockedSelf || 0) >= 1,
           `counted as this worker's own write (${w.stats.l3PromotionsBlockedSelf})`);
        ok((w.stats.l3DeletedWhileReading || 0) === 0,
           `and NOT as a prevented resurrection (${w.stats.l3DeletedWhileReading})`);
        ok(native.get('b') === 'V-ARENA-B',
           `nothing was promoted into the shared arena (${JSON.stringify(native.get('b'))})`);
        done(w, 'promotion-block-self');
    })();
    return;
}

// -------------------------------------- N3: minLevel 3 rode the delete mark
//
// `set(k, v, { minLevel: 3 })` keeps the value out of L1 AND out of L2, so it
// submits a DELETE of the L2 copy -- and that submission used to take a
// REMOVAL mark. #deletedHere then answered true, and getAsync refuses to ask
// L3 at all for a key it believes this worker removed. The value lives only in
// L3 by construction, so the acked write was unreadable through every form.
//
// The mark it wants is the WRITE mark: what is in flight is "L2's copy is
// superseded", not "this key is gone" -- the key is very much there, one tier
// down. That keeps local reads MISSING on L2's older copy (which is what the
// mark is for) while leaving L3 reachable.
if (process.env.TCR5_ROLE === 'ml3') {
    (async () => {
        const f = makeFake();
        const w = TurboKV.attachWorker(process.env.TCR5_ARENA, 1, { storage: 'bytes', l3: f.adapter });
        w.get('poke');
        ok(w.get('m1') === 'V-ARENA-M1', 'the arena holds an older copy of the key');

        // ---- the ASYNC form
        ok(await w.setAsync('m1', 'L3ONLY', { minLevel: TurboKV.L3 }) === true, 'setAsync(minLevel:3) is acked');
        ok(f.store.get('m1').value === 'L3ONLY', 'and the value is in L3');
        const marks = w.__unsafeMarkState();
        ok(marks.writtenKeys.indexOf('m1') >= 0,
           `the L2 eviction took a WRITE mark (${JSON.stringify(marks.writtenKeys)})`);
        ok(marks.pendingKeys.indexOf('m1') < 0,
           `and NOT a removal mark (${JSON.stringify(marks.pendingKeys)})`);
        ok(marks.deletedAtKeys.indexOf('m1') < 0,
           `and nothing recorded it as a removal for the promotion guard (${JSON.stringify(marks.deletedAtKeys)})`);

        ok(w.get('m1') === undefined, 'get() misses: the value was deliberately kept out of L1 and L2');
        let n = f.calls.filter(c => c[0] === 'get').length;
        ok(await w.getAsync('m1') === 'L3ONLY',
           `getAsync reads it back from L3 (${JSON.stringify(await w.getAsync('m1'))})`);
        ok(f.calls.filter(c => c[0] === 'get').length > n, 'having actually asked the adapter');

        // ---- the SYNC form, same effects
        ok(w.set('m2', 'L3ONLY2', { minLevel: TurboKV.L3 }) === true, 'set(minLevel:3) is accepted');
        const m2 = w.__unsafeMarkState();
        ok(m2.writtenKeys.indexOf('m2') >= 0, `it takes the same WRITE mark (${JSON.stringify(m2.writtenKeys)})`);
        ok(m2.pendingKeys.indexOf('m2') < 0, 'and no removal mark');
        for (let i = 0; i < 60 && !f.store.has('m2'); i++) await sleep(10);
        ok(f.store.has('m2'), 'the queued value reaches L3');
        ok(w.get('m2') === undefined, 'get() misses for it too');
        n = f.calls.filter(c => c[0] === 'get').length;
        ok(await w.getAsync('m2') === 'L3ONLY2',
           `and getAsync reads it back from L3 (${JSON.stringify(await w.getAsync('m2'))})`);
        ok(f.calls.filter(c => c[0] === 'get').length > n, 'having asked the adapter for it as well');

        // ---- and once the primary applies the eviction, nothing changes
        process.send({ step: 'drain' }); await step('drained');
        await sleep(20); w.get('poke');
        ok(native.get('m1') === undefined, 'the L2 copy is evicted once the primary drains');
        ok(w.get('m1') === undefined, 'get() still misses');
        ok(await w.getAsync('m1') === 'L3ONLY', 'and getAsync still reads L3');
        ok(w.__unsafeMarkState().writtenKeys.indexOf('m1') < 0, 'the mark is released by its own record');

        // A REAL DELETE still takes the removal mark. Without this the case
        // above would pass for a build that simply stopped marking.
        ok(w.delete('d1') === true, 'a real delete is accepted');
        const m3 = w.__unsafeMarkState();
        ok(m3.pendingKeys.indexOf('d1') >= 0, `and takes a REMOVAL mark (${JSON.stringify(m3.pendingKeys)})`);
        ok(m3.writtenKeys.indexOf('d1') < 0, 'not a write mark');
        n = f.calls.filter(c => c[0] === 'get').length;
        ok(await w.getAsync('d1') === undefined, 'and getAsync refuses it');
        ok(f.calls.filter(c => c[0] === 'get').length === n, 'without asking L3, which is what a removal means');
        done(w, 'minlevel3-mark');
    })();
    return;
}

// ------------------------------------------------------------------ parent
function runChild(role, arena, primaryOpts, onStep, before, arenaBytes = 16 << 20, indexSlots = 1 << 14) {
    return new Promise((resolve) => {
        const p = TurboKV.createPrimary(arena, arenaBytes, indexSlots,
            { storage: 'bytes', maintenance: false, ...primaryOpts });
        if (before) before(p);
        const held = [];
        const state = {
            drain: true,
            flushHeld() { while (held.length) TurboKV.applyBatch(held.shift()); },
        };
        const kid = fork(__filename, [], {
            env: { ...process.env, TCR5_ROLE: role, TCR5_ARENA: arena }, stdio: 'inherit',
        });
        kid.on('message', (m) => {
            if (TurboKV.isCacheMessage(m)) {
                if (state.drain) TurboKV.applyBatch(m); else held.push(m);
                return;
            }
            if (m && m.t === 'tcr') { if (state.drain) TurboKV.drainSubmissions(20000); return; }
            if (m && m.step) onStep(m.step, m, p, state, kid);
        });
        kid.on('exit', async (code) => { await p.close(); resolve(code); });
    });
}
const drainAll = () => { let g = 0; while (TurboKV.drainSubmissions(8192) > 0 && ++g < 200); };
// A full lap of the invalidation ring, written synchronously so no event loop
// turn happens: no doorbell, no backstop, nothing drained. `ringCap` is 32768
// records at the 16MB default (see store.h), so 70000 laps it comfortably.
const lapTheRing = (p) => { for (let i = 0; i < 70000; i++) p.set('filler' + (i % 50), 'x'); };

(async () => {
    for (const role of ['wrapres', 'wrapresipc']) {
        const code = await runChild(role, ARENA + (role === 'wrapres' ? '1' : '2'),
            role === 'wrapresipc' ? { transport: 'ipc' } : {},
            (s, m, p, state, kid) => {
                if (s === 'wrap') { state.drain = false; lapTheRing(p); kid.send({ step: 'wrapped' }); }
                if (s === 'drain') { state.drain = true; state.flushHeld(); drainAll(); kid.send({ step: 'drained' }); }
                // Consume the submission FIRST, then lap the ring so the record
                // that would have cleared the mark is gone.
                if (s === 'drainwrap') { drainAll(); state.drain = false; lapTheRing(p); kid.send({ step: 'drainwrapped' }); }
            }, (p) => { p.set('k2', 'OLD'); });
        ok(code === 0, `the ${role} cases passed (child exited ${code})`);
    }
    {
        // Its own runner: this is the one case where the primary is REPLACED
        // mid-test by a differently sized one, which runChild's single `p` has
        // no shape for. Heartbeats are left on -- a worker cannot notice a
        // death, let alone a recovery, without them.
        //
        // The replacement is created ONLY after the child reports it has let
        // go of the old mapping. On Windows a held handle makes shmCreate
        // refuse the name outright; on POSIX it would merely be unlinked from
        // under the child. See the child's comment.
        const arena = ARENA + '6';
        const ringOpts = { storage: 'bytes', submitRings: 2, submitRingBytes: 8 << 20 };
        let p2 = null;
        const code = await new Promise((resolve) => {
            const p1 = TurboKV.createPrimary(arena, 16 << 20, 1 << 14, ringOpts);
            const kid = fork(__filename, [], {
                env: { ...process.env, TCR5_ROLE: 'maxvalue', TCR5_ARENA: arena }, stdio: 'inherit',
            });
            kid.on('message', async (m) => {
                if (TurboKV.isCacheMessage(m)) { TurboKV.applyBatch(m); return; }
                if (m && m.t === 'tcr') { TurboKV.drainSubmissions(20000); return; }
                if (m && m.step === 'swap') { await p1.close(); kid.send({ step: 'closed' }); }
                // Not a clock: the child has proved it detached.
                if (m && m.step === 'released') {
                    p2 = TurboKV.createPrimary(arena, 2 << 20, 1 << 12, ringOpts);
                    kid.send({ step: 'swapped' });
                }
                if (m && m.step === 'bye') kid.send({ step: 'byebye' });
            });
            kid.on('exit', async (c) => { if (p2) await p2.close(); resolve(c); });
        });
        ok(code === 0, `the recovered-maxvalue cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('rejectset', ARENA + '5', {}, (s, m, p, state, kid) => {
            // One rejection, then drain: tail advances past a record the arena
            // never took.
            if (s === 'reject-drain') { native.__unsafeRejectDrainSets(1); drainAll(); kid.send({ step: 'reject-drained' }); }
            if (s === 'reject-wrap') {
                native.__unsafeRejectDrainSets(1); drainAll();
                state.drain = false; lapTheRing(p);
                kid.send({ step: 'reject-wrapped' });
            }
            if (s === 'report') {
                ok(m.arena1 === undefined,
                   `the shared arena holds nothing for the rejected write either (${JSON.stringify(m.arena1)})`);
                ok(m.arena2 === undefined,
                   `nor for the one a wrap followed (${JSON.stringify(m.arena2)})`);
                kid.send({ step: 'bye' });
            }
        }, (p) => { p.set('r1', 'OLD'); p.set('r2', 'OLD2'); });
        ok(code === 0, `the rejected-set cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('blockself', ARENA + '3', {}, () => {},
            (p) => { p.set('b', 'V-ARENA-B'); });
        ok(code === 0, `the promotion-block-self cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('ml3', ARENA + '4', {}, (s, m, p, state, kid) => {
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
        }, (p) => { p.set('m1', 'V-ARENA-M1'); p.set('d1', 'V-ARENA-D1'); });
        ok(code === 0, `the minLevel-3 cases passed (child exited ${code})`);
    }

    console.log(fail ? `\n  ${fail} FAILED` : '\n  [review5] all passed');
    process.exit(fail ? 1 : 0);
})();
