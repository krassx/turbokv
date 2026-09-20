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

    console.log(fail ? `\n  ${fail} FAILED` : '\n  [review5] all passed');
    process.exit(fail ? 1 : 0);
})();
