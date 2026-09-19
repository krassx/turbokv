'use strict';
// Regressions for the THIRD adversarial review, and for the architectural
// change made in answer to it.
//
// THE RULE: only the primary may write L2 with L3-derived data. A worker that
// reads through to L3 fills its own L1 and stops there, and the failure cap it
// used to submit as a blind re-write is now a conditional operation the
// primary applies. test/write_sites_test.js enforces the shape of that; this
// file is the behaviour, plus the defects the rule does NOT remove.
//
// Every worker case runs in a forked child against a real primary, because
// every one of them is about the gap between a worker's write and the moment
// the primary applies it -- which does not exist in one process.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');

let fail = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCR3_ARENA || ('/tcr3' + process.pid);

// A child waits for the parent to say a step is done.
const step = (want) => new Promise((r) => {
    const h = (m) => { if (m && m.step === want) { process.off('message', h); r(); } };
    process.on('message', h);
});
const done = (c, label) => {
    console.log(fail ? `  ${fail} failed (${label})` : `  [review3] ${label} cases passed`);
    if (c) c.close();
    process.exit(fail ? 1 : 0);
};

// ---------------------------------------------------------- F1: the self-mark
// A worker's getAsync is in flight; the same worker sets the key, the write
// lands in L1 and the ring and L3 ACKNOWLEDGES it. The L3 GET then answers from
// the pre-write value -- ordinary for a store that reads a snapshot -- and the
// promotion used to overwrite the worker's own L1 with it, so the caller who
// had awaited `setAsync === true` read the old value back from the very
// instance it had written through.
//
// Nothing existing could see it: the queue released the SET at the ack,
// #pendingDel and #deletedAt cover only removals, and the ring carries no
// record for a write the primary has not drained.
if (process.env.TCR3_ROLE === 'self') {
    (async () => {
        const f = makeFake();
        f.store.set('k', { value: 'OLD', expiresAt: 0 });
        f.latency.set('set', 10);
        // Snapshot-first: the value is read when the request is issued and the
        // reply is slow, which is what puts the pre-write value in flight.
        f.adapter.get = async (key) => {
            const rec = f.store.get(key);
            await sleep(120);
            return rec ? { value: rec.value } : undefined;
        };
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 5000 });
        w.get('poke');                                  // catch up on the ring
        process.send({ step: 'hold' }); await step('held');

        const readP = w.getAsync('k');
        await sleep(10);
        const acked = await w.setAsync('k', 'NEW');
        ok(acked === true, `the worker's set was accepted all the way to L3 (${acked})`);
        ok(f.store.get('k').value === 'NEW', 'and L3 holds it');
        ok(w.get('k') === 'NEW', 'the worker reads its own write before the read returns');

        const v = await readP;
        ok(v === 'OLD', `the overlapping read still returns what L3 gave it (${v})`);
        ok(w.get('k') === 'NEW',
           `and the worker STILL reads its own acked write afterwards (${w.get('k')})`);
        ok((w.stats.l3PromotionsBlockedSelf || 0) >= 1,
           `the promotion was refused as this process's own write (${w.stats.l3PromotionsBlockedSelf})`);

        process.send({ step: 'release' }); await step('released');
        w.get('poke');
        ok(native.get('k') === 'NEW', `and the arena agrees once it is drained (${native.get('k')})`);
        done(w, 'self-mark');
    })();
    return;
}

// -------------------------------------------- F2/F3: the cap is conditional
// The cap used to be a blind re-write the worker submitted after comparing the
// arena itself, with a hop between the compare and the apply. Anything the
// primary did inside that hop was overwritten.
if (process.env.TCR3_ROLE === 'cap') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 10, l3FailTtlMs: 5000 });

        // NON-VACUITY FIRST: a key nothing supersedes really does get the
        // deadline, so the refusals below are refusals rather than a cap that
        // never ran at all.
        ok(await w.setAsync('solo', 'REFUSED') === false, 'the L3 write failed');
        let rem = 0;
        for (let i = 0; i < 80 && rem <= 0; i++) {
            await sleep(25); w.get('poke');
            if (native.get('solo') !== undefined) rem = native.lastTtlRemainingMs();
        }
        ok(rem > 0 && rem <= 5000, `an untouched key is capped in the shared arena (${rem})`);

        // A NEWER WRITE FROM THE PRIMARY, landing between the moment the cap
        // was taken and the moment it is applied. The parent HOLDS the cap
        // back so that window is a fact rather than a race: whatever route the
        // cap takes -- the ring it used to take, or the request it takes now
        // -- it is delivered only after the primary has written.
        process.send({ step: 'hold' }); await step('held');
        ok(await w.setAsync('race', 'A-REFUSED') === false, 'a second L3 write failed');
        process.send({ step: 'drain' }); await step('drained');     // A is in the arena
        await sleep(80);                                            // a cap poll's worth
        process.send({ step: 'newer' }); await step('newer-done');  // primary writes B
        process.send({ step: 'release' }); await step('released');  // NOW the cap is delivered
        await sleep(80); w.get('poke');
        ok(native.get('race') === 'B-NEWER',
           `the primary's newer value stands (${JSON.stringify(native.get('race'))})`);
        ok(native.lastTtlRemainingMs() === 0,
           `and keeps its own (absent) deadline (${native.lastTtlRemainingMs()})`);

        // A DELETE FROM THE PRIMARY, same window. A cap that lands on top of
        // it resurrects the key box-wide for l3FailTtlMs.
        process.send({ step: 'hold' }); await step('held');
        ok(await w.setAsync('gone', 'A-REFUSED') === false, 'a third L3 write failed');
        process.send({ step: 'drain2' }); await step('drained2');
        await sleep(80);
        process.send({ step: 'remove' }); await step('removed');
        process.send({ step: 'release' }); await step('released');
        await sleep(80); w.get('poke');
        ok(native.get('gone') === undefined,
           `a cap never resurrects a key the primary deleted (${JSON.stringify(native.get('gone'))})`);

        // F3 ON THE WORKER PATH: the cap is a CEILING, never a new lease. The
        // value's own TTL is shorter than l3FailTtlMs here, so the cap has
        // nothing to do and must leave the deadline alone rather than pushing
        // it out to `now + cap`.
        const wt = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 1500, l3FailTtlMs: 60000 });
        const t0 = Date.now();
        ok(await wt.setAsync('ttl', 'v', { ttlMs: 3000 }) === false, 'a write with a TTL of its own failed');
        const failedAt = Date.now() - t0;
        process.send({ step: 'drain3' }); await step('drained3');
        await sleep(80);                                            // a cap poll's worth
        process.send({ step: 'drain4' }); await step('drained4');
        native.get('ttl');
        const left = native.lastTtlRemainingMs();
        ok(left > 0, `the entry is still there with a deadline (${left})`);
        ok(left <= 3000 - failedAt + 400,
           `and the cap did not extend it past its own TTL (${left} left of ${3000 - failedAt})`);
        wt.close();
        done(w, 'conditional-cap');
    })();
    return;
}

// ------------------------------- F4a: a minLevel-2 set shed by a full ring
// The mark that makes this worker's reads miss until the write comes back
// around the ring was taken BEFORE the submit. A full ring then shed the
// write, no record ever came back, and the key was unreadable from every tier
// -- L3 included -- for the life of the worker.
if (process.env.TCR3_ROLE === 'ringshed') {
    (async () => {
        const f = makeFake();
        f.store.set('k', { value: 'IN-L3', expiresAt: 0 });
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 50 });
        w.get('poke');
        const big = 'x'.repeat(3000);
        let n = 0;
        while (!(w.stats.writesShed > 0) && n < 10000) { w.set('fill' + n, big); n++; }
        ok(w.stats.writesShed > 0, `the submission ring is full (after ${n} writes)`);

        const before = w.stats.writesShed;
        ok(w.set('k', big, { minLevel: TurboKV.L2 }) === true, 'a minLevel:L2 set is accepted');
        ok(w.stats.writesShed > before, 'and shed by the full ring');
        ok(w.__unsafeMarkState().pendingKeys.indexOf('k') < 0,
           `a shed write leaves no mark behind (${JSON.stringify(w.__unsafeMarkState().pendingKeys)})`);
        ok(w.get('k') === 'V1', `so the key is still readable from L2 (${JSON.stringify(w.get('k'))})`);
        ok(w.has('k') === true, 'and has() agrees');
        ok(await w.getAsync('k') === 'V1', 'and the async form does too');
        done(w, 'ring-shed');
    })();
    return;
}

// ------------------------- F4b: an IPC batch shed inside flush(), later on
// On `transport: 'ipc'` both a delete and a minLevel >= 2 set mark before the
// outbox push, and the shed happens later, in flush(), which had no unmark.
if (process.env.TCR3_ROLE === 'ipcshed') {
    (async () => {
        const f = makeFake();
        f.store.set('k', { value: 'IN-L3', expiresAt: 0 });
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', transport: 'ipc', l3: f.adapter, l3RetryMs: 50,
              maxInFlightBytes: 0, outboxMaxBytes: 256 });
        w.get('poke');                       // drain the pre-attach records first
        ok(w.delete('k') === true, 'the worker deletes a key that is in L2');
        ok(w.set('m2', 'V2', { minLevel: TurboKV.L2 }) === true, 'and writes another at minLevel:L2');
        for (let i = 0; i < 6; i++) w.set('pad' + i, 'y'.repeat(100));
        await sleep(60);
        ok(w.stats.writesShed > 0, `flush() shed the batch (${w.stats.writesShed})`);
        ok(w.__unsafeMarkState().pendingKeys.length === 0,
           `and unmarked what it dropped (${JSON.stringify(w.__unsafeMarkState().pendingKeys)})`);
        ok(w.get('k') === 'V1', `the deleted key is readable again, honestly (${JSON.stringify(w.get('k'))})`);
        ok(w.get('m2') === 'V1', `and so is the minLevel:L2 one (${JSON.stringify(w.get('m2'))})`);
        ok(await w.getAsync('k') === 'V1', 'and neither is cut off from L3 either');

        // AND A CAP REQUEST SHED BY THE SAME BRANCH IS COUNTED. It is a cap
        // the primary will now never apply, which is the third of the ways
        // l3FailTtlUnapplied can move -- and the one most easily lost among
        // the writes it is shed with.
        f.fail.set('set', new Error('l3 down'));
        ok(await w.setAsync('capme', 'V') === false, 'a write whose L3 half failed');
        ok((w.stats.l3FailTtlApplied || 0) >= 1, 'its L1 copy was capped');
        for (let i = 0; i < 8; i++) w.set('pad2' + i, 'z'.repeat(100));   // force the shed
        await sleep(60);
        ok((w.stats.l3FailTtlUnapplied || 0) >= 1,
           `and the shed cap request is counted (${w.stats.l3FailTtlUnapplied})`);
        done(w, 'ipc-shed');
    })();
    return;
}

// --------------------- F5: a mark cleared by a record that predates the delete
// The mark was matched by hash alone, so ANY record for the key cleared it --
// including the primary's own write, sitting undrained on the ring since
// before the delete was issued. The deleted value was then served, and
// promoted.
if (process.env.TCR3_ROLE === 'oldrecord') {
    (async () => {
        const f = makeFake();
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 50 });
        w.get('poke');                                 // caught up with the ring
        process.send({ step: 'primary-set' }); await step('primary-set-done');
        // Deliberately NOT drained here: the primary's record for this key is
        // on the ring and this worker has not seen it.
        ok(native.get('k') === 'V1', `the arena holds the primary's write (${native.get('k')})`);
        ok(w.delete('k') === true, 'the worker deletes it');
        ok(w.get('k') === undefined,
           `and its own read misses, rather than being served by the older record (${JSON.stringify(w.get('k'))})`);
        ok(w.has('k') === false, 'has() agrees');
        ok(w.__unsafeMarkState().pendingKeys.indexOf('k') >= 0, 'the mark survived the older record');
        // ...and is cleared by the real one, so the fix is not just "never
        // clear the mark", which would be F4's hole through another door.
        process.send({ step: 'drain' }); await step('drained');
        for (let i = 0; i < 40 && w.__unsafeMarkState().pendingKeys.indexOf('k') >= 0; i++) {
            await sleep(25); w.get('poke');
        }
        ok(w.__unsafeMarkState().pendingKeys.indexOf('k') < 0, 'and cleared by the record for the delete itself');
        done(w, 'old-record');
    })();
    return;
}

// ------------- the shm/IPC coupling: a lost cap must become a number -----
// `transport: 'shm'` moves the WRITES off the cluster channel; it does not
// take a worker off it. A worker's l3FailTtlMs re-time is a REQUEST the
// primary applies, and it travels as a cluster message on both transports --
// so a consumer that routes the ring doorbell but never calls applyBatch
// leaves the value L3 refused in the SHARED arena with no expiry, box-wide,
// forever. The stats used to say that worked: l3FailTtlApplied moved for the
// L1 half and nothing at all moved on the other side.
if (process.env.TCR3_ROLE === 'lostcap') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 200, l3FailTtlMs: 100 });
        ok(w.transport === 'shm', `the worker is on the shm transport (${w.transport})`);

        // THE WRITE REACHES L2 BEFORE THE CAP IS TAKEN, deliberately: the
        // cap's ring mark is then PAST this worker's own record for the key,
        // so the retirement walk contains no record of ours at all and the
        // verdict can be proven rather than merely suspected. The parent
        // routes the ring doorbell, so this is one hop.
        w.set('lost', 'REFUSED');
        for (let i = 0; i < 80 && native.get('lost') === undefined; i++) { await sleep(25); w.get('poke'); }
        ok(native.get('lost') === 'REFUSED', `the write reached the shared arena (${native.get('lost')})`);
        await w.drainL3();                       // the L3 write is abandoned; the cap is taken now
        await sleep(50);
        ok(w.__unsafeCapState().caps === 1, `a read guard is outstanding (${w.__unsafeCapState().caps})`);
        ok(native.get('lost') === 'REFUSED' && native.lastTtlRemainingMs() === 0,
           `and the arena still holds it with no expiry (${native.lastTtlRemainingMs()})`);
        ok((w.stats.l3FailTtlApplied || 0) >= 1,
           `the L1 half alone reports success (${w.stats.l3FailTtlApplied})`);

        // ...which is exactly why the other side has to move too. The parent
        // never calls applyBatch, so the request was sent and never applied.
        for (let i = 0; i < 240 && !(w.stats.l3FailTtlUnapplied > 0); i++) await sleep(25);
        ok((w.stats.l3FailTtlUnapplied || 0) === 1,
           `the silence is counted, once (${w.stats.l3FailTtlUnapplied})`);
        ok((w.stats.l3FailTtlUnconfirmed || 0) === 0,
           `in the bucket that means PROVEN, not the unknowable one (${w.stats.l3FailTtlUnconfirmed})`);

        // AND THE OTHER ORDERING IS NOT PROVEN, AND MUST NOT CLAIM TO BE. A
        // queue that sheds everything abandons the write in a microtask, so
        // the cap's mark is taken BEFORE this worker's own record for the key
        // exists -- and a record bearing our own writer id cannot be told
        // from one written by an ipc-transport worker whose id happens to
        // equal our ring slot plus one. Unknowable, not proven.
        const w2 = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 100 });
        ok(await w2.setAsync('amb', 'REFUSED') === false, 'a shed L3 write');
        for (let i = 0; i < 240 && !((w2.stats.l3FailTtlUnapplied || 0) + (w2.stats.l3FailTtlUnconfirmed || 0)); i++) {
            await sleep(25); w2.get('poke');
        }
        ok((w2.stats.l3FailTtlUnconfirmed || 0) === 1,
           `an own-writer record inside the window is unconfirmed (${w2.stats.l3FailTtlUnconfirmed})`);
        ok((w2.stats.l3FailTtlUnapplied || 0) === 0,
           `and never proven (${w2.stats.l3FailTtlUnapplied})`);
        w2.close();
        done(w, 'lost-cap');
    })();
    return;
}

// -- the writer id cannot identify us, and must not be trusted to ---------
// Shared-memory submissions are stamped with the RING SLOT plus one; an IPC
// batch is stamped with the sender's WRITER ID; nothing keeps those two
// spaces apart. A shm worker holding slot 0 compares against `mine = 1` and
// cannot tell its own record from one written by a `transport: 'ipc'` worker
// that was given id 1 -- so skipping "our own" record hid a genuine
// cross-process rewrite and reported a proven loss about somebody else's
// fresh write. Systematic in an oversubscribed or mixed-transport cluster.
//
// The worker here takes a cap that is never applied (the parent does not
// route its batches); a SECOND PROCESS on the ipc transport, with id 1, then
// writes byte-identical unbounded bytes. Nothing was lost -- someone rewrote
// the key -- and the honest verdict is `unconfirmed`, never `unapplied`.
if (process.env.TCR3_ROLE === 'collide') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 5,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 200, l3FailTtlMs: 120 });
        ok(w.transport === 'shm', `the worker is on the shm transport (${w.transport})`);
        // Its own record lands BEFORE the mark (see the lostcap role), so the
        // only record inside the window is the other process's.
        w.set('col', 'SAME');
        for (let i = 0; i < 80 && native.get('col') === undefined; i++) { await sleep(25); w.get('poke'); }
        ok(native.get('col') === 'SAME', `the write reached the arena (${native.get('col')})`);
        await w.drainL3();
        await sleep(50);
        ok(w.__unsafeCapState().caps === 1, `a read guard is outstanding (${w.__unsafeCapState().caps})`);

        // The colliding writer: a different PROCESS, ipc transport, id 1 --
        // which is this worker's ring slot (0) plus one.
        process.send({ step: 'collide' }); await step('collided');
        for (let i = 0; i < 80; i++) { await sleep(25); w.get('poke'); if (w.stats.invalidated > 0) break; }
        ok(native.get('col') === 'SAME' && native.lastTtlRemainingMs() === 0,
           `the rewrite is resident and unbounded, exactly as a lost cap looks ` +
           `(${native.get('col')}/${native.lastTtlRemainingMs()})`);

        await sleep(2400);                       // past deadline + L3_CAP_WINDOW_MS
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `a rewrite by a colliding writer id is never reported as proven (${w.stats.l3FailTtlUnapplied})`);
        ok((w.stats.l3FailTtlUnconfirmed || 0) === 1,
           `it is unknowable, and says so (${w.stats.l3FailTtlUnconfirmed})`);
        done(w, 'writer-collision');
    })();
    return;
}

// A bare ipc-transport writer, forked by the parent for the role above. Its
// writer id is what the primary stamps on the records applyBatch applies.
if (process.env.TCR3_ROLE === 'ipcwriter') {
    (async () => {
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1, { storage: 'bytes', transport: 'ipc' });
        w.set('col', 'SAME');
        w.flush();
        await sleep(200);
        w.close();
        process.exit(0);
    })();
    return;
}

// ------ the lost-cap counter must not fire on a CROSS-PROCESS rewrite ----
// The retirement check infers "never applied" from the capped bytes still
// being resident and unbounded. That inference is not sufficient on its own:
// the primary can APPLY the cap, the entry can then expire, and another
// process can write byte-identical bytes with no TTL -- at which point the
// bytes look lost again and the counter fires about a fresh, L3-agreed write.
// A same-process rewrite cannot do this (#cancelCaps drops the guard), so the
// false positive is exactly cross-process: in the cluster the counter is for.
//
// The invalidation ring is the oracle, asked the question it already answers
// for the promotion guard -- did anyone ELSE write this key since -- with our
// own records skipped by writer id, because the write being capped is one of
// them.
if (process.env.TCR3_ROLE === 'rewrite') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 10, l3FailTtlMs: 120 });
        ok(await w.setAsync('rw', 'SAME') === false, 'the L3 write failed');
        // The parent DOES route, so the cap really lands. Proven, not assumed.
        let rem = 0;
        for (let i = 0; i < 120 && rem <= 0; i++) {
            await sleep(20); w.get('poke');
            if (native.get('rw') !== undefined) rem = native.lastTtlRemainingMs();
        }
        ok(rem > 0 && rem <= 120, `the primary applied the cap (ttlRemaining ${rem})`);
        // Now ANOTHER PROCESS writes the same bytes, unbounded, inside the
        // guard's window -- so at retirement the bytes look resident and
        // unbounded exactly as an unapplied cap would.
        process.send({ step: 'rewrite' }); await step('rewritten');
        for (let i = 0; i < 80 && native.get('rw') !== 'SAME'; i++) { await sleep(20); w.get('poke'); }
        ok(native.get('rw') === 'SAME', `the rewrite is in the arena (${native.get('rw')})`);
        ok(native.lastTtlRemainingMs() === 0,
           `with no expiry, which is what the byte test alone would call lost (${native.lastTtlRemainingMs()})`);
        // Well past deadline + L3_CAP_WINDOW_MS.
        await sleep(2400);
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `the cap that DID land is not reported as lost (${w.stats.l3FailTtlUnapplied})`);
        ok((w.stats.l3FailTtlUnconfirmed || 0) === 0,
           `nor as unknowable (${w.stats.l3FailTtlUnconfirmed})`);
        ok((w.stats.l3FailTtlApplied || 0) >= 1, 'while the applied counter still moved');
        done(w, 'cross-process-rewrite');
    })();
    return;
}

// -- a guard that cannot be kept is counted, not dropped in silence -------
// #noteCap declines past L3_CAP_MAX while #retimeOutbox still sends, so
// without an entry nothing can ever judge those requests. A sustained outage
// against a hand-wired primary therefore under-counted from the 4097th
// outstanding cap onward -- the scale at which the number matters most.
if (process.env.TCR3_ROLE === 'capmax') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR3_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 5, l3FailTtlMs: 600000 });
        const N = 4200;                                  // past the 4096 bound
        for (let i = 0; i < N; i++) w.set('m' + i, 'V');
        await w.drainL3();
        await sleep(200);
        const st = w.__unsafeCapState();
        ok(st.caps === 4096, `the guard map is bounded (${st.caps})`);
        ok((w.stats.l3FailTtlApplied || 0) >= N, `every failed write capped L1 (${w.stats.l3FailTtlApplied})`);
        ok((w.stats.l3FailTtlUnconfirmed || 0) === N - 4096,
           `and every guard that could not be kept is counted, ONCE ` +
           `(${w.stats.l3FailTtlUnconfirmed} of ${N - 4096})`);
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `in the unknowable bucket, not the proven one (${w.stats.l3FailTtlUnapplied})`);
        done(w, 'cap-max');
    })();
    return;
}

// ------------------------------------------------------------------ parent
// One child at a time, each against an arena shaped for what it needs.
function runChild(role, arena, primaryOpts, onStep, before, route = true) {
    return new Promise((resolve) => {
        const p = TurboKV.createPrimary(arena, 16 << 20, 1 << 14,
            { storage: 'bytes', maintenance: false, ...primaryOpts });
        if (before) before(p);
        // `drain: false` HOLDS rather than drops: a held batch is applied on
        // release, so a test can place the primary's own write inside the
        // window between a cap being taken and being applied.
        const held = [];
        const state = {
            drain: true,
            flushHeld() { while (held.length) TurboKV.applyBatch(held.shift()); },
        };
        const kid = fork(__filename, [], {
            env: { ...process.env, TCR3_ROLE: role, TCR3_ARENA: arena }, stdio: 'inherit',
        });
        kid.on('message', (m) => {
            // A worker's cap is a REQUEST to the primary now and travels in the
            // ordinary IPC batch. This is a plain fork, not a cluster, so the
            // parent routes it the way install() would -- except while a test
            // is deliberately holding the primary still, because applyBatch
            // drains every submission ring before it applies anything.
            // `route: false` is a primary that routes the ring doorbell and
            // nothing else -- the hand-wired consumer this coupling traps.
            if (TurboKV.isCacheMessage(m)) {
                if (!route) return;
                if (state.drain) TurboKV.applyBatch(m); else held.push(m);
                return;
            }
            if (m && m.t === 'tcr') { if (state.drain) TurboKV.drainSubmissions(20000); return; }
            if (m && m.step) onStep(m.step, p, state, kid);
        });
        kid.on('exit', async (code) => { await p.close(); resolve(code); });
    });
}
const drainAll = () => { let g = 0; while (TurboKV.drainSubmissions(8192) > 0 && ++g < 200); };

(async () => {
    // --- single process: the primary's own paths -------------------------
    //
    // F3 ON THE PRIMARY. `set(k, v, {ttlMs: 2000})` whose L3 write fails at
    // 1513ms had the L2 cap written from NOW, so its remaining life jumped
    // back up and the value outlived what the caller asked for. The L1 half
    // compared deadlines and shortened only; the L2 half never read the
    // remaining TTL at all.
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const c = TurboKV.createPrimary(ARENA + 'a', 8 << 20, 1 << 13,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 1200, l3FailTtlMs: 60000 });
        const t0 = Date.now();
        ok(await c.setAsync('k', 'v', { ttlMs: 2000 }) === false, 'the L3 write failed');
        const failedAt = Date.now() - t0;
        native.get('k');
        const left = native.lastTtlRemainingMs();
        ok(left > 0, `the entry still has a deadline (${left})`);
        ok(left <= 2000 - failedAt + 100,
           `the cap did not extend it past its own TTL (${left} left of ${2000 - failedAt})`);
        await sleep(2100 - failedAt);
        ok(c.get('k') === undefined, `and it expires when the caller said (${JSON.stringify(c.get('k'))})`);
        ok(native.get('k') === undefined, 'in L2 as well as L1');
        await c.close();
    }

    // The same cap still SHORTENS when it has something to shorten: a write
    // with no TTL of its own, whose L3 write failed, must not live forever.
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const c = TurboKV.createPrimary(ARENA + 'b', 8 << 20, 1 << 13,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 50, l3FailTtlMs: 4000 });
        ok(await c.setAsync('k', 'v') === false, 'the L3 write failed');
        native.get('k');
        const left = native.lastTtlRemainingMs();
        ok(left > 0 && left <= 4000, `an untimed value is capped at l3FailTtlMs (${left})`);
        await c.close();
    }

    // F6: THE GUARD REASONS ARE ORDERED BY WHAT THEY CHANGE. `owed === 'set'`
    // blocks the placement and hands the caller what L3 returned; a clear
    // answers undefined. Asked in the old order, a read that started before a
    // clearAll() and happened to overlap a set of this process's own was
    // handed the very value the clear was removing, while `get()` for the same
    // key said undefined.
    {
        const f = makeFake();
        f.store.set('k', { value: 'OLD', expiresAt: 0 });
        f.latency.set('set', 200); f.latency.set('get', 100); f.latency.set('clear', 20);
        const c = TurboKV.createPrimary(ARENA + 'c', 8 << 20, 1 << 13,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 5000 });
        const setP = c.setAsync('k', 'NEW', { minLevel: TurboKV.L3 });   // in flight 200ms
        const readP = c.getAsync('k');                                    // in flight 100ms
        await sleep(10);
        const clearP = c.clearAsync();                                    // barrier behind the set
        const v = await readP;
        ok(v === undefined,
           `a read that overlaps a clear is refused, whatever else it overlaps (${JSON.stringify(v)})`);
        ok(c.get('k') === undefined, 'and the sync form agrees, as it always did');
        ok((c.stats.l3ClearedWhileReading || 0) >= 1,
           `and says so (cleared=${c.stats.l3ClearedWhileReading} self=${c.stats.l3PromotionsBlockedSelf})`);
        ok((c.stats.l3PromotionsBlockedSelf || 0) === 0,
           'rather than reporting the weaker reason it also matched');
        await setP; await clearP;
        await c.close();
    }

    // F8: `l3RetryMs: 0` disabled the per-attempt deadline outright, because
    // the deadline helper treats anything <= 0 as "no bound". The worst of it
    // is `clear`, which retries indefinitely by design: a hung adapter.clear()
    // then never settled, its generation stayed open, and EVERY process
    // sharing the arena served L3 misses for every key until the worker
    // exited. 0 is documented as a value only for l3CloseTimeoutMs.
    {
        const f = makeFake();
        let threw = null;
        try {
            const c = TurboKV.createPrimary(ARENA + 'd', 4 << 20, 1 << 12,
                { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 0 });
            c.close();
        } catch (e) { threw = e; }
        ok(threw !== null && /l3RetryMs/.test(threw.message),
           `l3RetryMs: 0 is refused loudly (${threw && threw.message.slice(0, 60)})`);
        for (const bad of [-1, NaN, Infinity, '2000']) {
            let t = null;
            try {
                const c = TurboKV.createPrimary(ARENA + 'e', 4 << 20, 1 << 12,
                    { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: bad });
                c.close();
            } catch (e) { t = e; }
            ok(t !== null, `and so is l3RetryMs: ${String(bad)}`);
        }
        // A HUNG CLEAR THEREFORE SETTLES. With the bound in place the clear's
        // attempt fails, the generation is closed, and this process stops
        // serving L3 misses for every key.
        const f2 = makeFake();
        f2.hang.add('clear');
        const c = TurboKV.createPrimary(ARENA + 'f', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f2.adapter, l3RetryMs: 60, l3CloseTimeoutMs: 200 });
        c.set('k', 'v');
        const cleared = c.clearAsync();
        f2.store.set('k', { value: 'L3', expiresAt: 0 });
        // A clear retries forever by design, so this is not awaited; what must
        // be true is that each ATTEMPT gives up, which is what lets close()
        // finish and the process exit.
        let settled = false;
        cleared.then(() => { settled = true; }, () => { settled = true; });
        await sleep(300);
        await c.close();
        await sleep(100);
        ok(settled === true, 'a hung adapter.clear() settles instead of hanging forever');
    }

    // A cache with NO ADAPTER is untouched by any of this.
    {
        const c = TurboKV.createPrimary(ARENA + 'g', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3RetryMs: 0 });
        c.set('k', 'v');
        ok(c.get('k') === 'v', 'a cache with no adapter still reads back');
        ok(await c.getAsync('k') === 'v', 'and its async form is still just get');
        ok(c.stats.l3FailTtlApplied === undefined,
           `no L3 counter is invented for it (${c.stats.l3FailTtlApplied})`);
        c.close();
    }

    // THE SAME SILENCE, REACHED THROUGH A CLOSED CHANNEL. flush() dropped an
    // `r` when `!process.connected` without counting either side, so a cap
    // that could not leave the process at all still read as success.
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const c = TurboKV.createPrimary(ARENA + 'i', 8 << 20, 1 << 13,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 30, l3FailTtlMs: 5000 });
        // A worker handle in this same process: no cluster channel, so its
        // outbox has nowhere to go.
        const w = new TurboKV({ storage: 'bytes', l3: f.adapter, workerId: 1,
                                l3RetryMs: 30, l3FailTtlMs: 5000, l3CloseTimeoutMs: 1 });
        ok(process.connected !== true, 'this process has no cluster channel');
        w.set('nochannel', 'V');
        for (let i = 0; i < 80 && !(w.stats.l3FailTtlUnapplied > 0); i++) await sleep(25);
        ok((w.stats.l3FailTtlUnapplied || 0) >= 1,
           `a cap that cannot leave the process is counted (${w.stats.l3FailTtlUnapplied})`);
        await w.close();
        await c.close();
    }

    // `attached: false` YIELDS #id === 0 IN ANY PROCESS. It is an undeclared
    // escape hatch past the constructor's "workerId 0 is the primary" guard,
    // so the rule's own check cannot rest on the id alone.
    {
        const f = makeFake();
        f.store.set('p', { value: 'L3VAL', expiresAt: 0 });
        const c = TurboKV.createPrimary(ARENA + 'j', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false });
        const hatch = new TurboKV({ storage: 'bytes', l3: f.adapter, attached: false });
        ok(await hatch.getAsync('p') === 'L3VAL', 'the escape-hatch handle reads through to L3');
        // It IS the primary process here, so the promotion is legitimate --
        // the check below is that the rule looks at the process, which is the
        // half a non-primary process would fail.
        ok(native.get('p') === 'L3VAL', `and in the primary process it may promote (${native.get('p')})`);
        hatch.close();
        await c.close();
    }

    // THE ARCHITECTURAL RULE, from the primary's side: its OWN promotion still
    // writes L2. The worker's refusal is checked in l3_guard_test.js, and a
    // refusal nobody balances is indistinguishable from promotion being
    // broken.
    {
        const f = makeFake();
        f.store.set('p', { value: 'L3VAL', expiresAt: 0 });
        const c = TurboKV.createPrimary(ARENA + 'h', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f.adapter });
        ok(await c.getAsync('p') === 'L3VAL', 'the primary reads through to L3');
        ok(native.get('p') === 'L3VAL', `and its promotion reaches L2 (${native.get('p')})`);
        await c.close();
    }

    // --- forked children -------------------------------------------------
    {
        const code = await runChild('self', ARENA + '1', {}, (s, p, state, kid) => {
            if (s === 'hold') { state.drain = false; kid.send({ step: 'held' }); }
            if (s === 'release') { state.drain = true; drainAll(); kid.send({ step: 'released' }); }
        });
        ok(code === 0, `the self-mark cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('cap', ARENA + '2', {}, (s, p, state, kid) => {
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
            if (s === 'hold') { state.drain = false; kid.send({ step: 'held' }); }
            if (s === 'release') { state.drain = true; state.flushHeld(); drainAll(); kid.send({ step: 'released' }); }
            if (s === 'newer') { p.set('race', 'B-NEWER'); kid.send({ step: 'newer-done' }); }
            if (s === 'drain2') { drainAll(); kid.send({ step: 'drained2' }); }
            if (s === 'remove') { p.delete('gone'); kid.send({ step: 'removed' }); }
            if (s === 'drain3') { drainAll(); kid.send({ step: 'drained3' }); }
            if (s === 'drain4') { drainAll(); kid.send({ step: 'drained4' }); }
        });
        ok(code === 0, `the conditional-cap cases passed (child exited ${code})`);
    }
    {
        // A ring small enough to fill, and a primary that never drains it.
        const code = await runChild('ringshed', ARENA + '3', { submitRingBytes: 64 * 1024 },
            () => {}, (p) => { p.set('k', 'V1'); });
        ok(code === 0, `the ring-shed cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('ipcshed', ARENA + '4', { transport: 'ipc' },
            () => {}, (p) => { p.set('k', 'V1'); p.set('m2', 'V1'); });
        ok(code === 0, `the ipc-shed cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('lostcap', ARENA + '6', {}, () => {}, undefined, false);
        ok(code === 0, `the lost-cap cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('rewrite', ARENA + '7', {}, (s, p, state, kid) => {
            // The PRIMARY is the other process here: it writes the same bytes
            // with no TTL, straight into the arena, inside the cap's window.
            if (s === 'rewrite') { p.set('rw', 'SAME'); kid.send({ step: 'rewritten' }); }
        });
        ok(code === 0, `the cross-process-rewrite cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('capmax', ARENA + '8', {}, () => {}, undefined, false);
        ok(code === 0, `the cap-max cases passed (child exited ${code})`);
    }
    {
        // `route: false` for the cap holder -- its request must never be
        // applied -- while the colliding writer, forked below, IS routed, so
        // its record really reaches the arena stamped with its own id.
        const code = await runChild('collide', ARENA + '9', {}, (s, p, state, kid) => {
            if (s !== 'collide') return;
            const w2 = fork(__filename, [], {
                env: { ...process.env, TCR3_ROLE: 'ipcwriter', TCR3_ARENA: ARENA + '9' }, stdio: 'inherit',
            });
            w2.on('message', (m) => { if (TurboKV.isCacheMessage(m)) TurboKV.applyBatch(m); });
            w2.on('exit', () => kid.send({ step: 'collided' }));
        }, undefined, false);
        ok(code === 0, `the writer-collision cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('oldrecord', ARENA + '5', {}, (s, p, state, kid) => {
            if (s === 'primary-set') { p.set('k', 'V1'); kid.send({ step: 'primary-set-done' }); }
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
        });
        ok(code === 0, `the old-record cases passed (child exited ${code})`);
    }

    console.log(fail ? `\n  ${fail} FAILED` : '\n  [review3] all passed');
    process.exit(fail ? 1 : 0);
})();
