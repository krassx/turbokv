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

// ------------------------------------------------------------------ parent
// One child at a time, each against an arena shaped for what it needs.
function runChild(role, arena, primaryOpts, onStep, before) {
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
            if (TurboKV.isCacheMessage(m)) { if (state.drain) TurboKV.applyBatch(m); else held.push(m); return; }
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
        const code = await runChild('oldrecord', ARENA + '5', {}, (s, p, state, kid) => {
            if (s === 'primary-set') { p.set('k', 'V1'); kid.send({ step: 'primary-set-done' }); }
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
        });
        ok(code === 0, `the old-record cases passed (child exited ${code})`);
    }

    console.log(fail ? `\n  ${fail} FAILED` : '\n  [review3] all passed');
    process.exit(fail ? 1 : 0);
})();
