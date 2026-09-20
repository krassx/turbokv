'use strict';
// Regressions for the third adversarial pass, and for the representation
// change made in answer to it.
//
// THE ROOT CAUSE: `#pendingDel` was an OVERLOADED MARK. A worker took it for a
// DELETE ("I removed this key") and for a `minLevel: 2` SET ("I wrote this key
// and it has not landed"), and every consumer had to guess which it was
// looking at. Four of the seven findings are one guess each:
//
//   F1  the l3FailTtlMs cap read a set's mark as "our own delete is on its
//       way" and answered 'moot' -- no request, no guard, no counter, and the
//       value L3 refused resident in the SHARED arena with no expiry.
//   F2  the wrapped-ring reconciliation keeps a mark while the arena still
//       holds the key. Right for a delete; exactly backwards for a set, whose
//       landing IS "the key is held".
//   F5  the L1 self-mark was released by ANY record for the hash, including
//       one written before our own write.
//   INFO a set's mark landed in #deletedAt and was counted as
//       `l3DeletedWhileReading`.
//
// There are now two mark sets, each naming what it holds (see PendingMarks),
// and the one consumer that genuinely wants either says so (#unappliedHere).
// test/write_sites_test.js enforces the SHAPE -- that no site can take or
// release a mark without choosing a kind -- and this file is the behaviour.
//
// The three independent findings are here too: F3 (a dropped batch that is
// not the shed branch leaves its marks behind), F4 (the primary's promotion
// guard is not ordered against a worker's undrained submission), F6 (the
// cap's value compare is `===`, so NaN and a lone surrogate are never capped).
//
// Every worker case runs in a forked child against a real primary: all of
// them are about the gap between a worker's submission and the moment the
// primary applies it, which does not exist in one process.
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');

let fail = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCR4_ARENA || ('/tcr4' + process.pid);

const step = (want) => new Promise((r) => {
    const h = (m) => { if (m && m.step === want) { process.off('message', h); r(m); } };
    process.on('message', h);
});
const done = (c, label) => {
    console.log(fail ? `  ${fail} failed (${label})` : `  [review4] ${label} cases passed`);
    if (c) c.close();
    process.exit(fail ? 1 : 0);
};

// ------------------------------------------------- F1: the cap for minLevel 2
// A `minLevel: 2` write is a WRITE, and the cap must treat it as one. With a
// shed queue the L3 outcome settles `false` in a microtask -- long before any
// record can clear a mark -- which is the ordinary case once a queue backs up
// during an outage, not a race. The cap used to read the mark as a pending
// delete and answer 'moot': nothing was requested, no guard entry was kept,
// neither failure counter moved, and `l3FailTtlApplied` still reported success
// from the L1 half alone.
if (process.env.TCR4_ROLE === 'cap2' || process.env.TCR4_ROLE === 'cap2ipc') {
    (async () => {
        const ipc = process.env.TCR4_ROLE === 'cap2ipc';
        const f = makeFake();
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 300,
              ...(ipc ? { transport: 'ipc' } : {}) });
        w.get('poke');
        ok(w.transport === (ipc ? 'ipc' : 'shm'), `the worker is on the ${ipc ? 'ipc' : 'shm'} transport`);

        // The control is the same write at minLevel 1, through the same shed
        // queue: it was always capped, so a failure below is about the LEVEL
        // and not about the cap being broken outright.
        ok(await w.setAsync('ctl', 'V') === false, 'a minLevel-1 write is refused by the shed queue');
        ok(await w.setAsync('sub', 'V', { minLevel: TurboKV.L2 }) === false, 'and so is the minLevel-2 one');

        // THE MARK IS A WRITE MARK, and nothing else. Checked before any read
        // drains the ring, so the mark is still outstanding.
        const m = w.__unsafeMarkState();
        ok(m.writtenKeys.indexOf('sub') >= 0, `the minLevel-2 write took a WRITE mark (${JSON.stringify(m.writtenKeys)})`);
        ok(m.pendingKeys.indexOf('sub') < 0, `and not a removal mark (${JSON.stringify(m.pendingKeys)})`);
        ok(m.deletedAtKeys.indexOf('sub') < 0,
           `and nothing recorded it as a removal for the promotion guard (${JSON.stringify(m.deletedAtKeys)})`);

        ok(w.__unsafeHasCap('sub') === true, 'a read guard is outstanding for it');
        ok((w.stats.l3FailTtlApplied || 0) >= 2, `and both caps were applied (${w.stats.l3FailTtlApplied})`);

        process.send({ step: 'drain' }); await step('drained');
        await sleep(60);
        process.send({ step: 'drain' }); await step('drained');
        w.get('poke');
        native.get('ctl'); const ctlRem = native.lastTtlRemainingMs();
        native.get('sub'); const subRem = native.lastTtlRemainingMs();
        ok(ctlRem > 0 && ctlRem <= 300, `the control is bounded in the shared arena (${ctlRem})`);
        ok(subRem > 0 && subRem <= 300, `and so is the minLevel-2 write (${subRem})`);

        await sleep(400);
        w.get('poke');
        ok(native.get('sub') === undefined,
           `the value L3 refused is gone from L2 at the cap (${JSON.stringify(native.get('sub'))})`);
        ok(w.get('sub') === undefined, 'and the worker no longer serves it');
        ok((w.stats.l3FailTtlUnapplied || 0) === 0,
           `with nothing counted as lost (${w.stats.l3FailTtlUnapplied})`);
        done(w, ipc ? 'minlevel2-cap [ipc]' : 'minlevel2-cap [shm]');
    })();
    return;
}

// F1 again, with an ORDINARY queue and a REAL adapter failure inside
// l3RetryMs, while the primary is merely slow to drain. The mark is still
// outstanding when the cap runs, which is all the old check needed.
if (process.env.TCR4_ROLE === 'slowcap') {
    (async () => {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 60, l3FailTtlMs: 400 });
        w.get('poke');
        process.send({ step: 'hold' }); await step('held');
        ok(await w.setAsync('sub', 'V', { minLevel: TurboKV.L2 }) === false, 'the L3 write failed');
        ok(w.__unsafeCapState().caps === 1,
           `the cap was requested while the write was still undrained (${w.__unsafeCapState().caps})`);
        process.send({ step: 'release' }); await step('released');
        await sleep(120); w.get('poke');
        native.get('sub'); const rem = native.lastTtlRemainingMs();
        ok(rem > 0 && rem <= 400, `and applied once the primary caught up (${rem})`);
        ok((w.stats.l3FailTtlApplied || 0) >= 1, `counted as applied (${w.stats.l3FailTtlApplied})`);
        await sleep(500); w.get('poke');
        ok(native.get('sub') === undefined, 'the refused value converges away');
        done(w, 'slow-primary-cap');
    })();
    return;
}

// -------------------------------------------- F2: a wrapped ring and a write
// The reconciliation asks the arena "is the key still held?" and keeps the
// mark when it is. That is the REMOVAL's question. A write's landing is
// exactly "the key is held", so the mark survived its own success and the key
// was unreadable from every tier -- L3 never asked -- for the life of the
// worker.
if (process.env.TCR4_ROLE === 'wrap') {
    (async () => {
        const f = makeFake();
        f.store.set('w2', { value: 'L3VAL', expiresAt: 0 });
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1,
            { storage: 'bytes', l3: f.adapter, l3RetryMs: 500 });
        w.get('poke');
        ok(w.set('w2', 'V2', { minLevel: TurboKV.L2 }) === true, 'a minLevel-2 write is accepted');
        process.send({ step: 'drain' }); await step('drained');   // the primary APPLIES it
        // A DELETE THE PRIMARY HAS NOT APPLIED, taken after that drain and
        // left outstanding across the wrap: its mark must SURVIVE, or this
        // test would pass for a fix that simply threw every mark away.
        ok(w.delete('d2') === true, 'and a delete of a key the primary holds');
        process.send({ step: 'wrap' }); await step('wrapped');

        ok(w.get('w2') === 'V2', `the written key is readable after the wrap (${JSON.stringify(w.get('w2'))})`);
        ok(w.has('w2') === true, 'has() agrees');
        ok(await w.getAsync('w2') === 'V2', 'and the async form does too');
        const m = w.__unsafeMarkState();
        ok(m.writtenKeys.indexOf('w2') < 0, `the write mark is released (${JSON.stringify(m.writtenKeys)})`);
        ok(m.pendingKeys.indexOf('d2') >= 0,
           `while the REMOVAL mark survives the same wrap (${JSON.stringify(m.pendingKeys)})`);
        ok(w.get('d2') === undefined, 'so the deleted key still misses locally');
        done(w, 'wrapped-ring');
    })();
    return;
}

// ------------------------------- F5: an older record dropping our own write
// The L1 self-mark was a boolean, so ANY record for the key's hash dropped the
// entry -- including the primary's own write, sitting undrained on the ring
// since before ours. `set(k, NEW)` then `get(k)` on the same worker returned
// the primary's OLD value, deterministically.
if (process.env.TCR4_ROLE === 'ownrec') {
    (async () => {
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1, { storage: 'bytes' });
        w.get('poke');                                  // caught up with the ring
        process.send({ step: 'primary-set' }); await step('primary-set-done');
        ok(native.get('k') === 'OLD', 'the arena holds the primary\'s undrained write');
        ok(w.set('k', 'NEW') === true, 'the worker writes the same key');
        ok(w.get('k') === 'NEW',
           `and reads its own write back, not the older record's value (${JSON.stringify(w.get('k'))})`);
        ok(w.stats.invalidated === 0, `the older record dropped nothing (${w.stats.invalidated})`);

        // NON-VACUITY: a record AT OR PAST our write still drops the entry.
        // "Never drop an own entry" would pass the assertion above and leave
        // this worker serving its own value after somebody else replaced it.
        process.send({ step: 'primary-overwrite' }); await step('primary-overwrite-done');
        ok(w.get('k') === 'NEWER',
           `a later record still invalidates our own entry (${JSON.stringify(w.get('k'))})`);
        ok(w.stats.invalidated >= 1, `and is counted (${w.stats.invalidated})`);
        done(w, 'own-record');
    })();
    return;
}

// ---------------------------------- F3: a batch dropped by a send that threw
// flush()'s SHED branch unmarked what it dropped. The synchronous-throw branch
// -- `process.send` refusing a value the serializer cannot represent, which
// one bigint does for the whole batch under JSON serialization -- dropped the
// batch, counted `flushDropped`, counted caps, and left the marks. A delete in
// that batch was then unreadable from every tier, L3 included, forever.
if (process.env.TCR4_ROLE === 'sendthrow') {
    (async () => {
        const f = makeFake();
        f.store.set('d', { value: 'L3VAL', expiresAt: 0 });
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1,
            { storage: 'bytes', transport: 'ipc', l3: f.adapter, l3RetryMs: 200 });
        w.get('poke');
        ok(w.delete('d') === true, 'the worker deletes a key the arena holds');
        ok(w.set('m2', 'V2', { minLevel: TurboKV.L2 }) === true, 'and writes another at minLevel:L2');
        ok(w.set('n', 7n) === true, 'and a bigint lands in the same batch');
        await sleep(40);
        ok((w.stats.flushDropped || 0) >= 1, `the flush threw and dropped the batch (${w.stats.flushDropped})`);
        ok(/BigInt/.test(w.lastError || ''), `and said so (${w.lastError})`);

        const m = w.__unsafeMarkState();
        ok(m.pendingKeys.indexOf('d') < 0, `the removal mark went with it (${JSON.stringify(m.pendingKeys)})`);
        ok(m.writtenKeys.indexOf('m2') < 0, `and so did the write mark (${JSON.stringify(m.writtenKeys)})`);
        ok(w.get('d') === 'V-ARENA',
           `the key is readable again, honestly: the delete never left the process (${JSON.stringify(w.get('d'))})`);
        ok(w.has('d') === true, 'has() agrees');
        const before = f.calls.filter(c => c[0] === 'get').length;
        ok(await w.getAsync('d') === 'V-ARENA', 'and the async form is not cut off either');
        ok(w.get('m2') === 'V-ARENA-M2', `the minLevel-2 key reads L2 again (${JSON.stringify(w.get('m2'))})`);
        ok(f.calls.filter(c => c[0] === 'get').length === before,
           'no L3 round trip was needed for a key L2 can answer');
        done(w, 'send-threw');
    })();
    return;
}

// ------------------- F4: the primary's promotion vs a worker's submission
// The primary is the only writer of L3-derived data because its writes are
// ordered against its own. They were ordered against a WORKER's undrained
// submission by nothing: the promotion guard walks the invalidation ring, and
// a submission the primary has not drained has no record there. So the primary
// promoted the pre-write value over a write the worker had already been told
// succeeded -- and the promotion's own record then dropped the worker's
// self-marked L1 entry, so the worker read the old value back.
if (process.env.TCR4_ROLE === 'promo' || process.env.TCR4_ROLE === 'promodel') {
    (async () => {
        const del = process.env.TCR4_ROLE === 'promodel';
        const f = makeFake();
        const w = TurboKV.attachWorker(process.env.TCR4_ARENA, 1, { storage: 'bytes', l3: f.adapter });
        w.get('poke');
        process.send({ step: 'ready' }); await step('go');      // the primary's L3 get is in flight
        const acked = del ? await w.deleteAsync('k') : await w.setAsync('k', 'NEW');
        ok(acked === true, `the worker's ${del ? 'delete' : 'write'} was accepted (${acked})`);
        ok(w.get('k') === (del ? undefined : 'NEW'), 'and it reads its own operation back');
        process.send({ step: 'written' }); await step('promoted');
        const after = w.get('k');                                // drains the promotion's record
        ok(after === (del ? undefined : 'NEW'),
           `it STILL reads it back after the primary's promotion (${JSON.stringify(after)})`);
        process.send({ step: 'checked', arena: native.get('k') }); await step('bye');
        done(w, del ? 'promotion-vs-delete' : 'promotion-vs-write');
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
            env: { ...process.env, TCR4_ROLE: role, TCR4_ARENA: arena }, stdio: 'inherit',
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

(async () => {
    // --- F6: the cap's value compare -------------------------------------
    //
    // `sameStored` was `===`, so a value whose stored form is not identical to
    // its submitted form answered "the arena no longer holds this" and the cap
    // was skipped as 'moot' -- silently, with neither failure bucket moving.
    // Two values do that, and both round-trip through the arena perfectly:
    // NaN (which is not === itself) and a string with a lone surrogate (which
    // UTF-8 cannot carry, so it comes back as U+FFFD).
    {
        const f = makeFake();
        f.fail.set('set', new Error('l3 down'));
        const c = TurboKV.createPrimary(ARENA + 'a', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false, l3: f.adapter, l3RetryMs: 20, l3FailTtlMs: 300 });
        const cases = [['ctl', 'plain'], ['nan', NaN], ['neg0', -0], ['sur', 'ab\uD800cd'],
                       ['big', 5n], ['num', 1.5], ['nul', null], ['buf', Buffer.from('xyz')]];
        for (const [k, v] of cases) {
            ok(await c.setAsync(k, v) === false, `the L3 write for ${k} failed`);
            native.get(k);
            const rem = native.lastTtlRemainingMs();
            ok(rem > 0 && rem <= 300, `and its L2 copy is capped (${k}: ${rem})`);
        }
        ok(c.stats.l3FailTtlApplied === cases.length,
           `every one of them is counted (${c.stats.l3FailTtlApplied} of ${cases.length})`);
        ok((c.stats.l3FailTtlUnapplied || 0) === 0, 'and none as lost');
        await sleep(400);
        for (const [k] of cases)
            ok(native.get(k) === undefined, `${k} converges away at the cap`);
        await c.close();
    }

    // The same compare guards the WORKER's read guard (#capExpired), which
    // asks "is the arena still holding the value L3 refused, past its
    // deadline". A primary-side handle cannot reach it, but the fold itself
    // is a pure function of the two values and is exercised above in both
    // directions: `neg0` still caps (so the fix did not switch to Object.is,
    // which would have split -0 from 0 and stopped capping it) and `ctl`
    // still caps (so it is not now capping everything regardless).

    // A cache with NO ADAPTER takes none of these paths.
    {
        const c = TurboKV.createPrimary(ARENA + 'b', 4 << 20, 1 << 12,
            { storage: 'bytes', maintenance: false });
        c.set('k', NaN); c.set('s', 'ab\uD800cd');
        ok(Number.isNaN(c.get('k')), 'a cache with no adapter still stores NaN');
        // L1 hands back what was submitted; L2 can only hold what UTF-8 can
        // carry, which is what sameStored now compares against. (The two
        // differ for this one value shape, in this one direction, and always
        // did: it is the folding, not the compare.)
        ok(c.get('s') === 'ab\uD800cd', `L1 returns the value as submitted (${JSON.stringify(c.get('s'))})`);
        ok(native.get('s') === 'ab\uFFFDcd', `and L2 folds the lone surrogate (${JSON.stringify(native.get('s'))})`);
        ok(c.stats.l3FailTtlApplied === undefined, 'and invents no cap counter');
        c.close();
    }

    // --- forked children -------------------------------------------------
    {
        const code = await runChild('cap2', ARENA + '1', {}, (s, m, p, state, kid) => {
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
        });
        ok(code === 0, `the minLevel-2 cap cases passed on shm (child exited ${code})`);
    }
    {
        const code = await runChild('cap2ipc', ARENA + '2', {}, (s, m, p, state, kid) => {
            if (s === 'drain') { drainAll(); kid.send({ step: 'drained' }); }
        });
        ok(code === 0, `the minLevel-2 cap cases passed on ipc (child exited ${code})`);
    }
    {
        const code = await runChild('slowcap', ARENA + '3', {}, (s, m, p, state, kid) => {
            if (s === 'hold') { state.drain = false; kid.send({ step: 'held' }); }
            if (s === 'release') { state.drain = true; state.flushHeld(); drainAll(); kid.send({ step: 'released' }); }
        });
        ok(code === 0, `the slow-primary cap cases passed (child exited ${code})`);
    }
    {
        // The primary applies the worker's write, then laps the invalidation
        // ring so the record that would have cleared the mark is gone.
        const code = await runChild('wrap', ARENA + '4', {}, (s, m, p, state, kid) => {
            if (s === 'drain') { drainAll(); state.drain = false; kid.send({ step: 'drained' }); }
            if (s !== 'wrap') return;
            // NOT drained: the delete is still in the worker's submission ring,
            // and the record that would have cleared its mark is inside the
            // region the wrap discards.
            for (let i = 0; i < 70000; i++) p.set('filler' + (i % 50), 'x');
            kid.send({ step: 'wrapped' });
        }, (p) => { p.set('d2', 'V-DEL'); });
        ok(code === 0, `the wrapped-ring cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('ownrec', ARENA + '5', {}, (s, m, p, state, kid) => {
            // NOT drained: the primary's record sits on the ring, unseen by
            // the worker, which is the whole condition.
            if (s === 'primary-set') { state.drain = false; p.set('k', 'OLD'); kid.send({ step: 'primary-set-done' }); }
            if (s === 'primary-overwrite') {
                state.drain = true; drainAll();            // the worker's NEW is applied...
                p.set('k', 'NEWER');                       // ...and then overwritten by the primary
                kid.send({ step: 'primary-overwrite-done' });
            }
        });
        ok(code === 0, `the own-record cases passed (child exited ${code})`);
    }
    {
        const code = await runChild('sendthrow', ARENA + '6', { transport: 'ipc' }, () => {},
            (p) => { p.set('d', 'V-ARENA'); p.set('m2', 'V-ARENA-M2'); });
        ok(code === 0, `the send-threw cases passed (child exited ${code})`);
    }
    for (const role of ['promo', 'promodel']) {
        // A slow, SNAPSHOT-FIRST adapter: the value is read when the request
        // is issued, and the reply is released by the test -- so the primary's
        // L3 reply predates the worker's operation by construction.
        const f = makeFake();
        f.store.set('k', { value: 'OLD', expiresAt: 0 });
        let release = null;
        f.adapter.get = async (key) => {
            const rec = f.store.get(key);
            await new Promise(r => { release = r; });
            return rec ? { value: rec.value } : undefined;
        };
        let readP = null, primary = null;
        const del = role === 'promodel';
        const code = await runChild(role, ARENA + (del ? '8' : '7'), { l3: f.adapter },
            async (s, m, p, state, kid) => {
                primary = p;
                if (s === 'ready') {
                    readP = p.getAsync('k');
                    await sleep(5);
                    // The primary stops servicing the doorbell: the worker's
                    // operation is in its submission ring and nowhere else.
                    state.drain = false;
                    kid.send({ step: 'go' });
                }
                if (s === 'written') {
                    release();
                    const v = await readP;
                    ok(v === 'OLD', `the primary's own read still returns what L3 gave it (${JSON.stringify(v)})`);
                    ok(native.get('k') === (del ? undefined : 'NEW'),
                       `but it did not promote that over the worker's operation (${JSON.stringify(native.get('k'))})`);
                    ok((p.stats.l3PromotionsBlocked || 0) >= 1,
                       `the guard blocked it, having drained the ring first (${p.stats.l3PromotionsBlocked})`);
                    kid.send({ step: 'promoted' });
                }
                if (s === 'checked') {
                    ok(m.arena === (del ? undefined : 'NEW'),
                       `and L2 still holds the worker's own result (${JSON.stringify(m.arena)})`);
                    kid.send({ step: 'bye' });
                }
            });
        ok(code === 0, `the ${role} cases passed (child exited ${code})`);
        if (primary) { /* closed by runChild */ }
    }

    console.log(fail ? `\n  ${fail} FAILED` : '\n  [review4] all passed');
    process.exit(fail ? 1 : 0);
})();
