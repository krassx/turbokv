'use strict';
// The async API against a fake L3. The rule under test throughout: a sync
// method and its async twin have IDENTICAL effects; the async one only lets
// the caller wait for the L3 half.
const { TurboKV } = require('../src/turbokv');
const { makeFake, delay } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

(async () => {
    // 1. setAsync writes locally AND to L3, and resolves on the L3 outcome
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.setAsync('k', 'v') === true, 'setAsync resolves true');
        ok(c.get('k') === 'v', 'the value is in the local tiers immediately');
        ok(f.store.get('k').value === 'v', 'the value reached L3');
        c.close();
    }

    // 2. sync set reaches L3 too, just without waiting
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(c.set('k2', 'v2') === true, 'sync set still returns a boolean');
        ok(f.store.get('k2') === undefined, 'the L3 write has not happened yet');
        await c.drainL3();
        ok(f.store.get('k2').value === 'v2', 'the L3 write happens in the background');
        c.close();
    }

    // 3. a failed L3 write keeps the local value, resolves false, and is counted
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30, l3FailTtlMs: 5000 });
        ok(await c.setAsync('k3', 'v3') === false, 'a failed L3 write resolves false');
        ok(c.get('k3') === 'v3', 'the local cache keeps working during an L3 outage');
        ok(c.stats.l3SetFailed === 1, `the failure is counted (${c.stats.l3SetFailed})`);
        c.close();
    }

    // 4. background failures do NOT write lastError
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const errs = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 20,
                                onL3Error: (e) => errs.push(e.message) });
        c.set('k4', 'v4');                       // sync: returns before the failure
        c.set('this key is way too long'.repeat(200), 'x');   // a real, synchronous rejection
        const afterSync = c.lastError;
        await delay(120);
        ok(c.lastError === afterSync,
           `a background L3 failure does not overwrite lastError (${c.lastError})`);
        ok(errs.length > 0, 'the error listener hears about it instead');
        c.close();
    }

    // 5. minLevel L3 does not store locally, and a failed L3 write stores nothing
    //
    // "Does not store locally" means BOTH local tiers, not just L1. Asserting
    // l1Size alone let a real defect through: the write path split
    // `minLevel === 1` from everything else and then ran the L2 write
    // unguarded, so the value landed in the shared arena while the adapter was
    // told `willCache: false` -- a remote store registering no interest for a
    // key this box is in fact holding, which is a permanent cross-box stale
    // read. get() consults L1 and then L2, so with L1 empty it is exactly the
    // L2 probe this was missing.
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.setAsync('k5', 'v5', { minLevel: TurboKV.L3 }) === true, 'minLevel L3 write succeeds');
        ok(c.l1Size === 0, 'nothing was put in L1');
        ok(c.get('k5') === undefined, `and nothing in L2 either (${c.get('k5')})`);
        ok(f.store.get('k5').value === 'v5', 'the value is in L3');
        ok(f.calls.some(x => x[0] === 'set' && x[1] === 'k5' && x[2] === false),
           'the adapter is told willCache:false');
        // A copy ALREADY resident has to go, for the same reason the L1 copy
        // does: the caller asked for this value not to live here, and leaving
        // the previous one behind would serve it as though it were current --
        // this option would produce stale reads instead of misses.
        await c.setAsync('k5b', 'old');
        ok(c.get('k5b') === 'old', 'sanity: the ordinary write is resident locally');
        ok(await c.setAsync('k5b', 'new', { minLevel: TurboKV.L3 }) === true, 'the L3-only overwrite succeeds');
        ok(c.get('k5b') === undefined, `the resident copy is evicted, not left stale (${c.get('k5b')})`);
        ok(f.store.get('k5b').value === 'new', 'and L3 holds the new value');
        c.close();
    }

    // 6. without an adapter the async API still works, stopping at L2
    {
        const c = TurboKV.open({ storage: 'bytes' });
        ok(await c.setAsync('k6', 'v6') === true, 'setAsync works with no adapter');
        ok(c.get('k6') === 'v6', 'and stores locally');
        c.close();
    }

    // 7. getAsync reads through to L3 and fills the local tiers
    {
        const f = makeFake();
        f.store.set('r1', { value: 'from-l3', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('r1') === 'from-l3', 'getAsync reads through to L3');
        ok(c.get('r1') === 'from-l3', 'and the value is now local');
        ok(c.stats.l3Hits === 1, `L3 hits are counted (${c.stats.l3Hits})`);
        c.close();
    }

    // 8. concurrent misses on one key share a single L3 request
    {
        const f = makeFake();
        f.store.set('r2', { value: 'v', expiresAt: 0 });
        f.latency.set('get', 30);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const all = await Promise.all([c.getAsync('r2'), c.getAsync('r2'), c.getAsync('r2')]);
        ok(all.every(v => v === 'v'), 'every caller gets the value');
        const gets = f.calls.filter(x => x[0] === 'get').length;
        ok(gets === 1, `concurrent misses share one request (made ${gets})`);
        c.close();
    }

    // 9. minLevel L3 returns the value without storing it anywhere local
    {
        const f = makeFake();
        f.store.set('r3', { value: 'v', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('r3', { minLevel: TurboKV.L3 }) === 'v', 'the value is returned');
        ok(c.l1Size === 0 && c.get('r3') === undefined, 'nothing was stored locally');
        c.close();
    }

    // 10. a miss and a failed read are both misses, and the read failure is
    //     reported through the ONE listener path -- exactly once
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.getAsync('absent') === undefined, 'a key L3 does not have is undefined');
        ok(c.stats.l3Misses === 1, `the miss is counted (${c.stats.l3Misses})`);
        c.close();
    }
    {
        const f = makeFake();
        f.fail.set('get', new Error('L3 read down'));
        const errs = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter,
                                onL3Error: (e, op) => errs.push(op.kind) });
        ok(await c.getAsync('r4') === undefined, 'a failed L3 read is a miss, not a throw');
        ok(c.stats.l3Misses === 1, `the failed read is counted as a miss (${c.stats.l3Misses})`);
        ok(errs.length === 1 && errs[0] === 'get',
           `the read failure is reported once, as a get (${errs.join(',')})`);
        ok(c.lastError === null, `a background L3 failure still does not touch lastError (${c.lastError})`);
        c.close();
    }

    // 10. delete removes from local tiers and from L3
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        await c.setAsync('d1', 'v');
        ok(await c.deleteAsync('d1') === true, 'deleteAsync reports the key was present');
        ok(c.get('d1') === undefined && f.store.get('d1') === undefined, 'gone from both');
        c.close();
    }

    // 11. a failed L3 delete leaves the local delete standing
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 20 });
        await c.setAsync('d2', 'v');
        f.fail.set('delete', new Error('L3 down'));
        await c.deleteAsync('d2');
        ok(c.get('d2') === undefined, 'the local delete stands');
        ok(f.store.get('d2') !== undefined, 'L3 still has it, which the caller was told');
        c.close();
    }

    // 12. hasAsync consults L3, and uses the adapter's has when it exists
    {
        const f = makeFake();
        f.store.set('h1', { value: 'v', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.hasAsync('h1') === true, 'hasAsync sees a key that is only in L3');
        ok(f.calls.some(x => x[0] === 'has'), 'the adapter has() is used when present');
        ok(await c.hasAsync('nope') === false, 'and reports absence');
        c.close();
    }

    // 13. without adapter.has, hasAsync falls back to get
    {
        const f = makeFake({ noOptional: true });
        f.store.set('h2', { value: 'v', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        ok(await c.hasAsync('h2') === true, 'the get fallback answers correctly');
        // The fallback calls the adapter directly and never goes through the
        // fill path -- so it must not leave the value resident anywhere
        // local. "Correct by inspection" is how an untested property quietly
        // stops being true.
        ok(c.l1Size === 0 && c.get('h2') === undefined,
           'the get-fallback probe does not cache the value it discarded');
        c.close();
    }

    // 14. clear empties every tier
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        await c.setAsync('c1', 'v');
        ok(await c.clearAsync() === true, 'clearAsync resolves true');
        ok(c.get('c1') === undefined, 'local tiers are empty');
        ok(f.store.size === 0, 'L3 is empty');
        c.close();
    }

    // 15. while a clear is pending, L3 reads serve misses rather than values
    //     the clear was meant to remove
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30 });
        await c.setAsync('c2', 'v');
        f.fail.set('clear', new Error('L3 down'));
        c.clearAll();
        ok(await c.getAsync('c2') === undefined,
           'a pending clear makes L3 reads miss rather than resurrect the value');
        f.fail.delete('clear');
        await c.drainL3();
        c.close();
    }

    // 16. the pending-clear guard is PROCESS-WIDE, not per-instance: decision
    //     64 established that several TurboKV instances in one process share
    //     one arena, and clearAll() already reaches siblings (#clearOthers)
    //     for exactly that reason. A clear issued by ONE instance must block
    //     an L3 read on ANY instance sharing that arena -- otherwise a
    //     sibling's own (unset) flag lets it read L3's not-yet-cleared value
    //     straight back into the SHARED arena every instance then sees.
    {
        const f = makeFake();
        const A = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30 });
        const B = new TurboKV({ storage: 'bytes', l3: f.adapter, l3RetryMs: 30 });
        await A.setAsync('shared', 'v');
        ok(B.get('shared') === 'v', "sanity: B reads A's write through the shared arena");
        f.fail.set('clear', new Error('L3 down'));   // keep A's L3 clear outstanding
        A.clearAll();                                 // wipes the SHARED arena; queues A's L3 clear
        const got = await B.getAsync('shared');
        ok(got === undefined,
           `a sibling's clear blocks THIS instance's L3 read too (${got})`);
        ok(f.calls.filter(x => x[0] === 'get' && x[1] === 'shared').length === 0,
           "B's read never even asked L3 -- the guard short-circuited before the call");
        // Checked from both sides, since the shared arena is what is being
        // protected, not either instance individually.
        ok(A.get('shared') === undefined, 'and the clearing instance still sees it gone');
        ok(B.get('shared') === undefined, 'nothing was resurrected into the shared arena');
        f.fail.delete('clear');
        await A.drainL3();
        B.close(); A.close();
    }

    // 17. close() drains the queue and closes the adapter, and the process
    //     can exit
    {
        const f = makeFake(); f.latency.set('set', 20);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.set('s1', 'v');                      // queued, not yet sent
        await c.close();
        ok(f.store.get('s1').value === 'v', 'close() waits for queued L3 work');
        ok(f.calls.some(x => x[0] === 'close'), 'close() closes the adapter');
    }

    // 18. close() is safe without an adapter and still returns a promise
    {
        const c = TurboKV.open({ storage: 'bytes' });
        const r = c.close();
        ok(typeof r.then === 'function', 'close() returns a promise with no adapter too');
        await r;
    }

    // 19. close() cannot hang forever waiting out an L3 outage: a `clear` is
    //     never shed and retries without limit by design (see L3Queue), so
    //     if L3 stays unreachable the queue's pending count never returns to
    //     zero and drain() never resolves. close() bounds the wait
    //     (l3CloseTimeoutMs) and proceeds anyway on expiry. Without that
    //     bound this test would hang instead of failing -- the race against
    //     `guard` below turns that hang into a failed assertion, so a
    //     regression fails the suite rather than wedging CI.
    {
        const f = makeFake();
        f.fail.set('clear', new Error('L3 permanently down'));   // never removed: never succeeds
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3CloseTimeoutMs: 20 });
        c.clearAll();                                             // queues a clear that never lands
        const guard = delay(2000).then(() => 'TIMED_OUT');
        const winner = await Promise.race([c.close().then(() => 'CLOSED'), guard]);
        ok(winner === 'CLOSED', `close() honours its bound instead of hanging forever (${winner})`);
    }

    // 20. a failing adapter.close() is reported through onL3Error, not
    //     thrown, and close() still resolves -- shutdown must not get stuck
    //     on the one call it cannot retry.
    {
        const f = makeFake();
        f.adapter.close = async () => { throw new Error('close failed'); };
        const reports = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, onL3Error: (e, op) => reports.push(op.kind) });
        await c.close();
        ok(reports.includes('close'), `a failing adapter.close() is reported, not thrown (${reports})`);
    }

    // 21. an adapter.close() that never settles cannot hang close() either --
    //     it is bounded by l3CloseTimeoutMs same as the queue drain, just
    //     separately, since nothing about draining the queue first
    //     guarantees the adapter's own close() returns promptly. Same guard
    //     technique as test 19: a hang becomes a failed assertion instead
    //     of wedging CI.
    {
        const f = makeFake();
        f.adapter.close = () => new Promise(() => {});   // never resolves
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3CloseTimeoutMs: 20 });
        const guard = delay(2000).then(() => 'TIMED_OUT');
        const winner = await Promise.race([c.close().then(() => 'CLOSED'), guard]);
        ok(winner === 'CLOSED', `a hung adapter.close() does not hang close() (${winner})`);
    }

    // 22. close() is idempotent: a second call must not re-invoke
    //     adapter.close() or re-run the drain, and it returns the SAME
    //     promise every time so every caller observes the real outcome
    //     together. close() is exactly the method a shutdown hook, a
    //     signal handler and a test's own teardown all reach for -- two of
    //     those firing is ordinary, not exotic.
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const p1 = c.close();
        const p2 = c.close();
        ok(p1 === p2, 'both calls return the same promise');
        await Promise.all([p1, p2]);
        const closes = f.calls.filter(x => x[0] === 'close').length;
        ok(closes === 1, `adapter.close() is invoked exactly once across two close() calls (${closes})`);
        const p3 = c.close();
        ok(p3 === p1, 'a close() after the first has already resolved still returns the same promise');
        await p3;
        ok(f.calls.filter(x => x[0] === 'close').length === 1,
           'a third call, after resolution, still does not re-invoke the adapter');
    }

    // 23. l3FailTtlMs, the ABANDONED route. Decision 68: "on failure the value
    //     is KEPT locally under a short TTL: serve through the outage,
    //     converge afterwards". Without the cap a write L3 never accepted
    //     disagrees with L3 for as long as its own TTL says -- forever for the
    //     common ttlMs:0 -- which is the split brain the option exists to bound.
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 10, l3FailTtlMs: 60 });
        ok(await c.setAsync('ft1', 'v') === false, 'the write is abandoned and resolves false');
        await delay(5);
        ok(c.get('ft1') === 'v', 'and is still served locally: through the outage');
        ok(c.stats.l3FailTtlApplied === 1, `the cap is applied and counted (${c.stats.l3FailTtlApplied})`);
        await delay(140);
        ok(c.get('ft1') === undefined,
           `past l3FailTtlMs the local copy is gone, so the box converges on L3 (${c.get('ft1')})`);
        c.close();
    }

    // 24. l3FailTtlMs, the SHED route. The spec's failure matrix says the same
    //     thing for a write shed past l3QueueMaxBytes as for one abandoned past
    //     l3RetryMs: "keep, capped at l3FailTtlMs". A shed write never reaches
    //     the adapter at all, so it is the route most likely to be forgotten.
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3QueueMaxBytes: 1, l3FailTtlMs: 60 });
        ok(await c.setAsync('ft2', 'v') === false, 'a write shed past l3QueueMaxBytes resolves false');
        await delay(5);
        ok(c.get('ft2') === 'v', 'and is still served locally');
        ok(c.stats.l3FailTtlApplied === 1, `the cap is applied to a shed write too (${c.stats.l3FailTtlApplied})`);
        ok(f.calls.filter(x => x[0] === 'set').length === 0, 'the shed write never reached the adapter');
        await delay(140);
        ok(c.get('ft2') === undefined, `a shed write reverts on the same schedule (${c.get('ft2')})`);
        c.close();
    }

    // 24b. the cap SHORTENS and never lengthens, and it recognises a BINARY
    //      value in L2 -- which comes back out of the arena as a different
    //      Buffer object than the one that went in, so an identity comparison
    //      would decide the arena no longer held our value and skip the cap for
    //      every binary write there is.
    {
        const f = makeFake();
        f.fail.set('set', new Error('L3 down'));
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 10, l3FailTtlMs: 60 });
        ok(await c.setAsync('ftb', Buffer.from('binary')) === false, 'the binary write is abandoned');
        await delay(5);
        ok(c.stats.l3FailTtlApplied === 1, `a binary value is recognised and capped (${c.stats.l3FailTtlApplied})`);
        // A write whose own TTL is already shorter keeps it: the cap is a
        // ceiling on the disagreement, not a floor on the lifetime.
        ok(await c.setAsync('fts', 'v', { ttlMs: 20 }) === false, 'the short-TTL write is abandoned too');
        await delay(60);
        ok(c.get('fts') === undefined, `the shorter of the two deadlines wins (${c.get('fts')})`);
        await delay(80);
        ok(c.get('ftb') === undefined, `and the binary value reverts on schedule (${c.get('ftb')})`);
        c.close();
    }

    // 25. a write whose L3 half SUCCEEDS is not capped: the cap is a failure
    //     policy, not a ceiling on every write's lifetime.
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3FailTtlMs: 30 });
        ok(await c.setAsync('ft3', 'v') === true, 'the write lands in L3');
        await delay(90);
        ok(c.get('ft3') === 'v', `a successful write keeps its own lifetime (${c.get('ft3')})`);
        ok(c.stats.l3FailTtlApplied === undefined, 'and no cap was applied');
        c.close();
    }

    // 26. originId reaches the adapter. Spec section 4: it is native.arenaId(),
    //     stable per arena and identical in the primary and every worker
    //     mapping it. It is the loop-suppression field -- an adapter author
    //     writes `if (originId === mine) skip`, and handing them `undefined`
    //     means that test silently never fires.
    {
        const f = makeFake();
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        await c.setAsync('o1', 'v');
        await c.deleteAsync('o1');
        const s = f.calls.find(x => x[0] === 'set' && x[1] === 'o1');
        const d = f.calls.find(x => x[0] === 'delete' && x[1] === 'o1');
        ok(s && s[3] && typeof s[3].originId === 'string' && s[3].originId.length > 0,
           `set receives a real originId (${s && s[3] && JSON.stringify(s[3].originId)})`);
        ok(d && d[3] && d[3].originId === s[3].originId,
           `delete reports the same arena identity (${d && d[3] && JSON.stringify(d[3].originId)})`);
        c.close();
    }

    // 27. a HUNG adapter read -- one that neither resolves nor rejects -- is a
    //     miss within the operation budget, and DOES NOT POISON THE KEY.
    //     retryMs is consulted only in a catch, so a hang never reached it:
    //     getAsync's .finally never ran, the #inflight entry became permanent,
    //     and every later read of that key in this process was handed the same
    //     dead promise even after L3 recovered. Raced against a guard so a
    //     regression fails an assertion rather than wedging CI.
    {
        const f = makeFake();
        f.store.set('hung', { value: 'v', expiresAt: 0 });
        f.hang.add('get');
        const errs = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 40,
                                onL3Error: (e, op) => errs.push(op.kind) });
        const first = await Promise.race([c.getAsync('hung'), delay(3000).then(() => 'TIMED_OUT')]);
        ok(first === undefined, `a hung adapter read settles as a miss (${first})`);
        ok(errs.includes('get'), `and is reported like any other failed read (${errs.join(',')})`);
        f.hang.delete('get');
        const second = await Promise.race([c.getAsync('hung'), delay(3000).then(() => 'TIMED_OUT')]);
        ok(second === 'v', `the key recovers once L3 does rather than staying poisoned (${second})`);
        c.close();
    }

    // 28. a hung adapter WRITE follows the same retry-and-abandon policy, so
    //     setAsync settles instead of hanging its caller forever.
    {
        const f = makeFake();
        f.hang.add('set');
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter, l3RetryMs: 40 });
        const r = await Promise.race([c.setAsync('hw', 'v'), delay(3000).then(() => 'TIMED_OUT')]);
        ok(r === false, `a hung adapter write is abandoned rather than never settling (${r})`);
        c.close();
    }

    // 29. #inflight is keyed by LEVEL AND KEY. Keyed by key alone, a joiner's
    //     minLevel was silently replaced by whichever caller got there first --
    //     so a minLevel:L3 read could promote into L1 because someone else
    //     asked for the default, and the adapter heard one willCache for two
    //     different requests.
    {
        const f = makeFake();
        f.store.set('lv', { value: 'v', expiresAt: 0 });
        f.latency.set('get', 25);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const [a, b] = await Promise.all([
            c.getAsync('lv', { minLevel: TurboKV.L3 }),
            c.getAsync('lv'),
        ]);
        ok(a === 'v' && b === 'v', 'both callers get the value');
        const gets = f.calls.filter(x => x[0] === 'get' && x[1] === 'lv');
        ok(gets.length === 2, `reads at different levels are not joined into one request (${gets.length})`);
        ok(gets.some(g => g[2] === false) && gets.some(g => g[2] === true),
           `each level announced its own willCache (${gets.map(g => g[2]).join(',')})`);
        c.close();
    }

    // 30. close()'s L3 half cannot reject either, and it is caught separately
    //     from the local teardown so one failing does not swallow the other.
    //     The adapter is the caller's object, so merely LOOKING at it can
    //     throw -- `typeof adapter.close === 'function'` runs a getter.
    {
        const f = makeFake();
        let reads = 0;
        Object.defineProperty(f.adapter, 'close', {
            configurable: true,
            // assertAdapter reads it twice at construction (presence, then
            // type); close() is the read after that.
            get() { if (++reads > 2) throw new Error('adapter getter exploded'); return async () => {}; },
        });
        const reports = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter,
                                onL3Error: (e, op) => reports.push(op.kind) });
        let rejected = null;
        await c.close().then(() => {}, (e) => { rejected = e; });
        ok(rejected === null, `close() does not reject when the adapter throws on access (${rejected && rejected.message})`);
        ok(String(c.lastError).includes('adapter getter exploded'),
           `the failure is reported through lastError (${c.lastError})`);
        ok(reports.includes('close'), `and through the listener (${reports.join(',')})`);
    }

    // 31. close() NEVER REJECTS. Its synchronous teardown -- stopGuard(), the
    //     ring-release loop, the unwrapped native.destroy() -- used to throw to
    //     the caller. Once close() returned a promise, the same throw became a
    //     rejection of a promise every caller in this codebase deliberately
    //     ignores, which under Node 18's default terminates the process: a
    //     failed ring release would kill the process it was cleaning up.
    //
    //     LAST in this file on purpose: the injected failure aborts the rest of
    //     the teardown, leaving the arena undestroyed, and test/_cleanup.js
    //     releases it on exit.
    {
        const f = makeFake();
        const reports = [];
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter,
                                onL3Error: (e, op) => reports.push(op.kind) });
        c.stopGuard = () => { throw new Error('teardown exploded'); };
        let rejected = null;
        await c.close().then(() => {}, (e) => { rejected = e; });
        ok(rejected === null, `close() does not reject when its teardown throws (${rejected && rejected.message})`);
        ok(String(c.lastError).includes('teardown exploded'),
           `the failure is reported through lastError instead (${c.lastError})`);
        ok(reports.includes('close'), `and through the listener, as a close (${reports.join(',')})`);
    }

    // 22. CLEAR IS ORDERED AGAINST WRITES ON EVERY KEY, not just its own.
    //
    // `clear` has no key, so the per-key chains that keep `set(k,A); set(k,B)`
    // in order say nothing about a set on some other key. A set still in flight
    // when the clear was issued therefore landed in L3 AFTER it, leaving L3
    // holding exactly what the clear was meant to remove -- and the next
    // getAsync pulled it back into the local tiers, so a clear that reported
    // success had undone itself. Decision 66 leaves cross-key order alone so a
    // slow key cannot throttle the process; `clear` is its exception, because
    // "every key" is what the operation means.
    {
        const f = makeFake();
        f.latency.set('set', 60);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.set('k', 'v');                           // 60ms on the wire to L3
        ok(await c.clearAsync() === true, 'clearAsync still resolves true');
        // Drained before the assertion, because the resurrection is the set
        // landing AFTER the clear: checking the instant the clear resolves
        // would find L3 empty and the damage still on the wire.
        await c.drainL3();
        ok(f.store.get('k') === undefined,
           `a set in flight when the clear was issued does not survive it (${f.store.get('k') && f.store.get('k').value})`);
        ok(c.get('k') === undefined, 'and the key is gone locally');
        const got = await c.getAsync('k');
        ok(got === undefined, `so a later read cannot resurrect it out of L3 (${got})`);
        await c.close();
    }

    // 23. ...including a write that was still queued behind a slower one, and
    //     NOT including a write issued after the clear, which must survive it.
    {
        const f = makeFake();
        f.latency.set('set', 40);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.set('h', '1');                           // on the wire
        c.set('h', '2');                           // queued behind it
        const cleared = c.clearAsync();
        const later = c.setAsync('after', 'kept'); // pushed after the clear
        ok(await cleared === true, 'the clear settles');
        ok(await later === true, 'and so does the write issued after it');
        await c.drainL3();
        ok(f.store.get('h') === undefined, `the queued write did not outlive the clear (${f.store.get('h') && f.store.get('h').value})`);
        ok(f.store.get('after') && f.store.get('after').value === 'kept',
           'a write issued AFTER the clear survives it, which is what "later writes win" means');
        ok(await c.getAsync('after') === 'kept', 'and reads back');
        await c.close();
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-api] all passed');
    process.exit(fail ? 1 : 0);
})();
