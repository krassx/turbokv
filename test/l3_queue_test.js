'use strict';
// The queue is where "local first, then L3" becomes safe. Order per key is a
// correctness requirement: two writes to one key sent over a pool can arrive
// in either order, leaving L3 with the older value while this box holds the
// newer one.
const { L3Queue } = require('../src/l3/queue');
const { makeFake, delay } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
// close() UNREFS the backoff a retrying operation is sleeping in, on purpose:
// a closing queue must not detain a process that is trying to exit. That is
// right for production and awkward for a test, which wants to see the
// abandoned operation settle -- with nothing else ref'd, node exits mid-
// assertion. `hold` keeps the loop alive across those few blocks; a genuine
// hang still fails, via the per-file timeout in test/run.js.
const hold = () => { const t = setInterval(() => {}, 1000); return () => clearInterval(t); };

(async () => {
    // 1. order per key is preserved even when the FIRST write is much slower
    {
        const f = makeFake(); f.latency.set('set', 40);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'k', value: 'A', bytes: 10 });   // captures 40ms
        f.latency.set('set', 5);                                     // B is fast
        const last = q.push({ kind: 'set', key: 'k', value: 'B', bytes: 10 });
        await last; await q.drain();
        ok(f.store.get('k').value === 'B',
           `the last write to a key wins even when the first is slower (${f.store.get('k').value})`);
    }

    // 2. a superseded write is merged rather than sent, and still settles
    {
        const f = makeFake(); f.latency.set('set', 30);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'x', value: '1', bytes: 10 });   // starts immediately
        const a = q.push({ kind: 'set', key: 'x', value: '2', bytes: 10 });
        const b = q.push({ kind: 'set', key: 'x', value: '3', bytes: 10 });
        ok(await a === true && await b === true, 'a merged write still settles true');
        await q.drain();
        const sets = f.calls.filter(c => c[0] === 'set').length;
        ok(sets === 2, `a superseded write is not sent (sent ${sets}, expected 2)`);
        ok(q.stats.coalesced === 1, `coalesced counted (${q.stats.coalesced})`);
        ok(f.store.get('x').value === '3', 'L3 holds the newest value');
    }

    // 3. operations on DIFFERENT keys do not serialise behind each other
    {
        const f = makeFake(); f.latency.set('set', 25);
        const q = new L3Queue(f.adapter, {});
        const t0 = Date.now();
        await Promise.all([
            q.push({ kind: 'set', key: 'a', value: '1', bytes: 10 }),
            q.push({ kind: 'set', key: 'b', value: '1', bytes: 10 }),
            q.push({ kind: 'set', key: 'c', value: '1', bytes: 10 }),
        ]);
        ok(Date.now() - t0 < 60, `different keys run concurrently (${Date.now() - t0}ms for 3x25ms)`);
    }

    // 4. a transient failure is retried inside the budget and settles true --
    // and reports NOTHING through onError, because nothing was lost. onError
    // used to fire once here too (on the first failed attempt), which made a
    // "failed" listener hear about writes that went on to succeed; stats.retried
    // is what makes a mid-flight retry visible, onError is for abandonment only.
    {
        const f = makeFake(); const seen = [];
        f.fail.set('set', new Error('connection reset'));
        const q = new L3Queue(f.adapter, { retryMs: 500, onError: (e, op) => seen.push([e.message, op.kind]) });
        const p = q.push({ kind: 'set', key: 'r', value: 'v', bytes: 10 });
        setTimeout(() => f.fail.delete('set'), 40);
        ok(await p === true, 'a retry that succeeds inside the budget settles true');
        ok(q.stats.retried > 0, `retries counted (${q.stats.retried})`);
        ok(seen.length === 0, `a write that later succeeds reports nothing (${seen.length})`);
    }

    // 5. a failure that outlasts the budget settles false ONCE, and reports
    {
        const f = makeFake(); const seen = [];
        f.fail.set('set', new Error('still down'));
        const q = new L3Queue(f.adapter, { retryMs: 60, onError: (e, op) => seen.push([e.message, op.kind]) });
        let settled = 0;
        const p = q.push({ kind: 'set', key: 'd', value: 'v', bytes: 10 });
        p.then(() => settled++);
        ok(await p === false, 'an abandoned write settles false');
        await delay(120);
        ok(settled === 1, `the promise settles exactly once (${settled})`);
        ok(seen.length > 0 && seen[0][0] === 'still down', 'the error listener is told');
        ok(q.stats.failed === 1, `failures counted (${q.stats.failed})`);
    }

    // 6. over the byte bound, work is shed rather than queued without limit
    {
        const f = makeFake(); f.latency.set('set', 50);
        const q = new L3Queue(f.adapter, { maxBytes: 100 });
        const kept = [], shed = [];
        // maxBytes:100, bytes:30/op: i=0,1,2 are admitted (30,60,90 <= 100);
        // i=3 is the first to exceed it (90+30 > 100). Every push from i=3 on
        // must shed, so the assertion below checks ALL of them, not just one.
        for (let i = 0; i < 20; i++) {
            const p = q.push({ kind: 'set', key: 'k' + i, value: 'v', bytes: 30 });
            (i < 3 ? kept : shed).push(p);
        }
        const results = await Promise.all(shed);
        ok(results.every(r => r === false), `every push past the bound is shed (${results.filter(r => r !== false).length} were not)`);
        ok(q.stats.shed > 0, `sheds counted (${q.stats.shed})`);
        ok(q.pendingBytes <= 100 + 30, `pending bytes stay bounded (${q.pendingBytes})`);
    }

    // 7. clear retries past the budget, because until it lands we serve misses
    {
        const f = makeFake();
        f.fail.set('clear', new Error('NOPERM'));
        const q = new L3Queue(f.adapter, { retryMs: 30 });
        const p = q.push({ kind: 'clear', bytes: 0 });
        setTimeout(() => f.fail.delete('clear'), 150);   // well past retryMs
        ok(await p === true, 'clear keeps retrying past the retry budget and eventually settles true');
    }

    // 8. a drained key does not leak its chain
    {
        const f = makeFake();
        const q = new L3Queue(f.adapter, {});
        for (let i = 0; i < 500; i++) q.push({ kind: 'set', key: 'leak' + i, value: 'v', bytes: 10 });
        await q.drain();
        ok(q.chainCount === 0, `chains are released once drained (${q.chainCount} left)`);
    }

    // 9. delete reaches the adapter and removes the key
    {
        const f = makeFake();
        const q = new L3Queue(f.adapter, {});
        f.store.set('gone', { value: 'seed', expiresAt: 0 });   // present before the delete
        const p = q.push({ kind: 'delete', key: 'gone', bytes: 0 });
        ok(await p === true, 'a delete settles true');
        ok(f.store.has('gone') === false, 'delete removes the key from L3');
        ok(f.calls.some(c => c[0] === 'delete' && c[1] === 'gone'), 'the adapter delete method was actually called');
    }

    // 10. a delete is ordered against a set to the same key, not raced --
    // otherwise a slow set queued behind a fast delete could resurrect a key
    // this process just told L3 to remove.
    {
        const f = makeFake(); f.latency.set('set', 40); f.latency.set('delete', 5);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'resurrect', value: 'v', bytes: 10 });   // captures 40ms
        const p = q.push({ kind: 'delete', key: 'resurrect', bytes: 0 });   // queued; captures 5ms only once it starts
        await p; await q.drain();
        ok(f.store.has('resurrect') === false,
           `a delete queued behind a slower set still lands last and the key stays absent (has=${f.store.has('resurrect')})`);
        const order = f.calls.filter(c => c[1] === 'resurrect').map(c => c[0]).join(',');
        ok(order === 'set,delete', `set then delete were sent in that order (${order})`);
    }

    // 11. outstandingKind: the question the read path asks this queue.
    //
    // The invalidation ring can say whether someone ELSE changed a key. It
    // cannot say whether THIS process still owes L3 a change, because an
    // operation that has not reached L3 has produced no invalidation anywhere.
    // So the queue has to answer, and it has to answer for the operation on the
    // wire as well as the one merely queued -- the in-flight one is exactly the
    // case a concurrent read overlaps.
    {
        const f = makeFake(); f.latency.set('delete', 40); f.latency.set('set', 40);
        const q = new L3Queue(f.adapter, {});
        ok(q.outstandingKind('nothing') === undefined, 'no operation for an untouched key');
        const d = q.push({ kind: 'delete', key: 'k', bytes: 10 });
        ok(q.outstandingKind('k') === 'delete',
           `a delete ON THE WIRE is reported, not just a queued one (${q.outstandingKind('k')})`);
        q.push({ kind: 'set', key: 'k', value: 'v', bytes: 10 });          // queued behind it
        ok(q.outstandingKind('k') === 'set',
           `the NEWEST operation wins: the queued set supersedes the in-flight delete (${q.outstandingKind('k')})`);
        ok(q.outstandingKind('other') === undefined, 'and it is per key, not per queue');
        await d; await q.drain();
        ok(q.outstandingKind('k') === undefined, 'nothing outstanding once the chain has drained');
        ok(q.chainCount === 0, `and the chain entry is gone (${q.chainCount})`);
    }

    // 12. CLEAR IS A BARRIER: work already on the wire lands BEFORE it.
    //
    // A set that lands after a clear resurrects exactly what the clear removed,
    // and the clear has no key to be ordered against it by the per-key chain.
    {
        const f = makeFake(); f.latency.set('set', 60); f.latency.set('clear', 1);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'k', value: 'v', bytes: 10 });   // 60ms on the wire
        const c = q.push({ kind: 'clear', bytes: 0 });              // issued 0ms later
        ok(await c === true, 'the clear settles true');
        await q.drain();
        const order = f.calls.map(x => x[0]).join(',');
        ok(order === 'set,clear', `the in-flight set is awaited, then the clear runs (${order})`);
        ok(f.store.has('k') === false,
           `and L3 is actually empty afterwards, not holding the set that was in flight (has=${f.store.has('k')})`);
    }

    // 13. work PENDING BUT UNSENT when the clear arrives is dropped, and
    //     settles with the CLEAR's outcome -- the same rule coalescing already
    //     applies to a write a later write supersedes.
    {
        const f = makeFake(); f.latency.set('set', 40);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'h', value: '1', bytes: 10 });          // on the wire
        const queued = q.push({ kind: 'set', key: 'h', value: '2', bytes: 10 });   // pending
        const other = q.push({ kind: 'set', key: 'z', value: '9', bytes: 10 });    // on the wire
        const c = q.push({ kind: 'clear', bytes: 0 });
        ok(await queued === true, 'the dropped write settles with the clear\'s outcome rather than hanging');
        ok(await other === true, 'a write already on the wire still settles on its own result');
        await c; await q.drain();
        const sent = f.calls.filter(x => x[0] === 'set' && x[1] === 'h').length;
        ok(sent === 1, `the unsent write was never sent (${sent} set calls for h, expected 1)`);
        ok(f.store.size === 0, `L3 is empty after the clear (${f.store.size} keys left)`);
        ok(f.calls[f.calls.length - 1][0] === 'clear', 'and the clear was the last thing sent');
    }

    // 14. a dropped write settles FALSE when the clear itself fails: its
    //     promise means "L3 holds your value, or a later operation from this
    //     process" -- and if that later operation failed, it holds neither.
    {
        const f = makeFake(); f.latency.set('set', 40);
        f.fail.set('clear', new Error('down'));
        const q = new L3Queue(f.adapter, { retryMs: 5 });
        q.push({ kind: 'set', key: 'h', value: '1', bytes: 10 });
        const queued = q.push({ kind: 'set', key: 'h', value: '2', bytes: 10 });
        const c = q.push({ kind: 'clear', bytes: 0 });
        const release = hold();
        q.close();                       // a clear retries forever otherwise (decision 70)
        ok(await c === false, 'a clear against a dead L3 settles false once the queue is closing');
        ok(await queued === false, 'and the write it dropped settles false with it');
        release();
    }

    // 15. work pushed AFTER the clear queues behind it and survives it. This is
    //     what makes the spec's "later writes survive a clear" true; without
    //     the barrier the later write races the clear instead.
    {
        const f = makeFake(); f.latency.set('clear', 40);
        const q = new L3Queue(f.adapter, {});
        const c = q.push({ kind: 'clear', bytes: 0 });
        const later = q.push({ kind: 'set', key: 'after', value: 'kept', bytes: 10 });
        ok(q.outstandingKind('after') === 'set', 'the later write is outstanding while it waits');
        await c; await later; await q.drain();
        const order = f.calls.map(x => x[0]).join(',');
        ok(order === 'clear,set', `the clear goes first (${order})`);
        ok(f.store.get('after') && f.store.get('after').value === 'kept',
           'and the later write survives the clear');
    }

    // 16. two overlapping clears: the first one finishing must not let work
    //     through while the second is still on its way.
    {
        const f = makeFake(); f.latency.set('clear', 20);
        const q = new L3Queue(f.adapter, {});
        const c1 = q.push({ kind: 'clear', bytes: 0 });
        await delay(25 + 5);                      // c1 is now on the wire or just done
        const c2 = q.push({ kind: 'clear', bytes: 0 });
        const w = q.push({ kind: 'set', key: 'w', value: 'v', bytes: 10 });
        await Promise.all([c1, c2, w]); await q.drain();
        const order = f.calls.map(x => x[0]).join(',');
        ok(order === 'clear,clear,set', `both clears precede the later write (${order})`);
        ok(f.store.get('w').value === 'v', 'and the write survives them');
        ok(q.chainCount === 0, `no chain is left behind (${q.chainCount})`);
    }

    // 17. THE ESCAPE VALVE. A clear against an L3 that never answers retries
    //     indefinitely by design, so the work waiting behind it must not grow
    //     without bound: it keeps counting against maxBytes and later pushes
    //     are shed and settle false, exactly as a full submission ring sheds.
    //     Without this a stuck clear would be a memory leak instead.
    {
        const f = makeFake(); f.hang.add('clear');
        const q = new L3Queue(f.adapter, { maxBytes: 100, retryMs: 10 });
        const c = q.push({ kind: 'clear', bytes: 0 });
        const kept = [];
        for (let i = 0; i < 5; i++) kept.push(q.push({ kind: 'set', key: 'k' + i, value: 'v', bytes: 30 }));
        const shed = await q.push({ kind: 'set', key: 'overflow', value: 'v', bytes: 30 });
        ok(shed === false, 'work behind a stuck clear is shed once it passes the byte bound');
        ok(q.stats.shed > 0, `and counted as shed (${q.stats.shed})`);
        ok(q.pendingBytes <= 100, `the queue stays inside its bound (${q.pendingBytes}B)`);
        ok(f.calls.filter(x => x[0] === 'set').length === 0,
           'nothing waiting behind the clear was sent while it was stuck');
        const release = hold();
        q.close();
        ok(await c === false, 'and the stuck clear settles once the queue closes rather than hanging forever');
        for (const p of kept) await p;
        release();
    }

    // 18. a clear that has not been sent yet is replaced by a later one --
    //     two flushes in a row are one flush -- and the work the replaced
    //     clear had already swept up settles with the one that actually runs,
    //     rather than being forgotten along with it.
    {
        const f = makeFake(); f.latency.set('set', 30);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 's', value: 'v', bytes: 10 });   // on the wire
        const c1 = q.push({ kind: 'clear', bytes: 0 });             // dispatched, waiting for the wire
        const w = q.push({ kind: 'set', key: 'w', value: 'v', bytes: 10 });   // behind the gate
        const c2 = q.push({ kind: 'clear', bytes: 0 });             // queued behind c1; sweeps up w
        const c3 = q.push({ kind: 'clear', bytes: 0 });             // replaces c2
        const all = await Promise.all([c1, c2, c3, w]);
        ok(all.every(Boolean), `every caller settles, including the replaced clear (${all.join(',')})`);
        await q.drain();
        const order = f.calls.map(x => x[0]).join(',');
        ok(order === 'set,clear,clear', `the replaced clear is not sent (${order})`);
        ok(f.store.size === 0, `L3 is empty (${f.store.size} keys left)`);
        ok(q.chainCount === 0, `and no chain is left behind (${q.chainCount})`);
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-queue] all passed');
    process.exit(fail ? 1 : 0);
})();
