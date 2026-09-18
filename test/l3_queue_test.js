'use strict';
// The queue is where "local first, then L3" becomes safe. Order per key is a
// correctness requirement: two writes to one key sent over a pool can arrive
// in either order, leaving L3 with the older value while this box holds the
// newer one.
const { L3Queue } = require('../src/l3/queue');
const { makeFake, delay } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

(async () => {
    // 1. order per key is preserved even when the adapter is slow
    {
        const f = makeFake(); f.latency.set('set', 20);
        const q = new L3Queue(f.adapter, {});
        q.push({ kind: 'set', key: 'k', value: 'A', bytes: 10 });
        const last = q.push({ kind: 'set', key: 'k', value: 'B', bytes: 10 });
        await last; await q.drain();
        ok(f.store.get('k').value === 'B', 'the last write to a key wins in L3');
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

    // 4. a transient failure is retried inside the budget and settles true
    {
        const f = makeFake();
        f.fail.set('set', new Error('connection reset'));
        const q = new L3Queue(f.adapter, { retryMs: 500 });
        const p = q.push({ kind: 'set', key: 'r', value: 'v', bytes: 10 });
        setTimeout(() => f.fail.delete('set'), 40);
        ok(await p === true, 'a retry that succeeds inside the budget settles true');
        ok(q.stats.retried > 0, `retries counted (${q.stats.retried})`);
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
        for (let i = 0; i < 20; i++) {
            const p = q.push({ kind: 'set', key: 'k' + i, value: 'v', bytes: 30 });
            (i < 4 ? kept : shed).push(p);
        }
        const results = await Promise.all(shed);
        ok(results.some(r => r === false), 'work past the bound is shed');
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

    console.log(fail ? `  ${fail} failed` : '  [l3-queue] all passed');
    process.exit(fail ? 1 : 0);
})();
