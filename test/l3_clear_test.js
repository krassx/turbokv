'use strict';
// A CLEAR IS CLUSTER-WIDE, AND SO MUST THE GUARD THAT PROTECTS IT BE.
//
// `clearAll()` empties L1, L2 and L3. The L3 half takes a round trip, and until
// it lands L3 still answers with everything the clear is removing -- so a read
// that goes through to L3 in that window promotes those values back into the
// arena, with a fresh TTL, and the clear has undone itself (decision 70).
//
// The process that issued the clear guards its own reads with a module-scope
// counter. THAT IS NOT ENOUGH, because the arena is shared across PROCESSES and
// the counter is not: the other process reads L3 unguarded, promotes into the
// SHARED arena, and the issuing process then serves the value it cleared out of
// L2 -- its own guard never consulted, because the promotion was not its own.
// Both directions are the same hole, so both are tested here:
//
//   1. a WORKER clears, the PRIMARY reads through to L3;
//   2. the PRIMARY clears, a WORKER reads through to L3.
//
// Plus the in-process case where the clear BEGINS while a read is already in
// flight, which the entry guard cannot see and only the promotion guard can.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { fork } = require('child_process');
const { TurboKV } = require('../src/turbokv');
// The damage is in the SHARED arena rather than in any instance's L1, so the
// assertions have to look there directly.
const native = require('../src/native');
const { makeFake } = require('./l3_fake');

let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const ARENA = process.env.TCC_ARENA || ('/tcl3clear' + process.pid);
// A FILE-BACKED L3, because the two processes have to share one. An in-memory
// fake cannot express this defect at all: the whole failure is one process
// reading values that another process's clear has not removed yet.
const FILE = process.env.TCC_FILE || path.join(os.tmpdir(), 'tc-l3-clear-' + process.pid + '.json');

const load = () => { try { return JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch { return {}; } };
const save = (o) => fs.writeFileSync(FILE, JSON.stringify(o));
// `clearMs` is what holds the clear in flight while the assertions run. It is
// the whole point: a clear that lands instantly leaves no window to test.
const fileL3 = (clearMs, getMs = 0) => ({
    async get(k) { if (getMs) await sleep(getMs); const s = load(); return k in s ? { value: s[k] } : undefined; },
    async set(k, v) { const s = load(); s[k] = v; save(s); },
    async delete(k) { const s = load(); delete s[k]; save(s); },
    async clear() { await sleep(clearMs); save({}); },
});

// A one-shot wait for a coordination message, so each phase runs in a known
// order rather than on a sleep long enough to hide a regression.
const waitFor = (target, t) => new Promise((resolve) => {
    const on = (m) => { if (m && m.t === t) { target.off('message', on); resolve(m); } };
    target.on('message', on);
});

// --- the worker half -------------------------------------------------------
if (process.env.TCC_ROLE === 'worker') {
    (async () => {
        const w = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l3: fileL3(400, 200) });
        const say = (t) => process.send({ t });

        // PHASE 1: this worker clears. Its L3 clear takes 400ms, and the
        // primary reads through to L3 inside that window.
        ok(w.get('k') === 'v', `the worker sees the key before the clear (${w.get('k')})`);
        w.clearAll();
        // The clear reaches the primary on a scheduled flush; this keeps the
        // coordination message behind it, so the primary's assertions are
        // about a clear it has already applied rather than about a race.
        await sleep(30);
        say('cleared');
        await waitFor(process, 'checked');
        await sleep(500);                       // the worker's own clear has landed by now
        ok(load().k === undefined, 'the L3 store really is empty afterwards');
        ok(w.get('k') === undefined,
           `the clearing process is not served its own cleared value out of L2 (${w.get('k')})`);
        const back = await w.getAsync('k');
        ok(back === undefined, `nor by reading through to L3 (${back})`);

        // PHASE 2: the mirror. The PRIMARY clears; this worker reads.
        say('phase2');
        await waitFor(process, 'primary-cleared');
        const got = await w.getAsync('m');
        ok(got === undefined,
           `a worker misses while ANOTHER process's clear is in flight (${got})`);
        say('phase2-read');

        // PHASE 3: THE HEADER LAG. A worker cannot write the arena header, so
        // its generation is opened by the primary when it applies the batch --
        // and a read this worker already had in flight when it called
        // clearAll() lands inside that window. The primary holds the batch
        // here to make that window deterministic; in production it is one
        // scheduled flush wide. Only the process-local counter can cover it,
        // which is why the promotion guard has to consult BOTH.
        say('phase3');
        await waitFor(process, 'holding');
        const read = w.getAsync('zz');           // 200ms in the adapter
        await sleep(20);
        w.clearAll();                            // local counter armed; the header is not
        const lagged = await read;
        ok(lagged === undefined,
           `a read already in flight when THIS process cleared still misses (${lagged})`);
        ok(w.get('zz') === undefined, `and nothing was promoted (${w.get('zz')})`);
        say('phase3-read');

        await waitFor(process, 'bye');
        await w.close();
        console.log(fail ? `  ${fail} failed (worker)` : '  [l3-clear] worker cases passed');
        process.exit(fail ? 1 : 0);
    })();
    return;
}

// --- the primary half ------------------------------------------------------
(async () => {
    // A clear that starts WHILE A READ IS IN FLIGHT, in one process. The entry
    // guard cannot catch this one -- when the read started there was no clear
    // -- so it is the promotion guard's own case, and the reason it reports
    // has to be the clear rather than ordinary per-key contention.
    {
        const f = makeFake();
        f.store.set('c', { value: 'OLD', expiresAt: 0 });
        f.latency.set('get', 60);
        f.latency.set('clear', 150);             // still in flight when the read resolves
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const read = c.getAsync('c');
        await sleep(10);
        c.clearAll();
        const got = await read;
        ok(got === undefined, `the read misses rather than serving what the clear removes (${got})`);
        ok(c.get('c') === undefined, `and nothing is promoted (${c.get('c')})`);
        ok(native.get('c') === undefined, `not into the shared arena either (${native.get('c')})`);
        ok(c.stats.l3ClearedWhileReading === 1,
           `counted as blocked by the clear, not as key contention (${c.stats.l3ClearedWhileReading})`);

        // AND IT DISARMS AGAIN once the clear lands -- a guard that latched
        // would pass every assertion above for the wrong reason, and would
        // silently mask the cross-process cases below, which run in this same
        // process against the same module-scope counter.
        await c.drainL3();
        f.store.set('after', { value: 'NEW', expiresAt: 0 });
        ok(await c.getAsync('after') === 'NEW', 'a read after the clear landed reaches L3 again');
        ok(c.get('after') === 'NEW', `and is promoted again (${c.get('after')})`);
        await c.close();
    }

    // A clear that BEGINS AND LANDS entirely inside one read. By the time the
    // promotion is decided, nothing is in flight any more -- only the
    // GENERATION, sampled before the await and compared after it, still says a
    // clear happened at all. A real L3 answers from a snapshot older than its
    // response, and another box may have written the key back in the meantime;
    // this read cannot tell those apart, so it refuses, which costs residency
    // for one read and never correctness.
    {
        const f = makeFake();
        f.store.set('g', { value: 'OLD', expiresAt: 0 });
        f.latency.set('get', 80);
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        const read = c.getAsync('g');
        await sleep(10);
        c.clearAll();                            // no clear latency: it lands at once
        await c.drainL3();
        // What the read still in flight will be handed when it completes.
        f.store.set('g', { value: 'OLD', expiresAt: 0 });
        const got = await read;
        ok(got === undefined, `a clear that came and went during the read still blocks it (${got})`);
        ok(c.get('g') === undefined, `and nothing is promoted (${c.get('g')})`);
        ok(c.stats.l3ClearedWhileReading === 1,
           `counted as the clear it was (${c.stats.l3ClearedWhileReading})`);
        ok(c.stats.l3PromotionsBlocked === undefined,
           `and not also as key contention (${c.stats.l3PromotionsBlocked})`);
        await c.close();
    }

    // A DEGRADED handle has no arena to ask. The clear generation lives in the
    // arena header, so the guard has to fall back rather than throw -- and it
    // loses nothing by doing so: a degraded handle cannot promote into L2 at
    // all, and its OWN clears are still covered by the process-wide counter.
    {
        const f = makeFake();
        f.store.set('d', { value: 'L3', expiresAt: 0 });
        const c = TurboKV.open({ storage: 'bytes', l3: f.adapter });
        c.__unsafeForcePrimaryDead();
        const got = await c.getAsync('d');
        ok(got === 'L3', `a degraded handle still reads through to L3 (${got})`);
        ok(c.stats.l3PromotionsBlocked === 1,
           `and refuses the promotion for want of a ring, not for want of a header (${c.stats.l3PromotionsBlocked})`);
        await c.close();
    }

    // The cross-process cases need a real arena, a real primary and a real
    // worker: the counter that used to be the whole guard is module-scope, so
    // nothing inside one process can show what it fails to cover.
    save({ k: 'v', m: 'v' });
    const primary = TurboKV.createPrimary(ARENA, 16 << 20, 1 << 14, { storage: 'bytes', l3: fileL3(800) });
    primary.set('k', 'v');
    primary.set('m', 'v');
    await primary.drainL3();

    const kid = fork(__filename, [], {
        env: { ...process.env, TCC_ROLE: 'worker', TCC_ARENA: ARENA, TCC_FILE: FILE },
        stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
    });
    const say = (t) => { if (kid.connected) kid.send({ t }); };

    // Phase 3 holds the worker's batches instead of applying them, which is
    // the header lag stretched out far enough to assert on.
    let held = null;

    kid.on('message', async (m) => {
        // The worker's clearAll travels as an ordinary batch, exactly as it
        // does in a cluster; this is the primary applying it.
        if (TurboKV.isCacheMessage(m)) { if (held) held.push(m); else TurboKV.applyBatch(m); return; }

        if (m.t === 'cleared') {
            ok(primary.get('k') === undefined, `the worker's clear emptied L2 (${primary.get('k')})`);
            ok(load().k === 'v', 'and L3 still holds the value, because that clear has not landed yet');
            const v = await primary.getAsync('k');
            ok(v === undefined,
               `the primary misses while another process's clear is in flight (${v})`);
            ok(native.get('k') === undefined,
               `and promotes nothing into the SHARED arena (${native.get('k')})`);
            say('checked');
            return;
        }
        if (m.t === 'phase2') {
            save({ m: 'v' });                    // the worker's clear emptied the store
            primary.clearAll();                  // ... and now the PRIMARY clears, slowly
            say('primary-cleared');
            return;
        }
        if (m.t === 'phase2-read') {
            // A worker's promotion travels through the submission ring, so a
            // resurrection would land in L2 only once the primary drains it.
            TurboKV.drainSubmissions(8192);
            ok(native.get('m') === undefined,
               `no worker promotion reached L2 during the primary's clear (${native.get('m')})`);
            return;
        }
        if (m.t === 'phase3') {
            // This primary's own clear must be FULLY settled first, or the
            // header would still be armed and would mask the window under
            // test.
            await primary.drainL3();
            ok(native.l3ClearsInFlight() === 0, 'no clear is outstanding anywhere before phase 3');
            save({ zz: 'v' });                   // only L3 holds it: L2 was cleared
            held = [];                           // ... and now the primary stops applying batches
            say('holding');
            return;
        }
        if (m.t === 'phase3-read') {
            for (const b of held) TurboKV.applyBatch(b);
            held = null;
            TurboKV.drainSubmissions(8192);
            ok(native.get('zz') === undefined,
               `nothing the worker read inside the lag reached L2 (${native.get('zz')})`);
            say('bye');
        }
    });

    const code = await new Promise((resolve) => kid.on('exit', resolve));
    ok(code === 0, `the worker cases passed (child exited ${code})`);

    // A clear that is still in flight keeps the guard armed, and close() is
    // what settles it -- otherwise a clear that never lands would leave every
    // process in the cluster serving L3 misses for the life of the arena.
    await primary.close();

    try { fs.unlinkSync(FILE); } catch { /* best effort */ }
    console.log(fail ? `  ${fail} failed` : '  [l3-clear] all passed');
    process.exit(fail ? 1 : 0);
})();
