'use strict';
// THE TWO WAYS A CLEAR GENERATION CAN BE LEFT OPEN FOREVER.
//
// The clear guard lives in the arena header (decision 70): while `gen !=
// settled`, EVERY process on the box serves L3 misses for EVERY key. That is
// the right answer while a clear is genuinely outstanding, and a catastrophe if
// the generation is never closed -- the whole L3 tier stays dark until the
// arena is recreated. A worker cannot close its own generation (it maps the
// arena read-only), so the `-` that closes it is an IPC message, and an IPC
// message can fail to arrive in two ways:
//
//   1. THE WORKER IS STILL ALIVE AND THE MESSAGE STALLS. flush() defers a whole
//      batch while the IPC send window is full, and sheds it once the outbox is
//      also full. Nothing re-arms a deferred flush except the next write -- and
//      a worker that has just flushed its cache is exactly the process with
//      nothing more to say. This needs no crash: it is ordinary congestion.
//   2. THE WORKER VANISHES. SIGKILL, a crash, a container stop: the `-` is
//      never sent at all, and the primary is the only process that can write
//      the header.
//
// Both are exercised here against a real cluster, with the primary's own L3
// reads as the end-to-end evidence: blocked while the generation is open,
// working again once it is settled. The third case is the lever the `-` op
// creates -- a worker could previously only WIPE the cluster's cache; it must
// not be able to disarm someone else's clear.
const cluster = require('cluster');
const { TurboKV, MSG } = require('../src/turbokv');
const native = require('../src/native');
const { makeFake } = require('./l3_fake');

const ARENA = process.env.TCL_ARENA || ('/tcl3leak' + process.pid);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// --- the worker half -------------------------------------------------------
if (!cluster.isPrimary) {
    // `kill`: a clear that never lands, so the generation is still open when
    // the worker is killed. `stall`: one that lands on cue, so its `-` is
    // pushed at a moment we choose -- while the channel is congested.
    let releaseClear = null;
    const clearImpl = process.env.TC_CASE === 'kill'
        ? () => new Promise(() => {})
        : () => new Promise((r) => { releaseClear = r; });
    const adapter = {
        async get() { return undefined; },
        async set() {}, async delete() {},
        clear: clearImpl,
    };
    const opts = { storage: 'bytes', transport: 'ipc', l3: adapter };
    // Tiny window and tiny outbox, as in backpressure_test.js: a modest burst
    // then reaches the state that needs both to be full.
    if (process.env.TC_CASE === 'stall') { opts.maxInFlightBytes = 1024; opts.outboxMaxBytes = 2048; }
    const c = TurboKV.attachWorker(ARENA, cluster.worker.id, opts);
    process.on('message', (m) => { if (m && m.t === 'bye') process.exit(0); });

    (async () => {
        c.clearAll();
        await sleep(150);                       // the primary has applied the `+` by now
        process.send({ t: 'armed' });
        if (process.env.TC_CASE !== 'stall') return;

        // CONGEST THE CHANNEL, precisely rather than statistically. The bytes
        // reserved for a batch are returned by process.send's CALLBACK, so a
        // send whose callback never runs leaves the window full for as long as
        // we like -- which is what a congested channel really is.
        const realSend = process.send.bind(process);
        const held = [];
        process.send = (msg, cb) => { held.push(cb); return true; };

        let n = 0;
        const burst = () => { for (let i = 0; i < 600; i++) c.set('k' + (n++), 'V'.repeat(64)); c.flush(); };
        burst(); burst(); burst();              // window full, outbox over its cap: shedding

        releaseClear();                         // the L3 clear lands: `-` is pushed HERE
        await sleep(20);
        burst(); burst();                       // ... into a batch that is shed with the rest

        // The congestion clears: the sends complete and the window is returned.
        // Nothing is written after this point, deliberately -- the `-` has to
        // leave on its own, with no traffic to carry it.
        await sleep(20);
        process.send = realSend;
        for (const cb of held) if (typeof cb === 'function') cb(null);
        process.send({ t: 'shed', shed: c.stats.writesShed || 0 });
    })();
    return;
}

// --- the primary half ------------------------------------------------------
let fail = 0; const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };
const once = (w, t) => new Promise((resolve) => {
    const on = (m) => { if (m && m.t === t) { w.off('message', on); resolve(m); } };
    w.on('message', on);
});
const exited = (w) => new Promise((resolve) => w.once('exit', resolve));
// Polls rather than sleeps: the settle can arrive at any point once the
// congestion clears, and a fixed sleep would either be flaky or be slow.
const until = async (pred, ms) => {
    for (let waited = 0; waited < ms; waited += 25) { if (pred()) return true; await sleep(25); }
    return pred();
};

const f = makeFake();
for (const k of ['p1', 'p2', 'p3', 'p4']) f.store.set(k, { value: 'P', expiresAt: 0 });

(async () => {
    const primary = TurboKV.createPrimary(ARENA, 16 << 20, 1 << 14,
                                          { storage: 'bytes', transport: 'ipc', l3: f.adapter });
    TurboKV.install(cluster);                   // the whole primary-side wiring, reconciliation included

    // === 1. the worker lives, the settle meets a congested channel ==========
    const wA = cluster.fork({ TC_CASE: 'stall', TCL_ARENA: ARENA });
    await once(wA, 'armed');
    ok(native.l3ClearsInFlight() === 1, 'the worker opened a clear generation in the arena');
    const shed = await once(wA, 'shed');
    ok(shed.shed > 0, `its outbox really did shed under a full send window (writesShed=${shed.shed})`);
    const settledA = await until(() => native.l3ClearsInFlight() === 0, 4000);
    ok(settledA, 'the settle still arrives once the congestion clears, with no further writes to carry it');
    ok(await primary.getAsync('p1') === 'P', 'and the cluster reads L3 again');
    wA.send({ t: 'bye' });
    await exited(wA);

    // === 2. the worker vanishes with its clear still outstanding ===========
    const wB = cluster.fork({ TC_CASE: 'kill', TCL_ARENA: ARENA });
    await once(wB, 'armed');
    ok(native.l3ClearsInFlight() === 1, 'a clear that has not landed keeps the guard armed');
    ok(await primary.getAsync('p2') === undefined,
       'and every process serves L3 misses while it is -- including the primary');

    // === 3. the `-` op is not a lever anyone may pull ======================
    // Applied exactly as a worker's batch would be, under a writer id that has
    // opened nothing. Before the per-writer accounting this disarmed the
    // cluster-wide guard for everybody.
    TurboKV.applyBatch({ t: MSG, id: 424242, b: ['-', '', null, 0] });
    ok(native.l3ClearsInFlight() === 1,
       `a settle from a writer that opened nothing disarms nothing (${native.l3ClearsInFlight()})`);
    ok(TurboKV.releaseWorker(424242) === 0, 'and that writer has nothing to release either');
    ok(await primary.getAsync('p3') === undefined, 'the guard is still armed after the forged settle');

    // The count is per writer and it is a COUNT: one worker clearing twice
    // before the first lands owes two, and the first `-` must not disarm the
    // second clear (the same reason the process-local counter is a count).
    const open2 = { t: MSG, id: 777, b: ['+', '', null, 0] };
    TurboKV.applyBatch(open2); TurboKV.applyBatch(open2);
    ok(native.l3ClearsInFlight() === 3,
       `two clears from one worker are two generations (${native.l3ClearsInFlight()})`);
    TurboKV.applyBatch({ t: MSG, id: 777, b: ['-', '', null, 0] });
    ok(native.l3ClearsInFlight() === 2,
       `the first settle closes one of them, not both (${native.l3ClearsInFlight()})`);
    ok(TurboKV.releaseWorker(777) === 1, 'and releasing that writer settles the one still owed');
    ok(native.l3ClearsInFlight() === 1,
       `leaving the OTHER worker's generation untouched (${native.l3ClearsInFlight()})`);

    wB.kill('SIGKILL');
    await exited(wB);
    await sleep(50);                            // the 'exit' handler runs first; give it a turn
    ok(native.l3ClearsInFlight() === 0,
       `the primary settles what a vanished worker still owed (${native.l3ClearsInFlight()})`);
    ok(await primary.getAsync('p4') === 'P', 'and the cluster reads L3 again rather than staying dark');
    ok(TurboKV.releaseWorker(wB.id) === 0, 'releasing a worker twice settles nothing the second time');

    await primary.close();
    console.log(fail ? `  ${fail} FAILURES` : '  [l3-clear-leak] all passed');
    process.exit(fail ? 1 : 0);
})();

// A hang here is a failure, not a reason to wedge CI behind the suite timeout.
const guard = setTimeout(() => { console.log('  TIMEOUT'); process.exit(1); }, 60000);
if (guard.unref) guard.unref();
