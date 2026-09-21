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
// working again once it is settled. Three more cases guard the machinery
// itself: the `-` op is a lever that disarms a CLUSTER-WIDE guard, so a worker
// must not be able to pull it for someone else; RECONCILIATION is the same
// lever held by the primary, so a dead worker's reconciliation must not settle
// the clear of a live successor that reuses its writer id; and the congestion
// that could strand a `-` could also strand the `c` it belongs to, which loses
// the clearAll itself while its bookkeeping reports success.
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
    const HELD_OPEN = ['kill', 'recycle', 'shedclear', 'nononce'];
    const clearImpl = HELD_OPEN.includes(process.env.TC_CASE)
        ? () => new Promise(() => {})           // never lands: the generation stays open
        : () => new Promise((r) => { releaseClear = r; });
    const adapter = {
        async get() { return undefined; },
        async set() {}, async delete() {},
        clear: clearImpl,
    };
    const opts = { storage: 'bytes', transport: 'ipc', l3: adapter };
    // Tiny window and tiny outbox, as in backpressure_test.js: a modest burst
    // then reaches the state that needs both to be full.
    if (process.env.TC_CASE === 'stall' || process.env.TC_CASE === 'shedclear') {
        opts.maxInFlightBytes = 1024; opts.outboxMaxBytes = 2048;
    }
    // The writer id is the CALLER's to choose, and a stable per-slot index out
    // of the environment is the ordinary way to do it -- which is exactly how
    // two different worker processes come to share one id.
    const id = process.env.TC_ID ? Number(process.env.TC_ID) : cluster.worker.id;
    const c = TurboKV.attachWorker(ARENA, id, opts);
    process.on('message', (m) => { if (m && m.t === 'bye') process.exit(0); });

    // CONGEST THE CHANNEL, precisely rather than statistically. The bytes
    // reserved for a batch are returned by process.send's CALLBACK, so a send
    // whose callback never runs leaves the window full for as long as we like
    // -- which is what a congested channel really is.
    const realSend = process.send.bind(process);
    const held = [];
    const congest = () => { process.send = (msg, cb) => { held.push(cb); return true; }; };
    const relieve = () => {
        process.send = realSend;
        for (const cb of held) if (typeof cb === 'function') cb(null);
    };
    let n = 0;
    const burst = () => { for (let i = 0; i < 600; i++) c.set('k' + (n++), 'V'.repeat(64)); c.flush(); };

    (async () => {
        // A WORKER FROM BEFORE THE NONCE EXISTED, simulated by stripping the
        // field this version adds. A mixed-version rolling restart is exactly
        // the stable-slot deployment that makes writer ids collide, so a
        // primary that fell back to the id for such a worker would re-open the
        // hole the nonce closes -- silently, and only during an upgrade.
        if (process.env.TC_CASE === 'nononce') {
            process.send = (msg, cb) => {
                if (msg && msg.n) { const copy = { ...msg }; delete copy.n; return realSend(copy, cb); }
                return realSend(msg, cb);
            };
        }

        // The clear that is SHED rather than stalled: this worker congests the
        // channel FIRST, so the clearAll itself lands in a batch that is shed.
        if (process.env.TC_CASE === 'shedclear') {
            congest();
            burst(); burst(); burst();          // window full, outbox over its cap: shedding
            c.clearAll();                       // `c` and `+` pushed into that state
            burst(); burst();                   // ... and shed with the next batch
            relieve();
            c.flush();                          // one ordinary flush, no new writes
            await sleep(50);
            process.send({ t: 'shed', shed: c.stats.writesShed || 0 });
            return;
        }

        c.clearAll();
        await sleep(150);                       // the primary has applied the `+` by now
        process.send({ t: 'armed' });
        if (process.env.TC_CASE !== 'stall') return;

        congest();
        burst(); burst(); burst();              // window full, outbox over its cap: shedding

        releaseClear();                         // the L3 clear lands: `-` is pushed HERE
        await sleep(20);
        burst(); burst();                       // ... into a batch that is shed with the rest

        // The congestion clears: the sends complete and the window is returned.
        // Nothing is written after this point, deliberately -- the `-` has to
        // leave on its own, with no traffic to carry it.
        await sleep(20);
        relieve();
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
for (const k of ['p1', 'p2', 'p3', 'p4', 'p5', 'p6']) f.store.set(k, { value: 'P', expiresAt: 0 });

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
    // Kept the way install() keeps one: a worker is named by its attachment,
    // and any of its messages carries it.
    let lastB = null;
    wB.on('message', (m) => { if (TurboKV.isCacheMessage(m)) lastB = m; });
    await once(wB, 'armed');
    ok(native.l3ClearsInFlight() === 1, 'a clear that has not landed keeps the guard armed');
    ok(await primary.getAsync('p2') === undefined,
       'and every process serves L3 misses while it is -- including the primary');

    // === 3. the `-` op is not a lever anyone may pull ======================
    // Applied exactly as a worker's batch would be, under a writer id that has
    // opened nothing. Before the per-writer accounting this disarmed the
    // cluster-wide guard for everybody.
    const forged = { t: MSG, id: 424242, n: 'attach-forged', b: ['-', '', null, 0] };
    TurboKV.applyBatch(forged);
    ok(native.l3ClearsInFlight() === 1,
       `a settle from a sender that opened nothing disarms nothing (${native.l3ClearsInFlight()})`);
    ok(TurboKV.releaseWorker(forged) === 0, 'and that sender has nothing to release either');
    ok(await primary.getAsync('p3') === undefined, 'the guard is still armed after the forged settle');

    // The OLD call shape is a mistake now, not a deprecated spelling: it would
    // settle nothing and leak a generation on every worker replacement, so it
    // must fail loudly rather than quietly.
    let threw = null;
    try { TurboKV.releaseWorker(424242); } catch (e) { threw = e; }
    ok(threw instanceof TypeError && /cache message/.test(threw.message),
       `releasing by writer id throws rather than silently settling nothing (${threw && threw.message})`);

    // The count is per writer and it is a COUNT: one worker clearing twice
    // before the first lands owes two, and the first `-` must not disarm the
    // second clear (the same reason the process-local counter is a count).
    const open2 = { t: MSG, id: 777, n: 'attach-777', b: ['+', '', null, 0] };
    TurboKV.applyBatch(open2); TurboKV.applyBatch(open2);
    ok(native.l3ClearsInFlight() === 3,
       `two clears from one worker are two generations (${native.l3ClearsInFlight()})`);
    TurboKV.applyBatch({ t: MSG, id: 777, n: 'attach-777', b: ['-', '', null, 0] });
    ok(native.l3ClearsInFlight() === 2,
       `the first settle closes one of them, not both (${native.l3ClearsInFlight()})`);
    ok(TurboKV.releaseWorker(open2) === 1, 'and releasing that attachment settles the one still owed');
    ok(native.l3ClearsInFlight() === 1,
       `leaving the OTHER worker's generation untouched (${native.l3ClearsInFlight()})`);

    wB.kill('SIGKILL');
    await exited(wB);
    await sleep(50);                            // the 'exit' handler runs first; give it a turn
    ok(native.l3ClearsInFlight() === 0,
       `the primary settles what a vanished worker still owed (${native.l3ClearsInFlight()})`);
    ok(await primary.getAsync('p4') === 'P', 'and the cluster reads L3 again rather than staying dark');
    ok(TurboKV.releaseWorker(lastB) === 0, 'releasing the same attachment twice settles nothing');

    // === 4. a recycled writer id ==========================================
    // A supervisor that names workers by slot reuses the number when it
    // replaces one. The predecessor's reconciliation must not settle the
    // SUCCESSOR's clear -- and the successor can attach before the
    // predecessor's 'exit' is delivered, which is the ordering that makes
    // "release everything under this id" wrong however it is spelled.
    const wC = cluster.fork({ TC_CASE: 'recycle', TC_ID: '5', TCL_ARENA: ARENA });
    await once(wC, 'armed');
    ok(native.l3ClearsInFlight() === 1, 'the first worker in slot 5 opened a generation');
    const wD = cluster.fork({ TC_CASE: 'recycle', TC_ID: '5', TCL_ARENA: ARENA });
    await once(wD, 'armed');
    ok(native.l3ClearsInFlight() === 2, 'its replacement in the same slot opened another');
    wC.kill('SIGKILL');
    await exited(wC);
    await sleep(50);
    ok(native.l3ClearsInFlight() === 1,
       `the dead worker's reconciliation settles ITS generation only (${native.l3ClearsInFlight()})`);
    ok(await primary.getAsync('p5') === undefined,
       'so the live successor\'s clear is still guarded, rather than resurrecting what it removes');
    wD.kill('SIGKILL');
    await exited(wD);
    await sleep(50);
    ok(native.l3ClearsInFlight() === 0, 'and the successor is reconciled in its turn');
    ok(await primary.getAsync('p6') === 'P', 'after which L3 reads work again');

    // === 5. a batch that names no attachment ==============================
    // Version skew, not malice: a worker built before the nonce existed. Its
    // `+` must not open a generation -- nothing could reliably settle one,
    // since its `-` names nothing either and reconciliation has no handle --
    // while its `c` still applies, because a wipe can only remove data and
    // refusing it would leave L2 serving what that worker's L3 clear removes.
    primary.set('kept2', 'OLD');
    const wF = cluster.fork({ TC_CASE: 'nononce', TC_ID: '7', TCL_ARENA: ARENA });
    await once(wF, 'armed');
    ok(native.l3ClearsInFlight() === 0,
       `an unidentified batch opens no clear generation (${native.l3ClearsInFlight()})`);
    ok(native.get('kept2') === undefined, `but its clearAll still wiped L2 (${native.get('kept2')})`);
    ok(/names no attachment/.test(primary.lastError || ''),
       `and the refusal is reported rather than silent (${JSON.stringify(primary.lastError)})`);

    // ... and it cannot take a REAL worker's generation with it when it dies,
    // even though they share writer id 7.
    const wG = cluster.fork({ TC_CASE: 'recycle', TC_ID: '7', TCL_ARENA: ARENA });
    await once(wG, 'armed');
    ok(native.l3ClearsInFlight() === 1, 'a properly attached worker in the same slot opens one');
    wF.kill('SIGKILL');
    await exited(wF);
    await sleep(50);
    ok(native.l3ClearsInFlight() === 1,
       `the unidentified worker's death settles nothing (${native.l3ClearsInFlight()})`);
    wG.kill('SIGKILL');
    await exited(wG);
    await sleep(50);
    ok(native.l3ClearsInFlight() === 0, 'and the real one is reconciled as usual');

    // === 6. a clearAll shed by a congested channel =========================
    // The `-` is not the only op that can be thrown away under congestion.
    // Losing the `c` loses the clearAll itself while its own bookkeeping
    // reports success: the guard opens and settles cleanly, and L2 goes on
    // serving values L3 no longer has.
    primary.set('kept', 'OLD');
    ok(native.get('kept') === 'OLD', 'the primary has a value in L2 for the worker to clear');
    const wE = cluster.fork({ TC_CASE: 'shedclear', TC_ID: '9', TCL_ARENA: ARENA });
    const shedE = await once(wE, 'shed');
    ok(shedE.shed > 0, `the worker's outbox shed under a full window (writesShed=${shedE.shed})`);
    ok(native.get('kept') === undefined,
       `the clearAll still reached L2 rather than being shed with the writes (${native.get('kept')})`);
    wE.kill('SIGKILL');
    await exited(wE);
    await sleep(50);

    await primary.close();
    console.log(fail ? `  ${fail} FAILURES` : '  [l3-clear-leak] all passed');
    process.exit(fail ? 1 : 0);
})();

// A hang here is a failure, not a reason to wedge CI behind the suite timeout.
const guard = setTimeout(() => { console.log('  TIMEOUT'); process.exit(1); }, 60000);
if (guard.unref) guard.unref();
