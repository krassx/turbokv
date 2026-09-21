'use strict';
// WHAT install() KEEPS PER WORKER.
//
// Reconciliation has to be able to name a channel after it is gone (see
// decision 70), so install() remembers something from every batch it routes.
// That something must be the NAME and not the message: `m.b` is the batch --
// every key and every encoded value in it -- so keeping the message pins the
// largest batch that worker ever sent for the whole life of the channel, in a
// process whose entire purpose is a bounded memory footprint.
//
// Proven by retention rather than by inspection: a WeakRef to the batch, a full
// GC, and the batch must be gone. Deterministic, not timing-based -- there are
// no processes, no sockets and no timers here. install() takes any object with
// `on()`, so the worker is a plain EventEmitter and the messages are delivered
// by hand, which is exactly what applyBatch's contract describes.
const path = require('path');

// --expose-gc is the only way to ask for a full GC, and it has to be on the
// command line. Re-exec once rather than asking run.js to pass it to every
// test, and rather than skipping -- a retention test that silently does not
// run is worse than no test.
if (typeof global.gc !== 'function') {
    const { execFileSync } = require('child_process');
    try {
        execFileSync(process.execPath, ['--expose-gc', path.join(__dirname, 'install_retain_test.js')],
                     { stdio: 'inherit', env: process.env });
    } catch (e) { process.exit(typeof e.status === 'number' ? e.status : 1); }
    process.exit(0);
}

const { EventEmitter } = require('events');
const { TurboKV, MSG } = require('../src/turbokv');

let fail = 0; const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };

// The batch is built, delivered and dropped inside this call, so nothing but
// install() can still be holding it when it returns. A module-scope binding
// would keep it alive on its own and prove nothing.
function deliver(worker) {
    const value = 'V'.repeat(1 << 20);                  // 1MB, so a leak is a real one
    const msg = { t: MSG, id: 1, n: 'attach-retain-test', b: ['s', 'retained', value, 0] };
    const ref = new WeakRef(msg.b);
    worker.emit('message', msg);
    return ref;
}

const collected = async (ref) => {
    // Several passes with a turn of the loop between them: one full GC is
    // normally enough, but a value can still be live in a register or on the
    // stack at the moment gc() is called, and yielding drops that frame.
    for (let i = 0; i < 10; i++) {
        global.gc();
        if (ref.deref() === undefined) return true;
        await new Promise((r) => setImmediate(r));
    }
    return ref.deref() === undefined;
};

(async () => {
    const cache = TurboKV.open({ storage: 'bytes' });
    const worker = new EventEmitter();
    // install() reads `cluster.workers` and subscribes to 'online'/'fork'; a
    // plain object with the one worker in it is all it needs.
    TurboKV.install({ on() {}, workers: { 1: worker } });

    const ref = deliver(worker);
    ok(cache.get('retained') !== undefined, 'the batch really was applied (so it really was routed)');

    ok(await collected(ref), 'install() does not pin the batch it routed');

    // And what it kept is still enough to reconcile with: releaseWorker must
    // accept it and find nothing owed, rather than throwing on a half-kept name.
    worker.emit('exit', 0);
    ok(TurboKV.releaseWorker({ t: MSG, id: 1, n: 'attach-retain-test' }) === 0,
       'and the name it kept is still a name reconciliation can use');

    await cache.close();
    console.log(fail ? `  ${fail} FAILURES` : '  [install-retain] all passed');
    process.exit(fail ? 1 : 0);
})();
