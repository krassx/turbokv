'use strict';
// A CLOSED CACHE MUST NOT HOLD THE PROCESS OPEN.
//
// Decision 71 exists because "a synchronous close() would leave a process that
// cannot exit -- a hung CI job rather than a failing test". The bounded waits it
// added made close() RETURN during an L3 outage; they did not make the queue
// STOP. A `clear` is never shed and retries indefinitely by design (decision
// 70), and its retry backoff was a ref'd `setTimeout` that went on rescheduling
// itself after close() had returned -- against a cache that no longer existed.
// Same defect as the one decision 71 closed, reached by a different route.
//
// The SAME defect exists at the other end of the same lifecycle, and the fix
// for one is what reintroduced the other: an operation still IN SERVICE must
// hold the loop open. withDeadline's timer was unref'd unconditionally, so
// against an adapter that hangs -- neither resolving nor rejecting -- with
// nothing else ref'd, the process exited BEFORE ITS OWN DEADLINE and `await
// setAsync` never settled and never printed. Exit code 0, no output, no error:
// the work was dropped with no caller left to hear about it. Decision 71
// measured exactly this symptom for the retry backoff and called it a defect.
//
// This has to be tested in a CHILD PROCESS, because the property under test is
// "the process exits" (or does not), which nothing inside that process can
// observe. The parent's kill timer is the timeout guard: a regression fails an
// assertion here rather than wedging CI until the job-level limit kills it.
const { spawn } = require('child_process');
const path = require('path');

let fail = 0;
const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

const SRC = JSON.stringify(path.join(__dirname, '..', 'src', 'turbokv'));
// A `clear` that can never succeed, so the queue retries it forever. Nothing
// else in the child holds the event loop: close() clears the maintenance timer,
// drops the heap-guard subscription and destroys the arena, so the ONLY reason
// this child could fail to exit is the queue's own retry loop.
const CHILD = `
const { TurboKV } = require(${SRC});
const adapter = {
    async get() { return undefined; },
    async set() {},
    async delete() {},
    async clear() { throw new Error('L3 permanently down'); },
};
(async () => {
    const c = TurboKV.open({ storage: 'bytes', l3: adapter, l3CloseTimeoutMs: 50, l3RetryMs: 20 });
    c.clearAll();                 // queues a clear that will never land
    await c.close();
    process.stdout.write('CLOSED');
})();
`;

const BOUND_MS = 8000;

(async () => {
    const child = spawn(process.execPath, ['-e', CHILD], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { err += d; });

    const result = await new Promise((resolve) => {
        const killer = setTimeout(() => {
            child.kill('SIGKILL');
            resolve({ exited: false });
        }, BOUND_MS);
        child.on('exit', (code, signal) => {
            clearTimeout(killer);
            resolve({ exited: true, code, signal });
        });
    });

    ok(out.includes('CLOSED'),
       `the child's close() resolved (stdout=${JSON.stringify(out)} stderr=${err.slice(0, 400)})`);
    ok(result.exited,
       `a closed cache lets the process exit; it was still alive after ${BOUND_MS}ms and had to be killed`);
    ok(result.exited && result.code === 0 && result.signal === null,
       `and exits cleanly (code=${result.code} signal=${result.signal})`);

    // ------------------------------------------------------------------
    // A HUNG ADAPTER: the process must outlive its own deadline, settle the
    // caller, and only then exit. `throw` is the control -- decision 71 already
    // fixed that one, via the ref'd retry backoff -- and `hang` is the route
    // that had no backoff to be held open by, because the operation never
    // reaches the catch that starts one.
    for (const mode of ['throw', 'hang']) {
        const body = mode === 'hang' ? 'new Promise(() => {})' : 'Promise.reject(new Error("down"))';
        const src = `
const { TurboKV } = require(${SRC});
const adapter = {
    async get() { return undefined; },
    set() { return ${body}; },
    async delete() {}, async clear() {},
};
(async () => {
    const c = TurboKV.open({ storage: 'bytes', l3: adapter, l3RetryMs: 300 });
    const r = await c.setAsync('k', 'v');
    process.stdout.write('SETTLED:' + r);
    await c.close();
})();
`;
        const kid = spawn(process.execPath, ['-e', src], { stdio: ['ignore', 'pipe', 'pipe'] });
        let o = '';
        kid.stdout.on('data', (d) => { o += d; });
        const t0 = Date.now();
        const r = await new Promise((resolve) => {
            const killer = setTimeout(() => { kid.kill('SIGKILL'); resolve({ exited: false }); }, BOUND_MS);
            kid.on('exit', (code, signal) => { clearTimeout(killer); resolve({ exited: true, code, signal }); });
        });
        const ms = Date.now() - t0;
        ok(o === 'SETTLED:false',
           `[${mode}] the caller was told the write failed (stdout=${JSON.stringify(o)})`);
        // The deadline is 300ms and the process used to exit at ~37ms. Anything
        // under the bound means it exited before its own timer could fire.
        ok(ms >= 250, `[${mode}] the process outlived its own 300ms deadline (${ms}ms)`);
        ok(r.exited && r.code === 0 && r.signal === null,
           `[${mode}] and then exited cleanly (code=${r.code} signal=${r.signal})`);
    }

    // ------------------------------------------------------------------
    // ...and close() still wins. A cache closed while an adapter call is hung
    // must not be detained by that call's deadline, however long it has left.
    {
        const src = `
const { TurboKV } = require(${SRC});
const adapter = {
    get() { return new Promise(() => {}); },
    async set() {}, async delete() {}, async clear() {},
};
(async () => {
    const c = TurboKV.open({ storage: 'bytes', l3: adapter, l3RetryMs: 60000, l3CloseTimeoutMs: 50 });
    c.getAsync('k');              // hangs against a 60s deadline, deliberately not awaited
    await new Promise((r) => setTimeout(r, 30));
    await c.close();
    process.stdout.write('CLOSED');
})();
`;
        const kid = spawn(process.execPath, ['-e', src], { stdio: ['ignore', 'pipe', 'pipe'] });
        let o = '';
        kid.stdout.on('data', (d) => { o += d; });
        const t0 = Date.now();
        const r = await new Promise((resolve) => {
            const killer = setTimeout(() => { kid.kill('SIGKILL'); resolve({ exited: false }); }, BOUND_MS);
            kid.on('exit', (code, signal) => { clearTimeout(killer); resolve({ exited: true, code, signal }); });
        });
        const ms = Date.now() - t0;
        ok(o === 'CLOSED', `close() resolved with a read hung against a 60s deadline (stdout=${JSON.stringify(o)})`);
        ok(r.exited && r.code === 0, `and the process exited (code=${r.code} signal=${r.signal})`);
        ok(ms < BOUND_MS, `promptly, not after the deadline it was holding (${ms}ms)`);
    }

    console.log(fail ? `  ${fail} failed` : '  [l3-exit] all passed');
    process.exit(fail ? 1 : 0);
})();
