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
// This has to be tested in a CHILD PROCESS, because the property under test is
// "the process exits", which nothing inside that process can observe. The
// parent's kill timer is the timeout guard: a regression fails an assertion
// here rather than wedging CI until the job-level limit kills it.
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

    console.log(fail ? `  ${fail} failed` : '  [l3-exit] all passed');
    process.exit(fail ? 1 : 0);
})();
