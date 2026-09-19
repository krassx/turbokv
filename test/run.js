'use strict';
// Runs the JS suite. `npm test` needs one entry point, and listing the files in
// three places (here, CI, and by hand) is how a test stops being run without
// anyone noticing - so CI calls this too.
const { execFileSync } = require('child_process');
const path = require('path');

// Every test process releases its shared memory on exit. See test/_cleanup.js:
// segments outlive their creator by design, so without this a full run strands
// one arena and one ring per test file and needs gigabytes of /dev/shm.
// NODE_OPTIONS rather than a require in each file, so a new test cannot forget,
// and it reaches the workers tests fork as well.
const CLEANUP = path.join(__dirname, '_cleanup.js');
process.env.NODE_OPTIONS = `${process.env.NODE_OPTIONS ? process.env.NODE_OPTIONS + ' ' : ''}--require ${JSON.stringify(CLEANUP)}`;

const SUITE = [
    'test.js', 'api_test.js', 'instances_test.js', 'codec_test.js', 'prim_test.js', 'json_fastpath_test.js',
    'v8codec_test.js', 'storage_modes_test.js', 'cluster_api_test.js',
    'review_regression_test.js', 'gaps_test.js', 'typeflow_test.js', 'typematrix_test.js',
    'perf_regression_test.js', 'guard_test.js', 'recovery_test.js',
    'review2_regression_test.js', 'review3_regression_test.js', 'entrypoints_test.js', 'write_sites_test.js',
    'backpressure_test.js', 'worker_ops_test.js', 'shm_leak_test.js', 'doorbell_loss_test.js', 'minlevel_test.js',
    'l3_adapter_test.js', 'l3_queue_test.js', 'l3_api_test.js', 'l3_guard_test.js', 'l3_removal_test.js', 'l3_value_test.js', 'l3_joiner_test.js', 'l3_cap_test.js', 'l3_clear_test.js', 'l3_clear_leak_test.js', 'install_retain_test.js', 'l3_exit_test.js',
];
// Same file, both transports: the shared-memory path is the default and the IPC
// path is the fallback, and a regression in either is a regression.
const MATRIX = [
    ['transport_regression_test.js', { TC_T: 'shm' }], ['transport_regression_test.js', { TC_T: 'ipc' }],
    ['worker_lifecycle_test.js', { TC_T: 'shm' }], ['worker_lifecycle_test.js', { TC_T: 'ipc' }],
];

let failed = [];
const run = (file, env) => {
    const label = file + (env && env.TC_T ? ` [${env.TC_T}]` : '');
    process.stdout.write(`--- ${label}\n`);
    try {
        // A per-test timeout, because without one a hang wedges CI until the
        // job-level limit kills it with no indication of which test hung.
        execFileSync(process.execPath, [path.join(__dirname, file)],
            { stdio: 'inherit', env: { ...process.env, ...env }, timeout: 180000 });
    } catch (e) {
        failed.push(label + (e && e.signal === 'SIGTERM' ? ' (TIMED OUT)' : ''));
    }
};
for (const f of SUITE) run(f, null);
for (const [f, env] of MATRIX) run(f, env);

if (failed.length) { console.error(`\nFAILED: ${failed.join(', ')}`); process.exit(1); }
console.log('\nall suites passed');
