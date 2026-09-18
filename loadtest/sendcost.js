'use strict';
// NOTE: this harness measures the CLUSTER IPC path specifically, so it pins
// transport:'ipc'. Shared memory is the default now; without the pin these
// scripts silently measured the wrong transport and reported nonsense.
// How much SYNCHRONOUS event-loop time does one process.send() cost? This is the
// jitter question, separate from throughput: whatever it costs, the worker's
// event loop is frozen for that long, and every other thing that worker is doing
// -- app IPC, timers, request handling -- waits.
const cluster = require('cluster');
const { TurboKV } = require('../src/turbokv');
const ARENA = '/tcsend';
const SER = process.env.SER === 'advanced' ? 'advanced' : 'json';
const N = Number(process.env.N || 400000);
const VAL = 'v'.repeat(180);
function slotsFor(b) { return 1 << Math.max(12, Math.min(22, Math.ceil(Math.log2(Math.max(4096, b / 256))))); }
const pct = (a, p) => a.length ? a[Math.min(a.length - 1, Math.floor(a.length * p / 100))] : 0;

if (cluster.isPrimary) {
    const L2 = 192 * 1024 * 1024;
    const cache = TurboKV.createPrimary(ARENA, L2, slotsFor(L2), { storage: 'bytes', transport: 'ipc' });
    cluster.setupPrimary({ serialization: SER, exec: __filename });
    TurboKV.install(cluster);
    const w = cluster.fork({ TC_ARENA: ARENA });
    w.on('message', (m) => {
        if (!m || m.t !== 'r') return;
        console.log(`  serialization=${SER}`);
        console.log(`  process.send() calls   ${m.n}, mean batch ${(m.meanBytes/1024).toFixed(0)}KB`);
        console.log(`  synchronous cost       p50 ${m.p50.toFixed(2)}ms  p99 ${m.p99.toFixed(2)}ms  max ${m.max.toFixed(2)}ms`);
        console.log(`  total loop time frozen ${m.total.toFixed(0)}ms in sends`);
        w.kill(); cache.close(); process.exit(0);
    });
} else {
    const cache = TurboKV.attachWorker(ARENA, 1, { storage: 'bytes', l1MaxBytes: 2 << 20, transport: 'ipc' });
    // Wrap process.send to time the synchronous portion of each call.
    const realSend = process.send.bind(process);
    const durs = []; let bytes = 0;
    process.send = function (msg, ...rest) {
        if (msg && msg.b) {
            const t0 = process.hrtime.bigint();
            const r = realSend(msg, ...rest);
            durs.push(Number(process.hrtime.bigint() - t0) / 1e6);
            bytes += msg.b.length * 62;         // rough: 4 slots per write, ~250B each
            return r;
        }
        return realSend(msg, ...rest);
    };
    let i = 0;
    const CHUNK = 2000;
    (function step() {
        const end = Math.min(i + CHUNK, N);
        for (; i < end; i++) cache.set('s:' + i, VAL);
        if (i < N) return setImmediate(step);
        cache.flush();
        setTimeout(() => {
            const s = durs.slice().sort((a, b) => a - b);
            realSend({ t: 'r', n: s.length, meanBytes: s.length ? bytes / s.length : 0,
                p50: pct(s, 50), p99: pct(s, 99), max: s[s.length - 1] || 0,
                total: s.reduce((a, b) => a + b, 0) });
        }, 2000);
    })();
}
