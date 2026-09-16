// The primary must converge even if a doorbell is lost.
//
// A worker pushes to its submission ring and then rings RING_MSG so the primary
// drains it. That doorbell is the ONLY thing that makes the primary drain: its
// own get() returns early (see #drain, which does nothing for id 0), and the
// maintenance tick historically only stamped a heartbeat and swept expiries.
//
// The doorbell is swallowed in two places -- `if (process.connected)` and a
// catch around process.send -- so losing one left the pushed records sitting in
// the ring. That is not merely stale invalidation: the DATA never reaches L2,
// so every other worker misses a key that was written successfully. Workers
// self-heal because they drain on every operation; the primary had no such
// property until the maintenance tick started draining too.
//
// Here the worker drops its own doorbells, so the only thing that can deliver
// the write is the backstop.
const cluster = require('cluster');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');

const ARENA = '/tcdoor_' + process.pid;
const MAINT_MS = 300;

if (cluster.isPrimary && !process.env.TC_CHILD) {
    let fail = 0;
    const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };

    const c = TurboKV.createPrimary(ARENA, 32 << 20, 1 << 16,
        { storage: 'bytes', transport: 'shm', submitRingBytes: 1 << 16,
          maintenanceMs: MAINT_MS });
    TurboKV.install(cluster);

    const w = cluster.fork({ TC_CHILD: '1', TC_ARENA: ARENA });
    w.on('message', (m) => {
        if (!m || m.t !== 'wrote') return;

        // Nothing has rung. Without the backstop the record stays in the ring
        // for as long as the worker stays quiet.
        const immediately = native.get('doorbell-test');

        setTimeout(() => {
            const afterMaintenance = native.get('doorbell-test');
            ok(afterMaintenance === 'LANDED',
               `a write whose doorbell was lost still reaches L2 (got ${JSON.stringify(afterMaintenance)})`);
            ok(c.get('doorbell-test') === 'LANDED',
               "and the primary's own L1 serves it, not a stale miss");
            console.log(`  (before the maintenance tick it was ${JSON.stringify(immediately)})`);

            const ws = Object.values(cluster.workers);
            let left = ws.length;
            const done = () => { if (--left === 0) {
                native.destroy();
                console.log(fail ? `  ${fail} FAILURES` : '  all passed');
                process.exit(fail ? 1 : 0);
            } };
            const guard = setTimeout(() => { for (const x of ws) x.kill('SIGTERM'); }, 3000);
            guard.unref && guard.unref();
            for (const x of ws) { x.once('exit', done); try { x.send({ t: 'bye' }); } catch { x.kill('SIGTERM'); } }
        }, MAINT_MS * 4);
    });
    setTimeout(() => { console.log('  TIMEOUT'); process.exit(1); }, 20000);
} else {
    // --- worker: drop every doorbell, keep everything else
    const realSend = process.send.bind(process);
    let dropped = 0;
    process.send = (msg, ...rest) => {
        if (msg && msg.t === 'tcr') { dropped++; return true; }   // swallow the doorbell
        return realSend(msg, ...rest);
    };

    const c = TurboKV.attachWorker(process.env.TC_ARENA, cluster.worker.id,
                                   { storage: 'bytes', transport: 'shm' });
    c.set('doorbell-test', 'LANDED');
    process.on('message', (m) => { if (m && m.t === 'bye') process.exit(0); });
    setTimeout(() => process.send({ t: 'wrote', dropped }), 50);
}
