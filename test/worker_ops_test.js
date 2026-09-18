// Worker-side operations that the rest of the suite never reaches: the
// submission ring running out of room, and clearAll() reaching a worker's L1.
//
// Both have the same contract and it is worth stating once: when a write
// cannot reach L2, the value stays in the writing worker's L1 and the loss is
// COUNTED. Other workers then see a miss, never a wrong value. A silent drop
// here would be indistinguishable from success at the call site.
const cluster = require('cluster');
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');


// Wait for workers to actually EXIT, rather than guessing with a timer. Under
// coverage a worker flushes on SIGTERM, and a parent that exits first kills the
// flush -- which made measured coverage swing by 4.5 points between runs.
function reapThen(workers, done) {
    const list = [...workers];
    if (!list.length) return done();
    let left = list.length;
    const one = () => { if (--left === 0) done(); };
    // ASK them to leave, do not signal them. V8 writes coverage on a normal
    // exit; the SIGTERM handler is a backstop that has to win a race, and
    // losing it moved whole-suite coverage by more than a point between runs.
    const guard = setTimeout(() => { for (const wk of list) wk.kill('SIGTERM'); }, 3000);
    guard.unref && guard.unref();
    for (const wk of list) { wk.once('exit', one); try { wk.send({ t: 'bye' }); } catch { wk.kill('SIGTERM'); } }
}

const ARENA = '/tcwo_' + process.pid;

if (cluster.isPrimary && !process.env.TC_CHILD) {
    let fails = 0;
    const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fails++; };

    const c = TurboKV.createPrimary(ARENA, 32 << 20, 1 << 16,
        // A deliberately tiny ring: a modest burst must exhaust it, which is the
        // state the shed path exists for.
        { storage: 'bytes', transport: 'shm', submitRingBytes: 4096 });
    TurboKV.install(cluster);

    const w = cluster.fork({ TC_CHILD: '1', TC_ARENA: ARENA });
    w.on('message', (m) => {
        if (!m || !m.t) return;

        if (m.t === 'ring') {
            ok(m.shed > 0,
               `a full submission ring sheds the write rather than blocking (shed=${m.shed})`);
            ok(m.ownL1 === 'V',
               'a shed ring write still reads back from the writer\'s own L1');
            ok(m.tooBig === false && m.rejectedSize > 0,
               `a value too large for a ring RECORD is rejected, not shed (rejectedSize=${m.rejectedSize})`);
            ok(/submission-ring limit/.test(m.sizeError || ''),
               `the rejection names the limit and how to raise it (got ${JSON.stringify(m.sizeError)})`);
            ok(m.hadDoomed === true && m.doomedGone === true,
               'a worker delete goes through the ring and clears the local copy');
            // Drain whatever did fit, then tell the worker to move on.
            let guard = 0;
            while (TurboKV.drainSubmissions(8192) > 0 && ++guard < 256);
            c.set('sentinel', 'BEFORE-CLEAR');
            w.send({ t: 'clear-ready' });
            return;
        }

        if (m.t === 'seen') {
            // The worker has 'sentinel' in its L1. clearAll() on the primary must
            // reach it through the invalidation ring's clearAll sentinel.
            ok(m.before === 'BEFORE-CLEAR', 'the worker had the value in L1 before the clear');
            c.clearAll();
            setTimeout(() => w.send({ t: 'check-cleared' }), 150);
            return;
        }

        if (m.t === 'done') {
            ok(m.after === undefined,
               'clearAll() on the primary empties a WORKER\'s L1 (ring sentinel honoured)');
            reapThen(Object.values(cluster.workers), () => {
                native.destroy();
                console.log(fails ? `  ${fails} FAILURES` : '  all passed');
                process.exit(fails ? 1 : 0);
            });
        }
    });
    setTimeout(() => { console.log('  TIMEOUT'); process.exit(1); }, 30000);
} else {
    // --- worker
    const c = TurboKV.attachWorker(process.env.TC_ARENA, cluster.worker.id,
                                      { storage: 'bytes', transport: 'shm' });

    // A value can fit the ARENA and still be too big for a submission-ring
    // record. That is permanent, not backpressure, so it must be reported as a
    // rejection -- otherwise it becomes an endless stream of successful-looking
    // writes that never reach L2.
    const tooBig = c.set('huge', 'V'.repeat(1 << 14));
    const rejectedSize = c.stats.rejectedSize || 0;
    const sizeError = c.lastError;

    // A delete issued by a worker travels the same ring as a write.
    c.set('doomed', 'V');
    const hadDoomed = c.delete('doomed');

    for (let i = 0; i < 3000; i++) c.set('r' + i, 'V'.repeat(48));
    c.set('mine', 'V');
    process.send({ t: 'ring', shed: c.stats.writesShed || 0, ownL1: c.get('mine'),
                   tooBig, rejectedSize, sizeError, hadDoomed,
                   doomedGone: c.get('doomed') === undefined });

    process.on('message', (m) => {
        if (m && m.t === 'bye') { process.exit(0); return; }
        if (m && m.t === 'clear-ready') {
            const before = c.get('sentinel');          // pulls it into this worker's L1
            process.send({ t: 'seen', before });
        }
        if (m && m.t === 'check-cleared') {
            const after = c.get('sentinel');
            process.send({ t: 'done', after });
        }
    });
}
