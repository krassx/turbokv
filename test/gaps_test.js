// Covers the gaps closed after the adversarial review: binary values, TTL
// sweeping, and primary heartbeat.
const { TurboKV } = require('../src/turbokv');
const native = require('../src/native');
const __native = native;
let fail = 0, n = 0;
const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const mk = o => TurboKV.createPrimary('/tcgap' + process.pid + '_' + (n++), 32 << 20, 1 << 16,
    { storage: 'bytes', l1MaxBytes: 32 * 1024, maintenance: false, ...o });

// --- binary values (decision 4)
{
    const c = mk({});
    const cases = [['Buffer', Buffer.from([1, 2, 3, 255])], ['Uint8Array', new Uint8Array([9, 8, 7])],
                   ['Uint16Array', new Uint16Array([1000, 2000])], ['Float64Array', new Float64Array([1.5, -2.5])],
                   ['ArrayBuffer', new Uint8Array([4, 5, 6]).buffer]];
    for (const [name, v] of cases) {
        const want = Buffer.from(ArrayBuffer.isView(v)
            ? new Uint8Array(v.buffer, v.byteOffset, v.byteLength) : new Uint8Array(v));
        ok(c.set('b:' + name, v), `${name} accepted`);
        ok(Buffer.isBuffer(c.get('b:' + name)) && c.get('b:' + name).equals(want), `${name} L1 round-trip`);
        c.clearLocal();
        ok(c.get('b:' + name).equals(want), `${name} L2 round-trip`);
    }
    c.set('mut', Buffer.from([1, 2, 3]));
    const a = c.get('mut'); a[0] = 99;
    ok(c.get('mut')[0] === 1, 'binary results are copied per read, so mutation cannot corrupt L1');
    c.clearLocal();
    const b2 = c.get('mut'); b2[0] = 88;
    ok(c.get('mut')[0] === 1, 'the same holds on the L2 refill path');
    __native.destroy();
}

// --- TTL sweeping reclaims eagerly instead of waiting for the tail
{
    const c = mk({ maintenance: true, maintenanceMs: 40, sweepFullPassMs: 300 });
    for (let i = 0; i < 500; i++) c.set('e' + i, 'v', { ttlMs: 25 });
    c.set('keep', 'forever');
    const before = native.stats().live;
    setTimeout(() => {
        const after = native.stats().live;
        ok(before >= 500 && after <= 2, `expired entries reclaimed (${before} -> ${after})`);
        ok(c.get('keep') === 'forever', 'sweep leaves non-expiring entries alone');
        __native.destroy();
        stage2();
    }, 800);
}

function stage2() {
    // --- heartbeat
    const c = mk({ maintenance: true, maintenanceMs: 40 });
    ok(TurboKV.primaryAgeMs() >= 0 && TurboKV.primaryAgeMs() < 1000, 'primary stamps a heartbeat');
    __native.destroy();

    console.log(fail ? `  ${fail} FAILURES` : '  all passed');
    process.exit(fail ? 1 : 0);
}
