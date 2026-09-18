const { TurboKV } = require('../src/turbokv');
const __native = require('../src/native');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

const c = TurboKV.open({ storage: 'bytes' });

// get / set
ok(c.set('k', 'v') === true, 'set returns true on success');
ok(c.get('k') === 'v', 'get returns the value');
ok(c.get('absent') === undefined, 'get returns undefined for a miss');

// has: pure probe, must not promote or count a hit
c.set('probeme', 'x');
const before = { l1: c.stats.l1Hits, l2: c.stats.l2Hits };
ok(c.has('probeme') === true && c.has('nope') === false, 'has answers correctly');
ok(c.stats.l1Hits === before.l1 && c.stats.l2Hits === before.l2, 'has does not count as a hit');

// has must distinguish a stored null from an absent key
c.set('nul', null);
ok(c.get('nul') === null && c.has('nul') === true, 'stored null: get null, has true');
ok(c.get('gone') === undefined && c.has('gone') === false, 'absent key: get undefined, has false');

// delete
ok(c.delete('k') === true, 'delete returns true when present');
ok(c.get('k') === undefined && c.has('k') === false, 'deleted key is gone from both tiers');
ok(c.delete('k') === false, 'delete returns false when absent');

// ttl, in both tiers
c.set('tmp', 'x', { ttlMs: 60 });
ok(c.has('tmp') === true, 'ttl key present before expiry');
const until = Date.now() + 1100; while (Date.now() < until);
ok(c.has('tmp') === false && c.get('tmp') === undefined, 'ttl key expired in L1 and L2');

// clearLocal vs clearAll
c.set('a', '1'); c.set('b', '2');
c.clearLocal();
ok(c.get('a') === '1', 'clearLocal drops L1 only; value still served from the arena');
c.clearAll();
ok(c.get('a') === undefined && c.get('b') === undefined, 'clearAll wipes the arena');
ok(c.set('c', '3') === true && c.get('c') === '3', 'cache is usable after clearAll');

// set() reports acceptance, not durability - and a worker must reject an
// oversized value locally rather than queue something the primary will drop
ok(c.set('huge', 'x'.repeat(80 * 1024 * 1024)) === false, 'oversized value rejected at the call site');
ok(/exceeds the/.test(c.lastError) && c.stats.rejectedSize > 0, 'size rejection is explained and counted');

// set never throws and always reports
ok(c.set('x', { a: 1 }) === false, 'unsupported value returns false');
ok(typeof c.lastError === 'string', 'lastError explains the rejection');
ok(c.stats.rejectedType > 0, 'rejection counted in stats');

// enumeration — decision 3 named this as a benefit of storing key text
c.clearAll();
for (let i = 0; i < 5; i++) c.set('e' + i, 'v');
ok([...c.keys()].sort().join(',') === 'e0,e1,e2,e3,e4', 'keys() enumerates the arena');
ok(c.size === 5, 'size reports live entries');
ok(typeof TurboKV.arenaStats().live === 'number', 'arenaStats() exposes arena counters');
ok([...c.keys({ limit: 2 })].length === 2, 'keys() honours limit');

// lifecycle
ok(typeof c.close === 'function', 'close() exists');
c.close();

// --- a failed create explains itself --------------------------------------
//
// The failure a user actually hits is a container whose /dev/shm is smaller than
// the arena -- exactly what the musl release job hit, and the reason the message
// carries a hint rather than just saying "failed".
{
    // A deterministic failure, available on every platform: an index that cannot
    // fit the arena is refused by the geometry check regardless of how the OS
    // backs shared memory. Sizing the arena to fail instead was platform
    // -dependent and got this test wrong twice -- Linux reserves with
    // posix_fallocate and Windows commits the section against the pagefile, so
    // both fail, while macOS maps lazily and happily returns a 1TB arena.
    let msg = null, created = false;
    try {
        TurboKV.createPrimary('/tcfail' + process.pid, 1 << 20, 1 << 16, {});
        created = true;
    } catch (e) { msg = e.message; }
    ok(!created, 'an index that cannot fit the arena is refused on every platform');
    ok(/arena create failed/.test(msg || ''),
       `and the failure names what failed (got ${JSON.stringify(msg)})`);

    // The /dev/shm hint is Linux-only -- it is the one platform that can report
    // how much shared memory is left, and the hint is what made the musl CI
    // failure diagnosable at a glance.
    if (process.platform === 'linux') {
        let hint = null;
        try { TurboKV.createPrimary('/tcshm' + process.pid, 1024 * (1 << 30), 1 << 16, {}); }
        catch (e) { hint = e.message; }
        ok(/\/dev\/shm holds .*free.*arena needs/.test(hint || ''),
           `an arena larger than /dev/shm names the constraint and the shortfall (got ${JSON.stringify(hint)})`);
        ok(/--shm-size=/.test(hint || ''), 'and the Docker flag that fixes it');
    }
}

// --- the public surface is exactly what index.d.ts declares ----------------
//
// Undeclared-but-reachable is an API you support whether you meant to or not --
// the same problem TurboKV.native() had (decision 45). This pins the surface so
// a new helper cannot drift onto it unnoticed.
{
    const DECLARED_STATICS = [
        'createPrimary', 'attachWorker', 'open', 'install', 'isCacheMessage', 'applyBatch',
        'arenaStats', 'submitStats', 'primaryAgeMs', 'autoSize',
        'defaultName', 'hasCompression', 'deepFreeze', 'assertFastCodec',
        'JSON_CODEC', 'V8_CODEC', 'drainSubmissions', 'heapGuardPace', 'L1', 'L2', 'L3',
    ];
    const DECLARED_INSTANCE = [
        'get', 'set', 'has', 'delete', 'clearLocal', 'clearAll',
        'keys', 'flush', 'close', 'stopGuard',
        'stats', 'lastError', 'liveHeapFraction', 'primaryDead', 'storage',
        'transport', 'size', 'l1Size',
    ];

    const statics = Object.getOwnPropertyNames(TurboKV)
        .filter(n => !['length', 'name', 'prototype'].includes(n));
    const extraStatic = statics.filter(n => !DECLARED_STATICS.includes(n));
    const missingStatic = DECLARED_STATICS.filter(n => !statics.includes(n));
    ok(extraStatic.length === 0, `no undeclared statics (found: ${extraStatic.join(', ')})`);
    ok(missingStatic.length === 0, `every declared static exists (missing: ${missingStatic.join(', ')})`);

    // Both: methods and getters live on the prototype, while stats, lastError,
    // storage and liveHeapFraction are assigned in the constructor and are own
    // properties of the instance. Checking only the prototype reported four
    // declared members as missing.
    const probe = TurboKV.createPrimary('/tcsurf' + process.pid, 8 << 20, 1 << 13, {});
    const proto = [
        ...Object.getOwnPropertyNames(TurboKV.prototype).filter(n => n !== 'constructor'),
        ...Object.keys(probe),
    ];
    __native.destroy();
    // __internalOnGc is reachable by necessity: gcNotify() is a module-scope
    // function declared above the class, so it cannot reach a #private.
    const extraInst = proto.filter(n => !DECLARED_INSTANCE.includes(n) && !n.startsWith('__internal'));
    const missingInst = DECLARED_INSTANCE.filter(n => !proto.includes(n));
    ok(extraInst.length === 0, `no undeclared instance members (found: ${extraInst.join(', ')})`);
    ok(missingInst.length === 0, `every declared instance member exists (missing: ${missingInst.join(', ')})`);
}

// --- the native surface carries nothing the JS layer no longer calls --------
//
// Dead native code is worse than dead JS: it is reachable from anyone who can
// require the addon, and it is not covered by any test (decision 47).
{
    const REMOVED_NATIVE = ['incr', 'cas'];
    const present = REMOVED_NATIVE.filter(n => typeof __native[n] === 'function');
    ok(present.length === 0, `removed native functions are gone (still present: ${present.join(', ')})`);
}

// The summary and exit MUST be last. They were at line 68 of 126, so every
// block appended after them -- the create-failure diagnostic and the public
// surface pin -- was dead code that never ran and could never fail.
console.log(fail ? `  ${fail} FAILURES` : '  all passed');
process.exit(fail ? 1 : 0);
