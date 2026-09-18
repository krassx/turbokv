'use strict';
// Two instances in ONE process. The primary used to skip ring records it wrote
// itself (writerId 0), which assumes a process holds a single cache. It does
// not: opening the cache from two modules is ordinary, and each instance keeps
// its own L1. The worker path abandoned the same shortcut already.
const { TurboKV } = require('../src/turbokv');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

const one = TurboKV.open({ storage: 'bytes' });
const two = new TurboKV({ storage: 'bytes' });

one.set('k', 'A');
ok(one.get('k') === 'A', 'the writer reads its own value');

two.set('k', 'B');
ok(two.get('k') === 'B', 'the second instance reads its own write');
ok(one.get('k') === 'B', 'the first instance sees the second instance\'s write');

two.delete('k');
ok(two.get('k') === undefined, 'the deleting instance sees the delete');
ok(one.get('k') === undefined, 'the other instance sees the delete too');

// A value only one instance ever touched must survive the other's writes.
one.set('mine', 'kept');
two.set('theirs', 'also kept');
ok(one.get('mine') === 'kept' && two.get('theirs') === 'also kept',
   'unrelated keys are untouched');

// clearAll() is a write too, and the same bug applies: the primary's own
// branch cleared itself and the shared arena, but never the L1 of any other
// in-process instance. A key the other instance had CACHED (an L1 hit,
// served without ever touching the now-empty arena) is the path that was
// broken.
one.set('cached-elsewhere', 'was-here');
ok(one.get('cached-elsewhere') === 'was-here', 'sanity: cached in the first instance\'s L1 before the clear');
two.clearAll();
ok(two.get('cached-elsewhere') === undefined, 'the clearing instance sees its own clear');
ok(one.get('cached-elsewhere') === undefined,
   'clearAll from another instance drops a key the first instance had cached in L1');

// A key the first instance never cached (only reachable through the shared
// arena) is NOT covered by a separate assertion here: native.clearAll()
// empties the arena unconditionally, on both sides of this fix, so an L1
// miss falling through to it was never broken and no assertion here can
// distinguish the two -- verified by fault injection, see task-5-report.md.

console.log(fail ? `  ${fail} failed` : '  [instances] all passed');
process.exit(fail ? 1 : 0);
