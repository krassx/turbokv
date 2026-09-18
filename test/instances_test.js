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

console.log(fail ? `  ${fail} failed` : '  [instances] all passed');
process.exit(fail ? 1 : 0);
