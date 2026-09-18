'use strict';
// The adapter is something USERS implement, so its contract has to be checked
// at the boundary rather than discovered as a TypeError three layers down when
// a cache miss finally reaches L3.
const { assertAdapter } = require('../src/l3/adapter');
const { makeFake } = require('./l3_fake');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };
const throws = (fn, re, m) => {
    let e = null; try { fn(); } catch (x) { e = x; }
    ok(e instanceof TypeError && re.test(e.message), `${m} (got ${e && e.message})`);
};

const { adapter } = makeFake();
ok(assertAdapter(adapter) === adapter, 'a complete adapter is returned unchanged');

throws(() => assertAdapter(null), /must be an object/, 'null is rejected');
throws(() => assertAdapter(42), /must be an object/, 'a number is rejected');
for (const m of ['get', 'set', 'delete', 'clear']) {
    const partial = { ...adapter }; delete partial[m];
    throws(() => assertAdapter(partial), new RegExp(`missing.*${m}`), `a missing ${m} is rejected`);
    const wrong = { ...adapter, [m]: 'not a function' };
    throws(() => assertAdapter(wrong), new RegExp(m), `a non-function ${m} is rejected`);
}
// Optional members may be absent, but must be functions when present.
for (const m of ['has', 'subscribe', 'close']) {
    const without = { ...adapter }; delete without[m];
    ok(assertAdapter(without) === without, `${m} is optional`);
    throws(() => assertAdapter({ ...adapter, [m]: 7 }), new RegExp(m), `a non-function ${m} is rejected`);
}

console.log(fail ? `  ${fail} failed` : '  [l3-adapter] all passed');
process.exit(fail ? 1 : 0);
