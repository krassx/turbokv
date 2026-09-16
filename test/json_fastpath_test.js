// Guards the properties that keep JSON on V8's fast path.
// Node 26 made ASCII JSON.stringify ~34% faster but did NOT speed up the slow
// paths, so falling off one now costs relatively more than it used to.
const native = require('../src/native');
const __native = native;
const { TurboKV } = require('../src/turbokv');
const { callArgCounts } = require('../src/fastpath');
const fs = require('fs');
let fail = 0; const ok = (c, m) => { if (!c) { console.log('  FAIL:', m); fail++; } };

native.create('/tcfp' + process.pid, 32 << 20, 1 << 16, 2);
native.setCompressMin(1 << 30);

// 1. ASCII values must come back as ONE-BYTE strings. primBytes reports
//    16+len for one-byte and 16+2*len for two-byte, so it doubles as a probe.
const ascii = JSON.stringify({ id: 'abc', items: [1, 2, 3] });
native.set('a', ascii, 0);
const back = native.get('a');
ok(back === ascii, 'ascii value round-trips');
ok(native.primBytes(back) === ((16 + back.length + 7) & ~7), 'value returned as a ONE-BYTE string');

// 2. Non-ASCII must round-trip exactly rather than being mangled by latin1.
for (const s of ['héllo', '中文', '🚀', 'mixed ünï']) {
    native.set('u', s, 0);
    ok(native.get('u') === s, `non-ASCII round-trips: ${s}`);
}

// 3. NOTHING in this repo may pass a second argument to JSON.stringify/parse.
//    A replacer costs 3.51x on Node 26 and 2-space indent 2.11x, because Node 26
//    sped up the fast path without speeding up the slow ones. Balanced-paren
//    scanning, not a regex, so nested calls are not miscounted.
const path = require('path');
const root = path.join(__dirname, '..');
function walk(dir, acc = []) {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        if (e.name === 'node_modules' || e.name === 'build' || e.name === '.git') continue;
        const p = path.join(dir, e.name);
        if (e.isDirectory()) walk(p, acc);
        else if (e.name.endsWith('.js')) acc.push(p);
    }
    return acc;
}
// Comments discuss the slow paths on purpose; strip them before scanning.
function stripComments(src) {
    return src.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
}
let scanned = 0, offenders = 0, exempt = 0;
for (const file of walk(root)) {
    const raw = fs.readFileSync(file, 'utf8');
    // Files that measure the slow paths deliberately opt out by declaring it.
    if (raw.includes('json-fastpath-lint: allow')) { exempt++; continue; }
    const text = stripComments(raw);
    scanned++;
    for (const name of ['JSON.stringify', 'JSON.parse']) {
        for (const call of callArgCounts(text, name)) {
            if (call.args > 1) {
                offenders++;
                console.log(`  FAIL: ${path.relative(root, file)} -> ${call.text.slice(0, 70)}`);
            }
        }
    }
}
ok(offenders === 0, `${offenders} JSON call(s) off the fast path across ${scanned} files`);
console.log(`  scanned ${scanned} JS files (${exempt} opted out) for slow-path JSON calls`);

// 4. A caller-supplied codec is checked at construction, since a source lint
//    cannot see into the caller's closure.
const bad = [
    ['indent',            { encode: v => JSON.stringify(v, null, 2), decode: JSON.parse }],
    ['identity replacer', { encode: v => JSON.stringify(v, (k, x) => x), decode: JSON.parse }],
    ['key allowlist',     { encode: v => JSON.stringify(v, ['a']), decode: JSON.parse }],
    ['reviver on decode', { encode: v => JSON.stringify(v), decode: s => JSON.parse(s, (k, x) => x) }]
];
for (const [name, c] of bad) {
    let threw = false;
    try { TurboKV.assertFastCodec(c); } catch { threw = true; }
    ok(threw, `caller codec rejected: ${name}`);
}
let fine = true;
try { TurboKV.assertFastCodec({ encode: JSON.stringify, decode: JSON.parse }); } catch { fine = false; }
ok(fine, 'plain JSON codec accepted');

// 5. A cached substring must be flattened, or it retains its parent.
const cache = TurboKV.createPrimary('/tcfp2' + process.pid, 16 << 20, 1 << 16, { values: 'bytes' });
const parent = new Array(50000).fill('abcdefgh').join('');
cache.set('slice', parent.substring(0, 500));
ok(cache.get('slice').length === 500, 'substring cached correctly');
__native.destroy();

console.log(fail ? `  ${fail} FAILURES` : '  all passed');
process.exit(fail ? 1 : 0);
