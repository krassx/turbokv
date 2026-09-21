'use strict';
// The package entry points, exercised as a consumer reaches them.
//
// Coverage found these at 0%: every other test requires `../src/turbokv`
// directly, so `index.js`, `index.mjs` and the `exports` map that routes to them
// were never loaded by the suite. That is the surface consumers actually touch,
// and it is where the CJS/ESM split can break without any source file changing --
// a wrong `exports` condition, a re-export dropped from index.mjs, or a default
// export that is the module namespace instead of the class.
const assert = require('assert');
const path = require('path');
const { pathToFileURL } = require('url');

let fail = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };

const ROOT = path.join(__dirname, '..');
const EXPECTED = ['TurboKV', 'Cache', 'MSG'];

(async () => {
    // --- CommonJS: require('@krassx/turbokv')
    const cjs = require(path.join(ROOT, 'index.js'));
    ok(typeof cjs.TurboKV === 'function', 'cjs: TurboKV is exported');
    ok(cjs.Cache === cjs.TurboKV, 'cjs: Cache is the same class as TurboKV');
    ok(typeof cjs.MSG === 'string', 'cjs: MSG is exported');
    ok(typeof cjs.TurboKV.createPrimary === 'function',
       'cjs: the class carries its statics');
    ok(cjs.TurboKV.native === undefined,
       'cjs: the addon is NOT reachable from the public surface');

    // --- ESM: import '@krassx/turbokv'
    const esm = await import(pathToFileURL(path.join(ROOT, 'index.mjs')).href);
    for (const name of EXPECTED) {
        ok(esm[name] !== undefined, `esm: ${name} is a named export`);
    }
    ok(esm.default === esm.TurboKV,
       'esm: the DEFAULT export is the class, not the module namespace');
    ok(esm.TurboKV === cjs.TurboKV,
       'esm and cjs resolve to the same class (one implementation, two wrappers)');
    ok(esm.Cache === esm.TurboKV, 'esm: Cache aliases TurboKV');

    // --- the two entry points must agree on their surface
    const cjsNames = Object.keys(cjs).sort();
    const esmNames = Object.keys(esm).filter(n => n !== 'default').sort();
    ok(JSON.stringify(cjsNames) === JSON.stringify(esmNames),
       `both entry points export the same names (cjs=${cjsNames} esm=${esmNames})`);
    ok(JSON.stringify(cjsNames) === JSON.stringify([...EXPECTED].sort()),
       `the public surface is exactly ${[...EXPECTED].sort().join(', ')}`);

    // --- and the class actually works when reached this way
    const c = cjs.TurboKV.createPrimary('/tcep' + process.pid, 8 << 20, 1 << 13,
                                           { storage: 'direct' });
    c.set('k', { a: 1 });
    const got = c.get('k');
    ok(got != null && got.a === 1, 'a cache created through the entry point works');
    require(path.join(ROOT, 'src', 'native')).destroy();

    // --- THE TARBALL MUST CONTAIN WHAT THE ENTRY POINT REQUIRES.
    //
    // `files` is a hand-maintained list, and the suite always resolves modules
    // from the working tree, so a source file added without a `files` entry is
    // invisible here and fails at require time on the consumer's machine --
    // `Cannot find module`, for a package that installed cleanly. src/l3/ was
    // added by the L3 work and never listed; src/fastpath.js had been missing
    // for longer. Walk what is actually shipped instead of trusting the list.
    {
        const fs = require('fs');
        const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
        const shipped = (rel) => pkg.files.some(f =>
            f.endsWith('/') ? rel.startsWith(f) : f === rel);
        const seen = new Set();
        const missing = [];
        const walk = (rel) => {
            if (seen.has(rel)) return;
            seen.add(rel);
            if (!shipped(rel)) { missing.push(rel); return; }
            const src = fs.readFileSync(path.join(ROOT, rel), 'utf8');
            for (const m of src.matchAll(/require\(['"](\.[^'"]+)['"]\)|from\s+['"](\.[^'"]+)['"]/g)) {
                const spec = m[1] || m[2];
                const base = path.posix.join(path.posix.dirname(rel), spec);
                for (const cand of [base, base + '.js', base + '.mjs']) {
                    if (fs.existsSync(path.join(ROOT, cand)) &&
                        fs.statSync(path.join(ROOT, cand)).isFile()) { walk(cand); break; }
                }
            }
        };
        walk('index.js');
        walk('index.mjs');
        ok(missing.length === 0,
           `every module the entry points require is in package.json "files" (missing: ${missing.join(', ') || 'none'})`);
    }

    console.log(fail ? `  ${fail} FAILURES` : '  all passed');
    process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
