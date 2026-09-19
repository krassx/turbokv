'use strict';
// EVERY WRITE GOES THROUGH A PUBLISH HELPER.
//
// "Cancel an outstanding l3FailTtlMs cap if and only if a write actually
// lands" is one sentence, and it used to be enforced by hand at nine call
// sites: the primary's set and del, a worker's submitSet and submitDel, the
// minLevel-3 eviction, the L3 promotion, and three IPC-fallback outbox pushes.
// Four consecutive rounds of review each found one more of them on the wrong
// side of a branch -- a cancel before a size check, a cancel before a shed
// test, a path that had lost its cancel entirely, a promotion that cancelled
// and then shed. Every one of those was a resurrection or a value left in the
// shared arena with no expiry.
//
// The cancel now lives inside the helpers instead, so a call site cannot
// forget it and a rejected or shed write cannot trigger it. THIS TEST IS WHAT
// KEEPS THAT TRUE: it attributes every raw call to the arena, the submission
// ring and the outbox to its enclosing method, and fails if one appears
// anywhere but the publish family. A write site added later either goes
// through a helper -- and is correct by construction -- or fails here with the
// name of the method that skipped it.
//
// A source-level test, because the property is about the SHAPE of the code
// rather than about a behaviour: the behaviours were tested each round, and
// each round the next uncovered site was a different behaviour.
//
// ITS KNOWN LIMIT, so nobody mistakes it for exhaustive: this is a ONE-LEVEL
// TEXTUAL SCAN. It attributes a call to its enclosing member, so a violation
// moved one level down -- `#fillFromL3` calling a new `#helper()` that calls
// `native.submitSet` -- is caught by the stray check (the helper is not in
// ALLOWED) but the L3 section below, which reads one method body at a time,
// would not see it inside `#fillFromL3`. It is a tripwire against the edit
// that reintroduces the defect the obvious way, not a proof. The BEHAVIOURAL
// half is `l3_guard_test.js`, which forks a real worker, promotes a real L3
// value and asserts the arena never saw it -- with the primary's own
// promotion as a non-vacuity control, so "nothing reached L2" cannot pass by
// promotion being broken. Neither test is sufficient alone; both are cheap.
const fs = require('fs');
const path = require('path');

let fail = 0;
const ok = (c, m) => { console.log(`  ${c ? 'ok  ' : 'FAIL'}  ${m}`); if (!c) fail++; };

const SRC = path.join(__dirname, '..', 'src', 'turbokv.js');

// The raw primitives. Anything that mutates the arena, or queues something
// that will.
const PRIMITIVES = [
    'native.set(', 'native.del(', 'native.clearAll(',
    'native.submitSet(', 'native.submitDel(',
    'this.#outbox.push(',
];

// The publish family, and the cap's own pair. The second exists because the
// cap re-publishes a value this process already published, with a shorter
// deadline: it supersedes nothing, so it must NOT cancel -- doing so would
// drop the entry the read guard still needs. The names carry the difference,
// and this list is where that distinction is enforced.
const ALLOWED = new Set([
    '#publishArenaSet', '#publishArenaDel', '#publishArenaClear',
    '#publishRingSet', '#publishRingDel',
    '#publishOutbox', '#publishOutboxOp',
    '#retimeArena', '#retimeOutbox',
]);

// A class member declaration at the top level of the class body: four spaces,
// an optional `static`, an optional accessor keyword, a name, an open paren.
const MEMBER = /^ {4}(?:static\s+)?(?:get\s+|set\s+)?([#A-Za-z_$][\w$]*)\s*\(/;

const lines = fs.readFileSync(SRC, 'utf8').split('\n');
let member = '<module scope>';
const found = [];               // [method, line number, the primitive]
for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const m = MEMBER.exec(line);
    if (m) member = m[1];
    // Comments describe these by name constantly; only code counts.
    const code = line.replace(/^\s*\/\/.*$/, '');
    for (const p of PRIMITIVES) if (code.includes(p)) found.push([member, i + 1, p]);
}

// Non-vacuity first: a scanner that finds nothing passes everything.
ok(found.length >= 9,
   `the scanner found the write primitives at all (${found.length} call sites)`);
ok(new Set(found.map(f => f[2])).size === PRIMITIVES.length,
   `and every primitive is represented (${new Set(found.map(f => f[2])).size} of ${PRIMITIVES.length})`);
// ...and that it can tell members apart, rather than attributing everything to
// one name.
ok(new Set(found.map(f => f[0])).size > 1,
   `and attributes them to more than one method (${new Set(found.map(f => f[0])).size})`);

const strays = found.filter(([name]) => !ALLOWED.has(name));
ok(strays.length === 0,
   strays.length === 0
       ? 'every raw write is inside a publish helper'
       : `raw writes outside the publish family: ` +
         strays.map(([n, l, p]) => `${p} in ${n}() at line ${l}`).join('; '));

// Each helper must actually be a helper: the publish family cancels, the
// retime pair deliberately does not. A helper that stopped cancelling would
// pass the check above while reopening every defect it exists to close.
const text = fs.readFileSync(SRC, 'utf8');
const bodyOf = (name) => {
    let at = -1;
    for (const prefix of ['', 'static ', 'async ', 'static async ']) {
        at = text.indexOf(`\n    ${prefix}${name}(`);
        if (at >= 0) break;
    }
    if (at < 0) return null;
    // To the next member declaration at class-body indentation.
    const rest = text.slice(at + 1);
    const end = rest.search(/\n {4}(?:static\s+)?(?:\/\/|[#A-Za-z_$])[\w$]*\s*\(/);
    return end < 0 ? rest : rest.slice(0, end);
};
for (const name of ALLOWED) {
    const body = bodyOf(name);
    const cancels = body !== null && /#cancelCaps\(|#cancelAllCaps\(/.test(body);
    if (name.startsWith('#publish')) ok(cancels, `${name}() cancels`);
    else ok(!cancels, `${name}() deliberately does not cancel`);
}

// A helper that is listed must exist. bodyOf() returns null for a name that is
// gone, and `!cancels` is then vacuously true -- so a retime member deleted in
// a refactor would go on "passing" its own check forever.
for (const name of ALLOWED) ok(bodyOf(name) !== null, `${name}() exists`);

// --- L3-DERIVED DATA REACHES THE ARENA THROUGH EXACTLY ONE PROCESS -------
//
// Only the primary may write L2 with a value that came from L3. A worker that
// reads through to L3 fills its own L1 and stops there.
//
// Two rounds of review each closed one instance of a worker writing L3-derived
// data into the shared arena through the submission ring -- a promotion landing
// over the worker's own acked write, and a failure cap re-writing an old value
// a hop later -- and each time the next round found another door into the same
// room. The rule closes the room. This section is what keeps it closed: the
// behaviours are tested elsewhere, but the behaviours were tested each round
// too, and each round the next uncovered site was a different behaviour.
//
// The write primitives above plus the publish and retime families: anything
// that puts bytes into the arena, now or a hop later.
const L3_FORBIDDEN = PRIMITIVES.concat([...ALLOWED].map(n => n + '('));
// The ONE route L3-derived data may take, and the methods that handle it.
const L3_ROUTE = '#publishL3Derived(';
const L3_METHODS = ['#fillFromL3', '#fetchFromL3', '#acceptFromL3'];

for (const m of L3_METHODS) {
    const body = bodyOf(m);
    ok(body !== null, `${m}() exists`);
    if (body === null) continue;
    const code = body.split('\n').map(l => l.replace(/^\s*\/\/.*$/, '')).join('\n');
    const strayed = L3_FORBIDDEN.filter(p => code.includes(p));
    ok(strayed.length === 0,
       strayed.length === 0
           ? `${m}() writes the arena through nothing but ${L3_ROUTE.slice(0, -1)}()`
           : `${m}() reaches the arena directly: ${strayed.join(', ')}`);
}

// Non-vacuity: the route is actually taken, so the check above is not passing
// because the promotion stopped writing L2 altogether.
{
    const body = bodyOf('#fillFromL3');
    ok(body !== null && body.includes(L3_ROUTE),
       `#fillFromL3() does promote, through ${L3_ROUTE.slice(0, -1)}()`);
}

// And the route itself refuses anyone but the primary. Without this the
// section above would pass for a helper that had quietly become a passthrough.
{
    const raw = bodyOf('#publishL3Derived');
    ok(raw !== null, '#publishL3Derived() exists');
    // COMMENTS STRIPPED. These names are discussed in the prose right above
    // the code that uses them, so a check reading the whole body passes on
    // the explanation of a guard that has been deleted -- which is exactly
    // what happened when this was first written.
    const body = raw === null ? '' : raw.split('\n').map(l => l.replace(/^\s*\/\/.*$/, '')).join('\n');
    ok(/#id\s*!==\s*0/.test(body),
       '#publishL3Derived() refuses a caller whose id is not 0');
    // ...and `attached: false` yields #id === 0 in any process, so the id
    // alone is not the primary test.
    ok(/isPrimaryProcess/.test(body),
       '#publishL3Derived() also requires this to BE the primary process');
    // It is reached from ONE place. A second caller is not wrong in itself,
    // but it is exactly how "the organising rule" decays back into a list of
    // special cases, so it has to be a deliberate edit here.
    const calls = text.split('\n')
        .filter(l => !/^\s*\/\//.test(l) && l.includes(L3_ROUTE) && !l.includes('static #publishL3Derived'));
    ok(calls.length === 1, `#publishL3Derived() has exactly one call site (${calls.length})`);
}

console.log(fail ? `  ${fail} FAILURES` : '  [write-sites] all passed');
process.exit(fail ? 1 : 0);
