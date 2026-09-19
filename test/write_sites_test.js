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
    '#retimeArena', '#retimeRing',
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
    const at = text.indexOf(`\n    ${name}(`) >= 0
        ? text.indexOf(`\n    ${name}(`)
        : text.indexOf(`\n    static ${name}(`);
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

console.log(fail ? `  ${fail} FAILURES` : '  [write-sites] all passed');
process.exit(fail ? 1 : 0);
