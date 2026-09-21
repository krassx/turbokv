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
//
// The comment stripping below has the same shape of limit, and the same
// answer. It handles a line that IS a comment (`^\s*//`) -- which is the
// vacuity that actually bit, a guard deleted while the prose above it still
// named the thing it checked for. A trailing `//` comment on a line of code,
// or a `/* ... */` block, still fools it. Chasing that generally means parsing
// JavaScript, which is not what this file is for; the behavioural test is the
// real guard and this one is the tripwire.
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

// --- EVERY MARK IS TAKEN AND RELEASED BY KIND ---------------------------
//
// A worker carries two marks: "I REMOVED this key and the primary has not
// applied it" and "I WROTE this key and it has not landed". They used to be
// ONE set, `#pendingDel`, taken for a delete AND for a `minLevel: 2` set --
// and every consumer then had to guess which it was looking at. The third
// adversarial pass found four defects, one per guess: a cap that read a set's
// mark as a pending delete and skipped itself, a wrapped-ring reconciliation
// that keeps a mark on exactly the condition that means a WRITE has landed, a
// self-mark released by any record at all, and a set recorded as a removal for
// the promotion guard.
//
// The kind now lives in the RECEIVER: `this.#pendingDel` or
// `this.#pendingWrite`, each a PendingMarks built with its kind. There is no
// way to take or release a mark without having chosen one -- unless a later
// edit reintroduces one, which is what this section is for. Same one-level
// textual scan, same known limits, same reason: the behaviours are pinned in
// review4_regression_test.js, and the behaviours were pinned each round
// before, while the next round found the next site.
//
// THE FOURTH PASS FOUND THE OTHER HALF OF THE SAME SHAPE, and found it here.
// This section only ever watched the MUTATORS -- mark, release, releaseUpTo --
// plus three consumers named one at a time. So `matchAt`, `has`, `clear` and
// `touch` were invisible: swapping `#pendingDel.matchAt` for
// `#pendingWrite.matchAt` inside #drain passed this guard outright, and it is
// precisely the kind confusion the split exists to prevent -- a WRITE's record
// would then be handed to #deletedAt and counted as a prevented resurrection.
// #drain and #promotionBlock, the two members where a kind confusion changes
// what a caller is TOLD, were not inspected at all. The ledger below watches
// every mention of either set, by member and by verb, and the pairings after
// it watch the two places where the kind decides the answer.
//
// WHAT IT STILL CANNOT SEE, so nobody mistakes it for a proof:
//   - It reads text, one level deep, exactly as the sections above do. A
//     consumer moved into a new helper shows up under the helper's name, which
//     the ledger reports as an unexpected entry -- loud, but it is the ledger
//     noticing a NEW NAME, not the guard understanding the move.
//   - It checks WHICH set a line names and, for the two pairings, what that
//     same line returns. It does not read the condition. `if
//     (!this.#pendingWrite.has(key)) return 'l3PromotionsBlockedSelf';` passes
//     every check here and is exactly backwards.
//   - A kind confusion that never names a set -- reaching a mark's `kind`
//     field, or holding an entry object -- is outside it entirely.
//   - The ledger is a TABLE, so every legitimate new mark site fails it once
//     and has to be added deliberately. That is the point, and it is also the
//     only thing keeping it honest: a ledger maintained by widening it until
//     it stops complaining is worth nothing.
// The behavioural halves are review4_regression_test.js and
// review5_regression_test.js. Neither kind of test is sufficient alone.
{
    const MARK_SETS = ['#pendingDel', '#pendingWrite'];
    const MUTATORS = ['.mark(', '.release(', '.releaseUpTo('];
    // The mark sets are built once each, and say which kind they are.
    const built = text.split('\n').filter(l => !/^\s*\/\//.test(l) && l.includes('new PendingMarks('));
    ok(built.length === 2, `there are exactly two mark sets (${built.length})`);
    ok(built.filter(l => l.includes("PendingMarks('delete')")).length === 1, 'one for removals');
    ok(built.filter(l => l.includes("PendingMarks('write')")).length === 1, 'one for writes');

    // Every mutation names one of them ON THE SAME LINE. A helper that took
    // the set as a parameter, or picked it from a flag, would land here.
    let member2 = '<module scope>';
    const mutations = [];
    const strayMut = [];
    const lines2 = text.split('\n');
    // Inside PendingMarks itself the receiver is `this`, and that class IS the
    // kind: it is constructed with one and never changes it. Everything from
    // its declaration to the cache class is therefore skipped.
    const classFrom = lines2.findIndex(l => /^class PendingMarks \{/.test(l));
    const classTo = lines2.findIndex(l => /^class TurboKV \{/.test(l));
    ok(classFrom >= 0 && classTo > classFrom, 'PendingMarks is declared above the cache');
    for (let i = 0; i < lines2.length; i++) {
        const m = MEMBER.exec(lines2[i]);
        if (m) member2 = m[1];
        if (i >= classFrom && i < classTo) continue;
        const code = lines2[i].replace(/^\s*\/\/.*$/, '');
        if (!MUTATORS.some(v => code.includes(v))) continue;
        mutations.push([member2, i + 1]);
        if (!MARK_SETS.some(set => code.includes(set + '.'))) strayMut.push([member2, i + 1, code.trim()]);
    }
    ok(mutations.length >= 6, `the scanner finds the mark mutations at all (${mutations.length})`);
    ok(strayMut.length === 0,
       strayMut.length === 0
           ? 'every mark is taken or released on a set that names its kind'
           : `marks mutated without naming a kind: ` +
             strayMut.map(([n, l, c]) => `${c} in ${n}() at line ${l}`).join('; '));
    // ...and BOTH kinds are actually mutated, or the check above passes for a
    // build that quietly went back to one set.
    for (const set of MARK_SETS) {
        const n = lines2.filter(l => !/^\s*\/\//.test(l) && MUTATORS.some(v => l.includes(v)) && l.includes(set + '.')).length;
        ok(n >= 2, `${set} is both taken and released (${n} sites)`);
    }

    // No aliasing: a local holding "whichever set" is how the kind stops being
    // visible at the call site even though the receiver names it.
    const aliases = lines2.filter(l => !/^\s*\/\//.test(l) && /(=|\()\s*this\.#pending(Del|Write)\s*[;,)]/.test(l));
    ok(aliases.length === 0,
       aliases.length === 0 ? 'no call site aliases a mark set into a variable'
                            : `a mark set is aliased: ${aliases.map(l => l.trim()).join('; ')}`);

    // THE LEDGER: every member that touches a mark set, and which verbs it
    // uses on which kind. The mutator scan above says a line names A kind; this
    // says WHICH, per member, for every verb rather than three named ones -- so
    // swapping the kind inside a consumer moves an entry and fails here even
    // though the line still names a set.
    //
    // An exact table, and deliberately so. A new mark site is a change to how
    // this worker accounts for what it owes, which is where four consecutive
    // rounds of review each found a defect; it should cost a line here and a
    // moment's thought about which kind it is.
    const LEDGER = {
        '__unsafeMarkState': 'del.keys del.size write.keys write.size',
        '#drain':            'del.matchAt del.release del.size write.matchAt write.release write.size',
        '#deletedHere':      'del.has',
        '#unappliedHere':    'del.has write.has',
        '#reconcileRemovals': 'del.has del.keys del.release del.size del.touch',
        '#reconcileWrites':  'write.keys write.release write.size write.submittedAt write.touch',
        '#promotionBlock':   'del.has write.has',
        'set':               'del.release write.mark write.release',
        'delete':            'del.mark del.release',
        'clearLocal':        'del.clear write.clear',
        '#releaseBatchMarks': 'del.releaseUpTo write.releaseUpTo',
    };
    {
        let member3 = '<module scope>';
        const actual = new Map();
        for (let i = 0; i < lines2.length; i++) {
            const m = MEMBER.exec(lines2[i]);
            if (m) member3 = m[1];
            if (i >= classFrom && i < classTo) continue;      // inside PendingMarks itself
            const code = lines2[i].replace(/^\s*\/\/.*$/, '');
            for (const [set, tag] of [['#pendingDel', 'del'], ['#pendingWrite', 'write']]) {
                const re = new RegExp(set.replace('#', '#') + '\\.(\\w+)', 'g');
                let g;
                while ((g = re.exec(code)) !== null) {
                    if (!actual.has(member3)) actual.set(member3, new Set());
                    actual.get(member3).add(tag + '.' + g[1]);
                }
            }
        }
        // Non-vacuity: the extractor finds the sites at all, and more than one
        // member's worth. A regex that matched nothing would pass every diff
        // below by reporting an empty ledger against an empty expectation.
        ok(actual.size >= 8, `the ledger extractor finds the mark consumers (${actual.size} members)`);
        const names = new Set([...actual.keys(), ...Object.keys(LEDGER)]);
        const wrong = [];
        for (const n of names) {
            const got = actual.has(n) ? [...actual.get(n)].sort().join(' ') : '<absent>';
            const want = LEDGER[n] === undefined ? '<absent>' : LEDGER[n];
            if (got !== want) wrong.push(`${n}: expected [${want}], found [${got}]`);
        }
        ok(wrong.length === 0,
           wrong.length === 0
               ? `every mark site uses the kind and the verb the ledger records (${actual.size} members)`
               : `the mark ledger no longer matches the source:\n      ${wrong.join('\n      ')}`);
    }

    // THE CONSUMERS ASK THE QUESTION THEY MEAN.
    //
    //   #deletedHere   -- removals only. Its callers refuse to ask L3 at all,
    //                     which is right for a key we removed and wrong for
    //                     one we have just written.
    //   #unappliedHere -- the ONE question that wants either, said out loud.
    //   the cap        -- neither: the primary's compare answers it, one hop
    //                     later, where it can be answered synchronously.
    const bodyCode = (name) => {
        const b = bodyOf(name);
        return b === null ? null : b.split('\n').map(l => l.replace(/^\s*\/\/.*$/, '')).join('\n');
    };
    {
        const b = bodyCode('#deletedHere');
        ok(b !== null, '#deletedHere() exists');
        ok(b !== null && b.includes('#pendingDel.has('), '#deletedHere() asks the removal mark');
        ok(b !== null && !b.includes('#pendingWrite'), '#deletedHere() does NOT ask the write mark');
    }
    {
        const b = bodyCode('#unappliedHere');
        ok(b !== null, '#unappliedHere() exists');
        ok(b !== null && b.includes('#pendingDel.has(') && b.includes('#pendingWrite.has('),
           '#unappliedHere() asks both, which is what it is for');
        const callers = lines2.filter(l => !/^\s*\/\//.test(l) && l.includes('#unappliedHere(') && !l.includes('#unappliedHere(key) {'));
        ok(callers.length >= 2, `and it has the read-path callers (${callers.length})`);
    }
    {
        const b = bodyCode('#capL2AfterL3Failure');
        ok(b !== null, '#capL2AfterL3Failure() exists');
        ok(b !== null && !MARK_SETS.some(set => b.includes(set)),
           'the cap consults no mark: the primary compares the arena instead');
    }

    // WHERE THE KIND DECIDES WHAT A CALLER IS TOLD. Two places, and in both of
    // them the ledger above is satisfied by the WRONG answer: the line still
    // names a set, it just names the other one, or does the other thing with
    // what it got.
    {
        // #drain retires a record against both sets. Only the REMOVAL's match
        // may be handed to #deletedAt -- that is the promotion guard's record
        // of "you deleted this key", and a landed WRITE put there made an
        // in-flight getAsync answer undefined and counted the set as
        // l3DeletedWhileReading, a counter named for the opposite event.
        const b = bodyCode('#drain');
        ok(b !== null, '#drain() exists');
        const dl = (b || '').split('\n');
        const dAt = dl.findIndex(l => l.includes('#pendingDel.matchAt('));
        const wAt = dl.findIndex(l => l.includes('#pendingWrite.matchAt('));
        const nAt = dl.findIndex(l => l.includes('#noteDeleted('));
        ok(dAt >= 0 && wAt >= 0, `#drain() asks both sets for the record it is retiring (${dAt}, ${wAt})`);
        ok(nAt > dAt && nAt < wAt,
           `and only the REMOVAL's match reaches #deletedAt (del@${dAt}, note@${nAt}, write@${wAt})`);
        // The two reconciliations ask DIFFERENT sources, which is the whole
        // reason there are two: a removal's landing is a fact about the arena,
        // a write's is a fact about the submission ring's consumer index, and
        // each read the other way round is a defect this campaign has already
        // shipped once.
        const rr = bodyCode('#reconcileRemovals'), rw = bodyCode('#reconcileWrites');
        ok(rr !== null && rw !== null, 'both reconciliations exist');
        ok(rr !== null && /native\.has\(/.test(rr) && !/submitTail|#submitTailNow/.test(rr),
           '#reconcileRemovals() asks the arena, and only the arena');
        ok(rw !== null && /#submitTailNow\(/.test(rw) && !/native\.has\(/.test(rw),
           '#reconcileWrites() asks the submission ring, and not the arena');

        // THE ONE COMPARISON THE WHOLE MECHANISM RESTS ON, and the one thing
        // above cannot see. Inverting it -- `tail < sub` to `tail > sub` --
        // leaves every ledger entry and both pairings intact, and turns the
        // fix inside out: marks for writes the primary HAS applied are kept
        // (the key goes unreadable) and marks for writes it has NOT are
        // released (the superseded value is served). The behavioural test
        // catches it; so should the choke point, because this is the line a
        // refactor is most likely to "tidy".
        const rwl = (rw || '').split('\n');
        const keepAt = rwl.findIndex(l => l.includes('#pendingWrite.touch('));
        const relAt = rwl.findIndex(l => l.includes('#pendingWrite.release('));
        ok(keepAt > 0 && relAt > keepAt,
           `#reconcileWrites() keeps in a guarded branch and releases after it (keep@${keepAt}, release@${relAt})`);
        const cond = keepAt > 0 ? rwl[keepAt - 1] : '';
        ok(/tail\s*<\s*sub/.test(cond),
           `a mark is KEPT only while the consumer index is SHORT of its record (${cond.trim() || '<none>'})`);
        ok(/sub\s*>=\s*0/.test(cond) && /tail\s*>=\s*0/.test(cond),
           'and both -1 sentinels gate that branch, so an unknowable position RELEASES');
        // Belt and braces: the opposite comparison must not appear anywhere in
        // the body, so it cannot be smuggled in on a second line.
        ok(!/tail\s*>=?\s*sub/.test(rw || ''),
           'and the opposite comparison appears nowhere in the body');
    }
    {
        // #promotionBlock's two mark reasons are NOT interchangeable:
        // l3DeletedWhileReading changes the ANSWER (the caller is told
        // undefined, and a prevented resurrection is counted), while
        // l3PromotionsBlockedSelf blocks only the placement and still hands
        // back what L3 returned. Reverting the second to the first passed
        // every suite before review5 pinned it; this is the source-level half.
        const b = bodyCode('#promotionBlock');
        ok(b !== null, '#promotionBlock() exists');
        const pl = (b || '').split('\n');
        const dLine = pl.find(l => l.includes('#pendingDel.has('));
        const wLine = pl.find(l => l.includes('#pendingWrite.has('));
        ok(dLine !== undefined && /'l3DeletedWhileReading'/.test(dLine),
           `the REMOVAL mark answers l3DeletedWhileReading (${(dLine || '<absent>').trim()})`);
        ok(wLine !== undefined && /'l3PromotionsBlockedSelf'/.test(wLine),
           `and the WRITE mark answers l3PromotionsBlockedSelf (${(wLine || '<absent>').trim()})`);
        // ...and they are two different strings, or the pair above passes for
        // a build that folded the reasons back together.
        ok(dLine !== undefined && wLine !== undefined &&
           !/'l3PromotionsBlockedSelf'/.test(dLine) && !/'l3DeletedWhileReading'/.test(wLine),
           'and neither line carries the other reason');
    }

    // A BATCH THAT IS DROPPED RELEASES WHAT IT CARRIED, on every route out.
    // Only the shed branch did, so a synchronous send throw -- one bigint
    // under JSON serialization does it for a whole batch -- left a delete
    // marked forever. There are four drop routes in flush(); each must pair
    // with a release, and a fifth added later fails here.
    {
        const b = bodyOf('flush');
        ok(b !== null, 'flush() exists');
        const fl = (b || '').split('\n');
        const releases = fl.filter(l => !/^\s*\/\//.test(l) && l.includes('#releaseBatchMarks('));
        ok(releases.length === 4, `flush() releases marks on every route out (${releases.length} of 4)`);
        const drops = [];
        for (let i = 0; i < fl.length; i++) {
            if (/^\s*\/\//.test(fl[i])) continue;
            if (!/flushDropped = \(/.test(fl[i]) && !/writesShed = \(this\.stats\.writesShed \|\| 0\) \+ shed/.test(fl[i])) continue;
            const near = fl.slice(Math.max(0, i - 14), i + 8).join('\n');
            drops.push([i + 1, near.includes('#releaseBatchMarks(')]);
        }
        ok(drops.length === 4, `the scanner found every drop site (${drops.length})`);
        const unpaired = drops.filter(([, paired]) => !paired);
        ok(unpaired.length === 0,
           unpaired.length === 0 ? 'and each one is paired with a release'
                                 : `a batch is dropped without releasing its marks, near flush() line ${unpaired.map(d => d[0]).join(', ')}`);
    }
}

console.log(fail ? `  ${fail} FAILURES` : '  [write-sites] all passed');
process.exit(fail ? 1 : 0);
