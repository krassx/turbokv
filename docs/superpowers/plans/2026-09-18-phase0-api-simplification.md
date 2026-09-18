# Phase 0: API Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Shrink turbokv's API to `get`/`set`/`has`/`delete`/`clear` by removing `incr`, `cas` and namespaces, and fix a stale-read bug those removals uncovered.

**Architecture:** Three independent removals plus one bug fix, in an order that never leaves the tree broken. `incr`/`cas` come out of JS first, then out of the native addon. Namespaces come out of JS next (the native layer still accepts a namespace argument, which JS passes as `0`), then out of the native layer and the arena header. The stale-read fix is last and is the only behaviour change.

**Tech Stack:** Node-API addon (C++), plain JS (no framework), `node-gyp` build, a hand-rolled test runner (`test/run.js`), `c8` for JS coverage, `gcov` for native coverage.

**Spec:** `docs/superpowers/specs/2026-09-18-l3-adapter-design.md` (§2 Phase 0, §11 Bugs, §12.6 Why namespaces went)

## Global Constraints

- **Node floor is 18.** `package.json` declares `"engines": { "node": ">=18.0.0" }` and `binding.cc` line 1 declares `#define NAPI_VERSION 8`. Do not raise either.
- **No new runtime dependencies.** A checkout must build with only a compiler and Node.
- **Every new test must be verified to fail with the fix reverted.** This repo has found twelve tests that could not fail. A test that passes before the change is not evidence. Where a step says "verify it fails", that is a required step, not a formality.
- **Coverage floors are in `.c8rc.json`: lines 95, branches 82, functions 91.** The suite must still clear them at the end of the phase. Nothing may be exempted; there is no `c8 ignore` in the tree.
- **The public surface is pinned** by the block at the end of `test/api_test.js`. Any member added to or removed from the class must be reflected in `DECLARED_STATICS` / `DECLARED_INSTANCE` in the same commit.
- **`DESIGN.md` carries a numbered decision table.** The highest decision in the tree is 61. Use 62, 63, 64 in the order the tasks below assign them.
- **Commands:** build with `npm run build`, run one suite file with `node test/<file>.js`, run everything with `npm test`, coverage with `npm run coverage` and `npm run coverage:native`.
- **Commit messages** end with the two-line Claude Code attribution used by the rest of the tree.

---

### Task 1: Remove `incr` and `cas` from the JS API

Neither can be atomic across L1, L2, L3 and many boxes. `cas` already refuses in a worker for exactly this reason ("a queued CAS whose outcome the caller never learns is not a CAS"), and L3 makes that true in every process. Callers who need an atomic counter have a Valkey client.

**Files:**
- Modify: `src/turbokv.js` (the `incr` and `cas` methods; the `'i'` branch of `applyBatch`)
- Modify: `index.d.ts` (the `incr` and `cas` declarations)
- Modify: `test/api_test.js:117` (surface pin)
- Modify: `test/gaps_test.js:55-66`, `test/review2_regression_test.js:21-32`, `test/transport_regression_test.js:74`
- Modify: `DESIGN.md` (decision 62, open item 1)

**Interfaces:**
- Consumes: nothing.
- Produces: `TurboKV` no longer has `incr(key, by, opts)` or `cas(key, expected, next)`. `applyBatch` no longer accepts the `'i'` operation. Task 2 removes the native functions these were the only callers of.

- [ ] **Step 1: Write the failing test** — take `incr` and `cas` out of the surface pin in `test/api_test.js`, so the pin now asserts they are absent.

```js
    const DECLARED_INSTANCE = [
        'get', 'set', 'has', 'delete', 'clearLocal', 'clearAll',
        'clearNamespace', 'keys', 'flush', 'close', 'stopGuard',
        'stats', 'lastError', 'liveHeapFraction', 'primaryDead', 'storage',
        'transport', 'size', 'l1Size',
    ];
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node test/api_test.js`
Expected: FAIL — `no undeclared instance members (found: incr, cas)`

- [ ] **Step 3: Remove the two methods**

In `src/turbokv.js`, delete the whole `incr(key, by = 1, opts) { … }` method (it starts at the comment block "Atomic increment. The primary is the sole writer…") and the whole `cas(key, expected, next) { … }` method that follows it, up to but not including the `clearNamespace()` comment block.

In the same file, delete the `'i'` branch from `applyBatch`:

```js
            else if (op === 'i') { native.incr(key, b[i + 2], msg.id, b[i + 3], b[i + 4]); TurboKV.#localDrop(key); }
```

In `index.d.ts`, delete the `incr(...)` and `cas(...)` declarations and their doc comments.

- [ ] **Step 4: Run the pin to make sure it passes**

Run: `node test/api_test.js`
Expected: PASS

- [ ] **Step 5: Remove the tests that exercised them**

- `test/gaps_test.js`: delete the block at lines 55-66 (the six `incr`/`cas` assertions and the `ttlMs` line that follows them). If the surrounding `{ … }` scope has no assertions left, delete the scope and its comment too.
- `test/review2_regression_test.js`: delete lines 21, 23, 31 and 32. This file's point is that a codec mode refuses these operations; if a loop body ends up empty, delete the loop and its comment.
- `test/transport_regression_test.js`: delete line 74 (`c.incr('cnt', 1);`) and any later assertion about `'cnt'`.

Find any survivors: `grep -n "\.incr(\|\.cas(\|incrQueued" test/*.js src/*.js index.d.ts` must print nothing.

- [ ] **Step 6: Run the full suite**

Run: `npm test`
Expected: every suite passes.

- [ ] **Step 7: Record the decision**

Add to the decision table in `DESIGN.md`:

```
| 62 | **`incr` and `cas` are removed rather than extended to L3** | keep them local-only; give them async-only variants | Neither can be atomic across L1, L2, L3 and many boxes without a global ordering this design does not have, and a synchronous return value cannot carry a remote atomic result. The codebase already drew this line: `cas` refused in a worker because "a queued CAS whose outcome the caller never learns is not a CAS", and L3 makes that true in every process. A caller who needs an atomic counter already has a Valkey client and `INCR`. Reverses decision 1, whose remaining wart -- `incr`'s return type differing by process role -- disappears with the method. |
```

Update open item 1 in §13 to say the methods were removed, and strike the "remaining wart" sentence.

- [ ] **Step 8: Commit**

```bash
git add src/turbokv.js index.d.ts test/api_test.js test/gaps_test.js \
        test/review2_regression_test.js test/transport_regression_test.js DESIGN.md
git commit -m "$(cat <<'EOF'
Remove incr and cas from the public API

Neither can be atomic across L1, L2, L3 and many boxes. cas already
refused in a worker on exactly that reasoning; L3 makes it true
everywhere. A caller who needs an atomic counter has a Valkey client.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 2: Remove `incr` and `cas` from the native layer

Task 1 removed the only callers. The native functions are now dead code, and the repo's rule is that dead code goes (decision 47 deleted the compaction machinery on the same grounds).

**Files:**
- Modify: `src/binding.cc` (the `Incr` and `Cas` functions; their entries in the `FN(...)` registration list at line 1156)
- Modify: `src/store_ops.h` (`storeIncr` at line 536, `storeCas` at line 579)
- Modify: `test/api_test.js` (add a native-surface pin)
- Modify: `DESIGN.md` (decision 62, extended)

**Interfaces:**
- Consumes: Task 1's removal of every JS caller.
- Produces: `require('src/native')` no longer exposes `incr` or `cas`. No later task depends on them.

- [ ] **Step 1: Write the failing test** — append to `test/api_test.js`, immediately after the existing public-surface block, a pin on the native surface:

```js
// --- the native surface carries nothing the JS layer no longer calls --------
//
// Dead native code is worse than dead JS: it is reachable from anyone who can
// require the addon, and it is not covered by any test (decision 47).
{
    const REMOVED_NATIVE = ['incr', 'cas'];
    const present = REMOVED_NATIVE.filter(n => typeof __native[n] === 'function');
    ok(present.length === 0, `removed native functions are gone (still present: ${present.join(', ')})`);
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node test/api_test.js`
Expected: FAIL — `removed native functions are gone (still present: incr, cas)`

- [ ] **Step 3: Remove the native functions**

In `src/binding.cc`, delete the `static napi_value Incr(...)` and `static napi_value Cas(...)` function bodies, and remove `FN("incr", Incr)` and `FN("cas", Cas)` from the registration list at line 1156.

In `src/store_ops.h`, delete `storeIncr` and `storeCas` in full.

- [ ] **Step 4: Rebuild and run the pin**

Run: `npm run build && node test/api_test.js`
Expected: the build succeeds with no warnings about unused functions, and the suite passes.

- [ ] **Step 5: Run the full suite**

Run: `npm test`
Expected: every suite passes.

- [ ] **Step 6: Commit**

```bash
git add src/binding.cc src/store_ops.h test/api_test.js
git commit -m "$(cat <<'EOF'
Delete the native incr and cas now that nothing calls them

Reachable native code with no caller and no test is the same problem
decision 47 deleted the compaction machinery for.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 3: Remove the namespace API from the JS layer

A namespace is a key prefix plus a one-byte tag that charges a byte quota. The prefix is something callers can do themselves; the quota is the only capability they cannot reproduce, it is L2-only, and with L3 attached an evicted entry costs a round trip rather than the data. The native layer is left alone in this task: JS passes `0` where it used to pass a namespace id, so the tree stays working.

**Files:**
- Modify: `src/turbokv.js` (`#ns`, `#nsId`, the constructor's namespace block, `clearNamespace`, `namespaceStats`, `size`, `keys`, and the six `this.#ns + key` sites)
- Modify: `index.d.ts` (`NamespaceOptions`, `NamespaceStat`, the `namespace` option, `clearNamespace`, `namespaceStats`)
- Delete: `test/namespace_test.js`
- Modify: `test/run.js:18` (drop it from `SUITE`)
- Modify: `test/api_test.js`, `test/review_regression_test.js`, `test/worker_ops_test.js`
- Modify: `DESIGN.md` (decision 63; §13 items 2 and 16)

**Interfaces:**
- Consumes: nothing from Tasks 1-2.
- Produces: `TurboKV` no longer accepts a `namespace` option and has neither `clearNamespace()` nor `namespaceStats()`. Keys are passed through unchanged. `size` reads `native.stats().live`. `keys()` enumerates the whole arena. Task 4 removes the native namespace arguments this task leaves as `0`.

- [ ] **Step 1: Write the failing test** — update the surface pin in `test/api_test.js` so both members are expected to be gone, and stop opening the cache with a namespace.

Line 5 becomes:

```js
const c = TurboKV.open({ storage: 'bytes' });
```

In the pin block, `DECLARED_STATICS` drops `'namespaceStats'` and `DECLARED_INSTANCE` drops `'clearNamespace'`:

```js
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
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node test/api_test.js`
Expected: FAIL — `no undeclared statics (found: namespaceStats)` and `no undeclared instance members (found: clearNamespace)`

- [ ] **Step 3: Remove the namespace machinery from `src/turbokv.js`**

- Delete the field declarations `#ns = '';` and `#nsId = 0;`.
- Delete the constructor's namespace block: the `const nsOpt = …` line through the `if (this.#nsId < 0) throw …` statement, and the comment above them.
- Delete each `key = this.#ns + key;` line. Four remain — in `get`, `set`, `has` and `delete`; Task 1 removed the other two along with `incr` and `cas`. Confirm with `grep -n "#ns" src/turbokv.js`, which must print nothing when you are done.
- Delete `clearNamespace()` and `static namespaceStats()`.
- Replace the `size` getter's body with:

```js
    get size() {
        const st = native.stats();
        return st ? st.live : 0;
    }
```

- In `keys()`, pass `0` where `this.#nsId` was passed to `native.scanKeys`.
- At the five remaining `native.set` / `native.submitSet` / `native.submitDel` / `this.#outbox.push` sites, pass `0` in place of `this.#nsId`. Delete the `'n'` branch of `applyBatch` and the `clearNamespace` case it served.

In `index.d.ts`, delete `NamespaceOptions`, `NamespaceStat`, the `namespace?:` option, `clearNamespace()` and `namespaceStats()`.

- [ ] **Step 4: Run the pin to make sure it passes**

Run: `node test/api_test.js`
Expected: PASS. If `keys() enumerates this namespace` now fails, update its message to `keys() enumerates the arena` — the assertion itself still holds.

- [ ] **Step 5: Remove the namespace tests**

```bash
git rm test/namespace_test.js
```

In `test/run.js`, remove `'namespace_test.js',` from `SUITE`.

In `test/api_test.js`, delete the "namespace isolation" block at lines 28-31.

In `test/review_regression_test.js`, delete line 66 (`namespaceStats` before an arena), the over-long-name test at lines 70-77, and the namespace-id bounds test at lines 114-119. That last one guarded `nsBytes[16]` overruns; the array is gone in Task 4, so the guard has nothing left to protect.

In `test/worker_ops_test.js`, delete the `clearNamespace` assertions at lines 78 and 118 and the file-header comment at line 3 that names it.

Check: `grep -rn "namespace\|Namespace" src/*.js index.d.ts test/*.js` should only match the unrelated ES-module wording in `test/entrypoints_test.js`.

- [ ] **Step 6: Run the full suite**

Run: `npm test`
Expected: every suite passes.

- [ ] **Step 7: Record the decision**

Add to `DESIGN.md`:

```
| 63 | **Namespaces are removed** | keep them; keep only the quota as an eviction class | A namespace was a key prefix plus a one-byte tag charging a byte quota. The prefix is something callers do themselves in one line. The quota is the only capability they cannot reproduce -- measured in §9 at 0/1000 cold survivors without it and 942/1000 with it -- but it is L2-only and cannot exist in L3, where Valkey evicts across the whole database, so namespaces would promise in one tier what the next tier breaks. With L3 attached, losing an entry from L2 costs a round trip rather than the data. Removal also deletes a verified aliasing bug (namespace `users` key `42` and default-namespace key `users:42` were one entry, because identity is the full string and names were never checked for `:`), the 15-namespace limit, open item 16, and a namespace clear that wiped every other namespace's L1. If a workload ever needs eviction isolation, it returns as a per-write class without namespaces coming back as key identity. |
```

Mark §13 items 2 and 16 resolved by removal.

- [ ] **Step 8: Commit**

```bash
git add -A src/turbokv.js index.d.ts test DESIGN.md
git commit -m "$(cat <<'EOF'
Remove namespaces from the JS layer

A namespace was a key prefix plus a quota. Callers can prefix keys
themselves; the quota is L2-only and cannot exist in L3, so it would
promise in one tier what the next breaks. Removal also deletes the
verified aliasing bug between a namespace and a default-namespace key
of the same full string.

The native layer still takes a namespace argument, which JS now passes
as 0; the next commit removes it.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 4: Remove namespaces from the native layer and the arena header

The JS layer now passes `0` everywhere. This removes the parameter, the header table, the quota branch in eviction, and the namespace id in submission-ring records, and bumps both layout versions so a process built before this change cannot attach to an arena built after it.

**Files:**
- Modify: `src/store.h` (`TC_LAYOUT`, `NS_MAX`, `NS_NAMELEN`, the four header arrays, `nsCount`, `Entry.ns`)
- Modify: `src/store_ops.h` (`nsResolve`, the quota branch in eviction, `storeClearNamespace`, the `nsBytes` updates, the `ns` parameters)
- Modify: `src/binding.cc` (`NsResolve`, `ClearNamespace`, `NsStats` and their `FN(...)` entries; the `ns` arguments of `Set`, `Del`, `ScanKeys`, `SubmitSet`, `SubmitDel`)
- Modify: `src/submit.h` (`TCS_LAYOUT`, the `ns` field of the record header)
- Modify: `src/turbokv.js` (drop the `0` arguments this task makes unnecessary)
- Modify: `test/api_test.js` (extend the native-surface pin)
- Modify: `DESIGN.md` (decision 63, extended)

**Interfaces:**
- Consumes: Task 3's JS-side removal.
- Produces: native signatures become `set(key, value, writerId, ttlMs)`, `del(key, writerId)`, `scanKeys(cursorSlot, max)`, `submitSet(key, value, ttlMs)`, `submitDel(key)`. `nsResolve`, `clearNamespace` and `nsStats` no longer exist. `TC_LAYOUT` is 6 and `TCS_LAYOUT` is 2.

- [ ] **Step 1: Write the failing test** — extend the native-surface pin added in Task 2:

```js
    const REMOVED_NATIVE = ['incr', 'cas', 'nsResolve', 'clearNamespace', 'nsStats'];
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node test/api_test.js`
Expected: FAIL — `removed native functions are gone (still present: nsResolve, clearNamespace, nsStats)`

- [ ] **Step 3: Remove the header state**

In `src/store.h`:
- Bump `TC_LAYOUT` to `6` and extend its trailing comment with `; 6: namespace table removed`.
- Delete `#define NS_MAX 16` and `#define NS_NAMELEN 24`.
- Delete the four header arrays `nsName`, `nsBytes`, `nsQuota`, `nsProtected`, `nsDropped`, and the `nsCount` field.
- Delete `uint8_t ns;` from `Entry`. Leave the rest of the struct's field order alone; the byte it frees is padding either way.

In `src/submit.h`, bump `TCS_LAYOUT` to `2` and delete `uint16_t ns;` from the record header.

- [ ] **Step 4: Remove the logic that used it**

In `src/store_ops.h`:
- Delete `nsResolve` and `storeClearNamespace` in full.
- In the eviction path, replace the quota branch with the plain CLOCK decision it falls back to:

```c
    bool protect = liveHere && s.hints[slot].load(std::memory_order_relaxed);
```

  and delete every `byQuota` reference, including the two `h->nsProtected[e->ns]++;` lines and their comments.
- Delete every `h->nsBytes[...]` update (there are four, in the eviction and insert paths) and the `e->ns = ns;` assignment.
- Drop the `ns` parameter from the functions that took one.

In `src/binding.cc`, delete `NsResolve`, `ClearNamespace` and `NsStats` along with their `FN(...)` entries, and drop the namespace argument from `Set`, `Del`, `ScanKeys`, `SubmitSet` and `SubmitDel`, adjusting each `ARG(n)` count.

In `src/turbokv.js`, drop the now-extra trailing `0` arguments at the `native.set`, `native.submitSet`, `native.submitDel` and `native.scanKeys` call sites, and from the `#outbox.push` tuples and the `applyBatch` loop that reads them (the stride drops from 5 to 4 — update both the `i += 5` and every `b[i + n]` index).

- [ ] **Step 5: Rebuild and run the suite**

Run: `npm run build && npm test`
Expected: the build succeeds and every suite passes. A stale arena from an older layout is refused rather than misread, which is what the `TC_LAYOUT` bump buys; if a test fails to attach, remove the stale segment (`test/_cleanup.js` does this on exit) and re-run.

- [ ] **Step 6: Confirm both layout constants moved**

Run: `grep -n "TC_LAYOUT = \|TCS_LAYOUT = " src/store.h src/submit.h`
Expected: `TC_LAYOUT = 6` and `TCS_LAYOUT = 2`. A mismatched layout is refused by the header check at `src/store.h:258`, which `test/guard_test.js` already exercises — that is what makes the bump load-bearing rather than cosmetic, so it must not be skipped just because nothing in this tree reads an old arena.

- [ ] **Step 7: Commit**

```bash
git add src/store.h src/store_ops.h src/binding.cc src/submit.h src/turbokv.js test/api_test.js DESIGN.md
git commit -m "$(cat <<'EOF'
Remove namespaces from the arena and the submission ring

Deletes the header table, the quota branch in eviction, the namespace
id in ring records and the ns parameter from every native call.
TC_LAYOUT 5 -> 6 and TCS_LAYOUT 1 -> 2, so a process built before this
cannot attach to an arena built after it.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 5: Fix the primary's stale L1 across instances

Two `TurboKV` instances in one process serve each other stale values, including deleted ones. The primary skips ring records it wrote itself (`writerId === 0`), which assumes one instance per process. The worker path already abandoned that shortcut, and its comment says why: *"Own records are NOT skipped. The old 'our own write, L1 is already correct' shortcut was false whenever L1 had been refilled from L2 between queuing and apply."* The primary kept it.

**Files:**
- Create: `test/instances_test.js`
- Modify: `test/run.js` (add it to `SUITE`)
- Modify: `src/turbokv.js` (`set` and `delete`, primary branches)
- Modify: `DESIGN.md` (decision 64; §13)

**Interfaces:**
- Consumes: Tasks 1-4 (no namespaces, so the test opens plain caches).
- Produces: no API change. A `set` or `delete` on the primary drops the key from every other instance's L1 in the same process.

- [ ] **Step 1: Write the failing test** — create `test/instances_test.js`:

```js
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
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `node test/instances_test.js`
Expected: FAIL on exactly two assertions — `the first instance sees the second instance's write` and `the other instance sees the delete too`. Both report the stale value `A`: the first instance keeps serving it after the second instance writes `B`, and keeps serving it after the second instance deletes the key. The deleting instance's own view is correct, so its two assertions pass.

- [ ] **Step 3: Fix the primary's `set`**

In `src/turbokv.js`, the primary branch of `set` currently is:

```js
        if (this.#id === 0) {
            const ok = native.set(key, enc, 0, ttlMs) === true;
            if (!ok) { this.stats.rejectedSize++; this.lastError = 'value does not fit the arena'; this.#l1Drop(key); }
            return ok;
        }
```

Make it drop the key from every other instance in this process, the way `applyBatch` already pairs `native.set` with `#localDrop`:

```js
        if (this.#id === 0) {
            const ok = native.set(key, enc, 0, ttlMs) === true;
            if (!ok) { this.stats.rejectedSize++; this.lastError = 'value does not fit the arena'; this.#l1Drop(key); }
            // Our own ring record is skipped on the primary, so nothing else
            // invalidates the copies other instances in THIS process hold.
            else TurboKV.#dropOthers(key, this);
            return ok;
        }
```

- [ ] **Step 4: Fix the primary's `delete` and add the helper**

The primary branch of `delete` becomes:

```js
        if (this.#id === 0) {
            const had = native.del(key, 0);
            this.#l1Drop(key);
            TurboKV.#dropOthers(key, this);
            return had;
        }
```

Add the helper next to the existing `#localDrop`:

```js
    // Every instance keeps its own L1, and the primary skips the ring records it
    // wrote itself, so a write through one instance leaves the others holding
    // the old value. `instances` is normally a set of one, so this costs a
    // branch per write in the common case.
    static #dropOthers(fullKey, self) {
        if (instances.size < 2) return;
        for (const c of instances) if (c !== self) c.#l1Drop(fullKey);
    }
```

- [ ] **Step 5: Run the test to make sure it passes**

Run: `node test/instances_test.js`
Expected: PASS

- [ ] **Step 6: Verify the test can fail** — revert just the two `#dropOthers` call sites, re-run, confirm the three assertions fail again, then restore them.

Run: `node test/instances_test.js`
Expected: FAIL while reverted, PASS when restored. Do not skip this: twelve tests in this repo have been found unable to fail.

- [ ] **Step 7: Pin the worker path, which is already correct**

Append to `test/instances_test.js` a note rather than a fork: the worker path is covered by `test/worker_ops_test.js`, and its correctness comes from the ring drain not skipping own records (`src/turbokv.js`, the "Own records are NOT skipped" comment). Add this assertion so a future change that reintroduces the shortcut in the worker drain is caught here too:

```js
// The worker path stays correct for the same reason it was fixed once: its ring
// drain does not skip its own records. Guard the comment with a real check.
const drain = require('fs').readFileSync(require.resolve('../src/turbokv'), 'utf8');
ok(/Own records are NOT skipped/.test(drain),
   'the worker drain still declines to skip its own ring records');
```

- [ ] **Step 8: Register the test**

In `test/run.js`, add `'instances_test.js',` to `SUITE`, next to `'api_test.js'`.

- [ ] **Step 9: Run the full suite**

Run: `npm test`
Expected: every suite passes, including the new file.

- [ ] **Step 10: Record the decision**

```
| 64 | **A write on the primary drops the key from every other instance in the process** | leave it; make instances share one L1 | Two instances in one process served each other stale values, including deleted ones: `one.set('k','A'); two.set('k','B'); one.get('k')` returned `A`, and it still returned `A` after `two.delete('k')`. The primary skips the ring records it writes itself (`writerId === 0`), which silently assumes a process holds one cache -- but opening the cache from two modules is ordinary, and `open()` was given a second-handle path on purpose. The worker drain had already abandoned the same shortcut for a neighbouring reason ("false whenever L1 had been refilled from L2 between queuing and apply"); the primary kept it. Sharing one L1 between instances was rejected: the per-instance byte budget and the heap guard are per instance, and merging them would make one instance's pressure evict another's entries. Verified by injection: reverting the two call sites fails three assertions in `instances_test.js`. |
```

Add to §13 a line recording that the bug was live in the published code path.

- [ ] **Step 11: Commit**

```bash
git add src/turbokv.js test/instances_test.js test/run.js DESIGN.md
git commit -m "$(cat <<'EOF'
Stop the primary serving stale L1 values across instances

Two instances in one process served each other stale values, including
deleted ones, because the primary skips the ring records it writes
itself -- which assumes one cache per process. The worker drain had
already abandoned that shortcut.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

### Task 6: Verify the phase and record what it closed

**Files:**
- Modify: `DESIGN.md` (§13 open items, §14 assumptions)
- Modify: `README.md` if it mentions any removed member

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: a tree whose coverage floors still hold and whose docs describe the smaller API. Plan B (L3 core) starts from here.

- [ ] **Step 1: Check the docs for removed members**

Run: `grep -rn "incr\|\.cas(\|namespace" README.md index.d.mts index.mjs index.js`
Expected: no matches. Fix any that appear.

- [ ] **Step 2: Run the type tests**

Run: `npm run test:types`
Expected: PASS. The declarations no longer name the removed members, and `test/types/types.test.ts` must not reference them.

- [ ] **Step 3: Run JS coverage against the floors**

Run: `npm run coverage`
Expected: at or above lines 95, branches 82, functions 91. Removing code raises the ratio for untested branches that went with it; if a floor now sits far below what the tree achieves, raise it to just under the minimum of four consecutive runs, as decision 55 did — do not lower a floor.

- [ ] **Step 4: Run native coverage**

Run: `npm run coverage:native`
Expected: at or above the value recorded in `DESIGN.md` (87.88% at decision 55). The quota branch and the namespace functions were partly covered, so this may move either way; record the new number.

- [ ] **Step 5: Update the open items**

In `DESIGN.md` §13, mark resolved: item 2 (namespace eviction budget), item 16 (quotas not validated against capacity), and the namespace half of item 1. Add the `keys()`/`size` scope change to §14 if the assumptions table mentions namespaces.

- [ ] **Step 6: Commit and push**

```bash
git add DESIGN.md README.md
git commit -m "$(cat <<'EOF'
Record what the API simplification closed

Coverage floors re-checked after the removals; open items 2 and 16
are resolved by deletion rather than by fixing them.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
git push
```

- [ ] **Step 7: Confirm CI is green**

Run: `gh run list --repo krassx/turbokv --limit 1`
Expected: the run for this push succeeds on all nine jobs (ubuntu 18/20/22/24, macos, windows, TSan+ASan+UBSan, opt-in LZ4, coverage). The layout bump touches the arena header, so the sanitiser job is the one to watch.

---

## Self-Review

**Spec coverage.** §2 Phase 0 item 1 (`incr`/`cas`) is Tasks 1-2; item 2 (namespaces) is Tasks 3-4; item 3 (stale L1) is Task 5. §11.1 is closed by Task 3's removal, as the spec says. §11.2 is Task 5. §12.6's rationale is quoted into decision 63. Nothing in Phase 0's scope is unassigned. Phase 1 is deliberately out of this plan.

**Placeholder scan.** No "TBD", "similar to", or "add error handling" steps. Every code step carries the code. The one procedural step without literal code is Task 3 Step 5, which gives exact file-and-line targets plus a `grep` that must print nothing.

**Type consistency.** `#dropOthers(fullKey, self)` is defined in Task 5 Step 4 and called in Steps 3 and 4 with the same signature. `REMOVED_NATIVE` is introduced in Task 2 and extended in Task 4. The native signatures Task 4 produces (`set(key, value, writerId, ttlMs)`, `del(key, writerId)`, `scanKeys(cursorSlot, max)`, `submitSet(key, value, ttlMs)`, `submitDel(key)`) match the call-site edits in the same task. `TC_LAYOUT` 6 and `TCS_LAYOUT` 2 are used consistently.

**Ordering risk.** Task 3 leaves JS passing `0` to native functions that still take a namespace argument, and Task 4 removes both together. That is the only intermediate state, it is deliberate, and `npm test` passes at the end of each task.
