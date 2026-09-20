'use strict';
// Thin JS layer over the L2 prototype, implementing the architecture in DESIGN.md:
//   L1  - per-worker JS Map with a byte budget (values are decoded strings)
//   L2  - shared arena; primary is the sole writer, workers map it read-only
//   writes - worker buffers, then batches to the primary over cluster IPC
//   coherence - workers drain the shared invalidation ring and drop stale L1 entries
const cluster = require('cluster');
const native = require('./native');
const fs = require('fs');
const v8 = require('v8');
const v8ser = require('v8');
const { callArgCounts } = require('./fastpath');
const { assertAdapter } = require('./l3/adapter');
const { L3Queue } = require('./l3/queue');

// An unpaired surrogate encodes to U+FFFD in UTF-8, so '\uD800', '\uDC00' and
// '\uFFFD' all became ONE key in the arena and returned each other's values --
// the same aliasing class as the latin1 folding fixed earlier. Reject such keys
// at the boundary: the native layer refuses them too, but only this check can
// report it as `false` rather than as a silent miss or a shed ring write.
function hasLoneSurrogate(s) {
    for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c < 0xD800 || c > 0xDFFF) continue;
        if (c > 0xDBFF) return true;                       // lone low surrogate
        const n = s.charCodeAt(i + 1);
        if (!(n >= 0xDC00 && n <= 0xDFFF)) return true;    // unpaired high surrogate
        i++;
    }
    return false;
}

// The same folding, applied rather than rejected. A VALUE may contain a lone
// surrogate -- only keys are refused at the boundary -- and UTF-8 has no way
// to carry one, so what comes back out of the arena is U+FFFD where it went
// in. Anything comparing a submitted value against the stored one has to
// compare the STORED form of both. See sameStored.
function foldLoneSurrogates(s) {
    if (!hasLoneSurrogate(s)) return s;                    // the overwhelming case: no copy
    let out = '';
    for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c < 0xD800 || c > 0xDFFF) { out += s[i]; continue; }
        if (c > 0xDBFF) { out += '\uFFFD'; continue; }     // lone low surrogate
        const n = s.charCodeAt(i + 1);
        if (n >= 0xDC00 && n <= 0xDFFF) { out += s[i] + s[i + 1]; i++; continue; }
        out += '\uFFFD';                                   // unpaired high surrogate
    }
    return out;
}

// Mutating methods that bypass Object.freeze because they operate on internal
// slots rather than properties. Shadowed on frozen values so a mutation raises
// instead of silently corrupting the cached object. See deepFreeze.
const DATE_MUTATORS = ['setTime', 'setFullYear', 'setMonth', 'setDate', 'setHours', 'setMinutes',
    'setSeconds', 'setMilliseconds', 'setUTCFullYear', 'setUTCMonth', 'setUTCDate', 'setUTCHours',
    'setUTCMinutes', 'setUTCSeconds', 'setUTCMilliseconds', 'setYear'];
const MAP_MUTATORS = ['set', 'delete', 'clear'];
const SET_MUTATORS = ['add', 'delete', 'clear'];
function frozenMutator(name) {
    return function () {
        throw new TypeError(`Cannot call ${name}() on a frozen cached value ` +
            `(turbokv freeze:true). Copy it before mutating.`);
    };
}

// A single process-wide GC observer feeding every cache that wants the heap
// guard. See the constructor for why this is not per-instance.
// The heap guard needs the LIVE heap: a reading taken after a collection.
// used_heap_size sampled at an arbitrary moment includes uncollected garbage,
// so it rises when you shed and the guard thrashes (decision 22).
//
// A gc PerformanceObserver was the original signal. It is Node-only -- Bun and
// Deno accept the subscription and never emit an entry, so the guard was
// silently inert on both -- and it is less accurate than it looks, because it
// fires on scavenges too, which do not collect old space, so most of its
// samples are taken mid-garbage.
//
// A FinalizationRegistry callback runs only once its sentinel has actually been
// collected, so sampling there is a genuine post-collection reading, and it
// exists on all three runtimes. Measured against a known 42.7MB live set under
// heavy churn: observer mean 147.3MB, FinalizationRegistry mean 52.2MB.
//
// The spec promises nothing about whether or when finalizers run, so a floor
// poll backs it up: the minimum of a rolling window approximates the post-GC
// floor with no GC event at all (mean 85.3MB -- worse than the registry, far
// better than raw polling's 147.1MB, and available unconditionally).
const GC_POLL_MS = 1000;      // backstop cadence; unref'd, so it never holds the process open
const GC_WINDOW = 16;         // samples retained for the floor estimate
const GC_QUIET_MS = 5000;     // finalizers silent this long -> fall back to the floor
const GC_MIN_INTERVAL_MS = 500;   // debounce; see gcOnFinalizer
// -Infinity, not 0: monoMs() is performance.now(), which also starts near zero,
// so a 0 initialiser made the FIRST reading look like it arrived moments after a
// previous one and debounced it away. The guard then sat idle until the second
// collection, and at a long minInterval it could stay idle for a long time.
let gcRegistry = null, gcTimer = null, gcLastSignal = -Infinity, gcLastEval = -Infinity;
let gcMinInterval = GC_MIN_INTERVAL_MS;
let gcEvals = 0, gcDebounced = 0;
const gcSubscribers = new Set();
const gcWindow = [];

function gcArm() {
    // The sentinel is unreachable the moment this returns, so the next
    // collection finalizes it and re-arms the signal.
    if (gcRegistry) { try { gcRegistry.register({}, 1); } catch { /* registry unusable */ } }
}

// How often the guard is willing to LOOK, independent of how often the runtime
// happens to collect. Collection frequency is the runtime's business and varies
// by more than 4x across them under identical churn: 6.3/s on Node, 9.7/s on
// Bun, 28.6/s on Deno. Two reasons not to follow it:
//
//   - the sample is not free, and on Bun 1.4.2 it is expensive out of all
//     proportion: v8.getHeapStatistics() is ~110ns and constant-time on Node,
//     but O(heap) on Bun -- 3.2ms at a 17MB heap rising to 74ms at 411MB.
//     Reported upstream and being fixed (oven-sh/bun#30596, unmerged as of
//     2026-09-10), so treat that figure as dated rather than permanent.
//   - shedding is not free either, and this reason does not expire. Acting 28
//     times a second churns L1 far harder than a memory guard needs to, on
//     every runtime.
//
// So the debounce stays even once Bun's sampling cost is fixed: it was never
// only about that.
//
// A guard acts on a timescale of seconds, so 500ms is ample. The registry is
// re-armed on EVERY callback regardless: dropping a sample must never drop the
// signal, or the chain stops and the guard goes quiet permanently.
function gcOnFinalizer() {
    gcArm();                                   // first and unconditional
    const now = monoMs();
    if (now - gcLastEval < gcMinInterval) { gcDebounced++; return; }
    gcLastEval = now;
    gcLastSignal = now;
    gcEvals++;
    // Canonical source. process.memoryUsage().heapUsed is far cheaper and equals
    // it exactly on Node and Deno (ratio 1.000), but on Bun it tracks something
    // else - it stayed flat at 9.4MB while used_heap_size grew - and Bun's
    // heap_size_limit is not constant either (318MB -> 644MB as the heap grew),
    // so neither the numerator nor the denominator can be shortcut.
    const h = v8.getHeapStatistics();
    gcWindow.length = 0;                       // a real post-GC reading supersedes the floor
    gcNotify(h.used_heap_size, h.heap_size_limit);
}

function gcNotify(used, limit) {
    for (const c of gcSubscribers) c.__internalOnGc(used, limit);
}

function gcSubscribe(inst, minIntervalMs) {
    const first = gcSubscribers.size === 0;
    gcSubscribers.add(inst);
    // The signal is process-wide but the cadence is per-caller, so the most
    // eager subscriber sets the pace for everyone.
    if (typeof minIntervalMs === 'number' && minIntervalMs >= 0) {
        gcMinInterval = first ? minIntervalMs : Math.min(gcMinInterval, minIntervalMs);
    }
    if (gcTimer) return;                       // already running for this process
    if (typeof FinalizationRegistry === 'function') {
        gcRegistry = new FinalizationRegistry(gcOnFinalizer);
        gcArm();
    }
    gcTimer = setInterval(() => {
        const h = v8.getHeapStatistics();
        gcWindow.push(h.used_heap_size);
        if (gcWindow.length > GC_WINDOW) gcWindow.shift();
        // While the registry is delivering, its readings are strictly better;
        // the floor only takes over once finalizers go quiet.
        if (gcRegistry && monoMs() - gcLastSignal < GC_QUIET_MS) return;
        if (!gcWindow.length) return;
        // The debounce applies here too. gcLastSignal only advances on a
        // NON-debounced callback, so with minIntervalMs > GC_QUIET_MS every
        // finalizer after the first was debounced, gcLastSignal froze, this
        // branch decided the registry had gone quiet, and evaluated once a
        // second -- ignoring the very setting that was raised to slow it down.
        const now = monoMs();
        if (now - gcLastEval < gcMinInterval) { gcDebounced++; return; }
        gcLastEval = now; gcEvals++;
        gcNotify(Math.min(...gcWindow), h.heap_size_limit);
    }, GC_POLL_MS);
    if (gcTimer.unref) gcTimer.unref();
}

function gcUnsubscribe(inst) {
    gcSubscribers.delete(inst);
    if (gcSubscribers.size) return;
    if (gcTimer) { clearInterval(gcTimer); gcTimer = null; }
    gcRegistry = null;                         // drops any pending registration with it
    gcWindow.length = 0;
    gcLastSignal = -Infinity; gcLastEval = -Infinity;
    gcMinInterval = GC_MIN_INTERVAL_MS;
}

// Observable pace, so a caller can see the guard is alive and how often it is
// actually looking - rather than how often the runtime happens to collect.
function gcPace() { return { evaluations: gcEvals, debounced: gcDebounced, minIntervalMs: gcMinInterval }; }

// L1 expiry runs on a MONOTONIC clock, matching the arena's tick-based epoch.
// With Date.now() an NTP step moved L1 and L2 expiry in opposite directions:
// a backward step made L1 entries immortal while L2 expired them on schedule,
// and a forward step did the reverse. performance.now() measured 21.5ns against
// Date.now()'s 23.7ns, so correctness here is free.
const { performance } = require('perf_hooks');
const monoMs = () => performance.now();

// Keys are strings. Anything else is a caller error, and the API promises to
// report those rather than throw -- but handing a non-string straight to the
// native layer throws outright for a Symbol, and runs arbitrary caller code
// for an object with a toString. It also silently coerced: set(undefined, v)
// stored under 'undefined', and every plain object aliased to
// '[object Object]', so two different objects were one key. Reject anything
// that is not already a string.
function isStringKey(k) { return typeof k === 'string'; }

// Wire size of an encoded value, in BYTES. Not `.length`: that is UTF-16 units
// for a string (so a multi-byte value undercounts) and `undefined` for a
// bytes-mode number, boolean or bigint, which poisons any total it is added to.
// 8 is what set() charges a scalar against the arena bound, and the two
// measurements must agree or the same value is two different sizes depending on
// which bound is asking.
function encodedBytes(enc) {
    if (typeof enc === 'string') return Buffer.byteLength(enc);
    return Buffer.isBuffer(enc) ? enc.length : 8;
}

// Is the arena still holding exactly the value we wrote? Buffers compare by
// content -- a binary value read back out of L2 is a different Buffer object
// than the one that went in, so `===` would answer "no" for every binary value
// and the caller would skip a step it should have taken.
//
// `===` IS NOT THE RIGHT COMPARE FOR THE REST EITHER, and the two cases it
// gets wrong are both silent: the caller (the l3FailTtlMs cap) reads a false
// answer as 'moot' -- "the arena no longer holds this, there is nothing to
// bound" -- so nothing is capped, nothing is counted, and the value L3
// refused stays resident with no expiry.
//
//   NaN  -- `NaN !== NaN`, so a cached NaN was never capped. It round-trips
//     through the arena perfectly; only the comparison failed.
//   a lone surrogate -- UTF-8 cannot carry one, so `'ab\uD800cd'` is stored,
//     and read back, as `'ab' + U+FFFD + 'cd'`. The submitted and stored forms differ
//     by construction, for every such value.
//
// Both are answered by comparing the STORED forms: fold the submitted string
// the way the arena folds it, and treat two NaNs as the one value they are.
// -0 stays equal to 0 -- `Object.is` would have split them, and the arena
// does not, so using it here would have STOPPED capping a value that caps
// correctly today.
function sameStored(a, b) {
    if (Buffer.isBuffer(a) || Buffer.isBuffer(b))
        return Buffer.isBuffer(a) && Buffer.isBuffer(b) && a.equals(b);
    if (a === b) return true;
    if (typeof a === 'number' && typeof b === 'number')
        return Number.isNaN(a) && Number.isNaN(b);
    if (typeof a === 'string' && typeof b === 'string')
        return foldLoneSurrogates(a) === foldLoneSurrogates(b);
    return false;
}

// The storage presets. Anything else is a misconfiguration, not a mode.
const STORAGE_MODES = ['bytes', 'direct', 'safe', 'primitives'];

const MSG = 'tc';
const RING_MSG = 'tcr';   // doorbell only: 'your submission rings are non-empty'
// Max second-chance reprieves per L1 insert. Matches the arena's budget in
// store_ops.h; see the eviction loop for why an unbounded value is O(n).
const L1_SECOND_CHANCE_BUDGET = 16;

// THE WORKER'S HALF OF `l3FailTtlMs`.
//
// A write L3 rejected is kept locally under a short TTL, so the box serves
// through the outage and then converges (decision 68). The L2 half of that cap
// is applied by the PRIMARY in both cases -- on its own behalf, or on a
// worker's behalf through the conditional 'r' op (see #capArena). What stays
// on the worker is its OWN READS: between the moment the cap falls due and the
// moment the primary applies it, the capped L1 entry has expired and a read
// falls straight through to the uncapped L2 copy, so the worker would serve
// the value L3 refused past its own deadline. #l3Caps is that guard, and the
// two numbers below bound it.
//
//   POLL is how promptly a retired entry stops costing anything. It is a
//   timestamp compare per entry -- no arena work at all since the cap became a
//   conditional op -- and the timer only exists while an entry is outstanding.
//
//   WINDOW is how long PAST its deadline an entry is kept. Before the deadline
//   the capped L1 entry answers and the guard is not consulted; after it, the
//   question is whether the primary has applied the cap yet. A cap sent in the
//   ordinary IPC batch is applied on the primary's next turn, or -- if that
//   batch was shed -- never; four maintenance backstop intervals (500ms each)
//   is long enough for a loaded primary and short enough that a guard for a
//   cap that is never coming stops making the key miss.
const L3_CAP_POLL_MS = 20;
const L3_CAP_WINDOW_MS = 2000;
// Bound on outstanding caps, for the same reason #pendingDel has one: a worker
// writing hard against a dead L3 must not grow a map without limit. Dropping
// one costs a longer local disagreement, which is what this whole mechanism is
// bounding anyway.
const L3_CAP_MAX = 4096;
// The same bound, applied per mark set -- what this process has REMOVED and
// what it has WRITTEN and not seen land (see PendingMarks) -- and to the map
// recording where this instance's own removals landed on the ring. Named
// rather than repeated, because a wrapped ring no longer clears the removal
// marks (see #drain) and the bound is now the only thing keeping them finite.
const PENDING_MARK_MAX = 4096;
// How many REMOVAL marks one drain may check against the arena after a wrapped
// ring. A check is a has(), which copies no value, but #drain() runs from every
// get() and has(), and a ring that keeps lapping between reads makes every read
// wrap again -- so a full pass over the bound (measured at 0.28ms) would be
// paid per read, ~28% of a core at 1k reads/s. A budgeted slice with the
// remainder carried to the next drain rather than dropped -- the shape the
// cap poll used to share, before the cap stopped touching the arena at all.
const PENDING_MARK_SCAN = 64;
let storeReady = false;
let submitName = null;    // primary: the segment it created, null = IPC transport
let submitReady = null;   // worker: the segment name it successfully opened
let attachedName = null;  // worker: the arena name, so it can re-attach after a primary death
let isPrimaryProcess = false;   // set by createPrimary; guards the id-0 write path
const installedWorkers = new WeakSet();   // workers already wired by install()      // the native store is a per-process singleton
const instances = new Set();  // live caches in THIS process, for local invalidation

// PROCESS-WIDE count of L3 clears currently in flight, not per-instance.
// clearAll() wipes the shared arena (decision 64: several instances in one
// process share one L2), so while ANY instance's clear is on its way to L3,
// an L3 fill from ANY instance -- including one that never issued the clear
// itself -- would promote a value the clear is erasing straight back into
// the arena every instance reads from. A count rather than a boolean so two
// overlapping clears cannot have the first one's completion unblock reads
// while the second is still pending. Read on every L3 fetch, so kept to one
// integer comparison.
//
// IT IS NOT THE WHOLE GUARD, AND CANNOT BE. The arena is shared across
// PROCESSES and this counter is not, so a clear issued here left every other
// process reading L3 unguarded -- and one of them promoting the pre-clear
// value into the shared arena undid the clear for everyone, this process
// included (decision 70). The cross-process half lives in the arena header
// (Header::l3ClearGen) and is read by #sharedClearsInFlight below.
//
// The counter STAYS ALONGSIDE it rather than being replaced, because it is
// the only signal that exists during the window the header cannot cover: on a
// worker the generation is opened by the primary when it applies the clear's
// IPC batch, which is a scheduled flush away, and this process must serve
// misses from the instant clearAll() returns, not from whenever the primary
// gets to it. It is also all a degraded worker (no arena to read) has left.
let l3ClearsInFlight = 0;

// THIS PROCESS'S ATTACHMENT IDENTITY, minted once and never reused.
//
// The primary tracks which clear generations a worker has opened so it can
// settle them if that worker vanishes (see #clearGensOwed). Keying that on the
// WRITER ID was wrong, and not in a theoretical way: `attachWorker` takes the
// id from the caller, and a stable per-slot index out of the environment is
// the normal way to name workers across restarts. A worker that died and was
// replaced in the same slot therefore had its SUCCESSOR's outstanding clear
// settled by its own reconciliation -- the guard dropped while L3 was still
// mid-clear, and the next read resurrected exactly what the clear was
// removing. No misuse required; the ids are supposed to be reused.
//
// The nonce is per PROCESS rather than per instance: it names the channel the
// generation arrived on, which is the thing that goes away, and several
// instances in one process share one channel (decision 64). It rides on the
// batch message, so every wiring gets it -- install()'s and a hand-rolled
// one alike -- and a successor in the same slot is a different attachment
// however early it attaches, which is what makes the ordering safe: nothing
// is ever released by a name a live process could also be answering to.
const ATTACH_NONCE = `${process.pid}.${Date.now().toString(36)}.${Math.random().toString(36).slice(2, 10)}`;

// WHAT THIS WORKER OWES L2, AND WHICH KIND OF THING IT OWES.
//
// A worker cannot write the arena. Between submitting an operation and the
// primary applying it, the arena still holds the PREVIOUS value for that key,
// so this worker must not read L2 for it -- and it must remember the fact
// until its own operation comes back around the invalidation ring.
//
// There used to be ONE set for that, `#pendingDel`, and a worker took it for
// two different things: a DELETE ("I removed this key") and a `minLevel: 2`
// SET ("I wrote this key and it has not landed"). One mark, two meanings, and
// every consumer had to guess which it was looking at. Each guessed
// differently, and the third adversarial review found four defects in the
// guesses -- one High:
//
//   - the l3FailTtlMs cap read a SET's mark as "our own delete is on its way"
//     and answered 'moot', so a value L3 had refused was never capped, never
//     counted, and sat in the shared arena with no expiry. The ordinary case
//     during an outage, because a shed queue settles `false` in a microtask,
//     before any mark can clear.
//   - the wrapped-ring reconciliation keeps a mark while `native.has(key)` is
//     true. That is right for a delete, whose landing is "the key is GONE",
//     and exactly backwards for a set, whose landing is "the key is HELD" --
//     so a wrap made the key unreadable from every tier, L3 included.
//   - `#promotionBlock` reported a pending SET as `l3DeletedWhileReading` and
//     answered the caller `undefined`, i.e. counted sets in a counter named
//     for prevented resurrections.
//   - the mark landed in `#deletedAt`, which exists to refuse an in-flight
//     read that would resurrect a REMOVED key.
//
// So there are two sets, each naming what it holds, and a consumer asks the
// question it actually means. The one consumer that genuinely wants EITHER --
// "can I read L2 for this key at all" -- says so explicitly (#unappliedHere).
//
// Each mark carries:
//   hash  the key's ring hash, so a record can find it. #l1Drop has already
//         removed the #byHash entry, so without this a marked key stayed
//         suppressed even after another process recreated it.
//   at    the ring head as it was BEFORE this operation was published. Only a
//         record AT OR PAST that can be ours; a hash match alone let a record
//         written before our operation -- and not yet drained here -- clear
//         the mark, which served the value we had just removed or replaced.
//         Read before the submission, never after: the primary can drain and
//         append our record between the push and the read, and a mark stamped
//         past its own record is one nothing can ever clear.
//   seq   a process-wide, monotonic mark number, so a batch that is dropped
//         releases the marks IT took and not a newer mark for the same key.
//   sub   the SUBMISSION ring position just past our own record, read from
//         native.submitHead() immediately after the push. The submission ring
//         is SPSC with this worker as the producer, so the primary's consumer
//         index answers "has my record been applied?" exactly, with no
//         guessing from the arena -- see #reconcileWrites, which is the one
//         place that needs an answer the invalidation ring cannot give.
//         -1 when the question is unanswerable: the IPC fallback, whose
//         batches are acked by nothing, and any push with no ring claimed.
//
// Zero for `at` when the head cannot be read (degraded, detached): every
// record then clears the mark, which is the behaviour this replaces and the
// safe direction -- a mark dropped early costs one possible stale read, a
// mark never dropped makes the key unreadable from every tier.
let markSeq = 0;
class PendingMarks {
    // `kind` is 'delete' or 'write'. It is carried rather than inferred, and
    // it is what the two fields of TurboKV are named after: there is no way to
    // take or release a mark without having chosen one of the two sets, which
    // is the property test/write_sites_test.js pins.
    constructor(kind) { this.kind = kind; this.byKey = new Map(); this.byHash = new Map(); }
    get size() { return this.byKey.size; }
    // Guarded on `size` at every call site in the hot path; kept here too so a
    // new caller cannot forget: an empty Map lookup is still a lookup.
    has(key) { return this.byKey.size !== 0 && this.byKey.has(key); }
    keys() { return this.byKey.keys(); }
    clear() { this.byKey.clear(); this.byHash.clear(); }

    // THE BOUND EVICTS THE OLDEST, and does not wipe. A Map is
    // insertion-ordered, and the oldest mark is the one whose operation has
    // had the longest to be applied. Dropping ONE costs one possible stale
    // read; dropping four thousand costs four thousand.
    //
    // Delete before set: re-marking a key must refresh its place in the
    // order, not leave a fresh mark in an old slot -- `Map.set` on a key
    // already present keeps its FIRST-insertion position, which would evict
    // exactly the keys most likely to still need the mark.
    mark(key, hash, at, sub = -1) {
        this.byKey.delete(key);
        if (this.byKey.size >= PENDING_MARK_MAX) {
            const oldest = this.byKey.entries().next();
            if (!oldest.done) {
                this.byKey.delete(oldest.value[0]);
                if (this.byHash.get(oldest.value[1].hash) === oldest.value[0])
                    this.byHash.delete(oldest.value[1].hash);
            }
        }
        this.byKey.set(key, { hash, at, seq: ++markSeq, sub });
        this.byHash.set(hash, key);
    }
    // Where on the SUBMISSION ring this mark's record ends, or -1 when that is
    // unknowable. `undefined` means there is no such mark -- a caller
    // iterating a slice has to tell "released under me" from "nothing to
    // compare", which is the distinction #reconcileRemovals makes with has().
    submittedAt(key) {
        const m = this.byKey.get(key);
        return m === undefined ? undefined : m.sub;
    }
    // Move an existing mark to the BACK of the order without changing it --
    // not a re-mark: the position it was taken at is what a record is compared
    // against, and its sequence number is what a dropped batch releases by, so
    // a reconciliation pass that merely re-examines a mark must not renumber
    // it. See #reconcileRemovals.
    touch(key) {
        const m = this.byKey.get(key);
        if (m === undefined) return;
        this.byKey.delete(key);
        this.byKey.set(key, m);
    }
    release(key) {
        const m = this.byKey.get(key);
        if (m === undefined) return false;
        this.byKey.delete(key);
        // Only if it is still OURS: two keys can share a hash, and the later
        // mark owns the slot.
        if (this.byHash.get(m.hash) === key) this.byHash.delete(m.hash);
        return true;
    }
    // Release only a mark taken at or before `seq`. Used by the batch-drop
    // paths in flush(): the send can fail asynchronously, long after the
    // worker has written the same key again and taken a NEW mark for a
    // submission that is still perfectly live.
    releaseUpTo(key, seq) {
        const m = this.byKey.get(key);
        if (m === undefined || m.seq > seq) return false;
        return this.release(key);
    }
    // The key this set holds for `hash`, but only when the record at `pos` is
    // at or past where that mark was taken. Anything earlier was already on
    // the ring when we marked, so it cannot be ours.
    matchAt(hash, pos) {
        if (this.byHash.size === 0) return undefined;
        const key = this.byHash.get(hash);
        if (key === undefined) return undefined;
        const m = this.byKey.get(key);
        return m !== undefined && pos >= m.at ? key : undefined;
    }
}

class TurboKV {
    #l1 = new Map();          // key -> { v, bytes, hits }
    #byHash = new Map();      // hash hex -> key   (ring records carry hashes)
    #l1Bytes = 0;
    #l1Iter = null;
    #ringIdx = -1;            // shared-memory submission ring, -1 = use IPC
    #ringMaxValue = 0;        // largest value one ring record can carry
    #transportOpt = 'shm';
    // THE TWO MARKS, and they are not interchangeable. See PendingMarks for
    // what each one means, what every consumer used to guess, and why the
    // guessing is what had to go.
    #pendingDel = new PendingMarks('delete');    // "I REMOVED this key, the primary has not applied it"
    #pendingWrite = new PendingMarks('write');   // "I WROTE this key and it has not landed" (minLevel >= 2)
    // Keys THIS instance removed, with the invalidation-ring position at which
    // the removal became visible. #pendingDel answers "has my removal been
    // applied yet"; this answers the different question the promotion guard
    // asks -- "was this key removed AFTER the read in my hand started" -- and it
    // has to outlive #pendingDel, which is cleared the moment the removal
    // lands. Without it, a delete that both began and landed during one
    // getAsync left the guard with only a ring hash to go on, which says "this
    // key changed" and not "you removed it", so the caller was handed the value
    // it had just deleted while get() answered undefined for the same key.
    //
    // Populated only when an adapter is attached: with no L3 there is nothing
    // to promote and nothing to guard.
    #deletedAt = new Map();
    // Marks a wrapped ring left this process unable to account for, still to be
    // reconciled against the arena. A count rather than a flag, because the
    // reconciliation is budgeted and has to resume across drains. See
    // #reconcileRemovals.
    #reconcileOwed = 0;
    // The same, for WRITE marks, against the submission ring's consumer index
    // rather than the arena. Separate from #reconcileOwed because it is a
    // different question with a different answer source, and folding the two
    // into one counter is precisely the overloading this pass exists to undo.
    // See #reconcileWrites.
    #writeOwed = 0;
    #doorbellPending = false;           // retained FIFO cursor into #l1; see #oldestEntry
    #l1Max;
    #outbox = [];
    // Keys of the `r` (conditional re-time) entries currently sitting in the
    // outbox. Tracked alongside rather than scanned out of it, so flush() --
    // which is the IPC hot path -- pays nothing at all when no cap is
    // outstanding, which is every moment L3 is healthy. See #capUnapplied.
    #outboxCapKeys = null;
    #outboxBytes = 0;
    #outboxMaxBytes = 1 << 20;      // flush eagerly past this, bounding worker memory
    #inFlightBytes = 0;       // bytes handed to process.send and not yet drained
    #maxInFlightBytes = 8 << 20;
    #flushScheduled = false;
    #cursor = 0;
    #maxValue = 0;
    #timer = null;
    #sweepCursor = 0;
    #drainTicks = 0;
    #lastStaleCheck = 0;
    #noHeartbeatWarned = false;
    #staleMs = 5000;
    #primaryDead = false;
    #keyMax = 1024;
    #l3 = null;
    #queue = null;
    #l3FailTtlMs = 5000;
    // key -> { enc, cap, deadline, until, sent }. Worker-only: L2 caps waiting
    // for the primary to apply the write they are re-timing. See
    // L3_CAP_POLL_MS. Empty on a primary and on a cache with no adapter, and
    // every read-path check is guarded on `.size` so it costs nothing there.
    #l3Caps = new Map();
    #capTimer = null;
    #l3TtlMs = 60000;
    #l3RetryMs = 2000;
    #l3CloseTimeoutMs = 5000;
    #closePromise = null;       // memoized: makes close() idempotent, see close()
    #lastQueued = null;
    #onL3Error = null;
    #inflight = new Map();      // key -> in-flight L3 read, so a herd shares one request
    // Native expiry is milliseconds from the arena's creation time.
    #id;
    #codec;
    #l1Decoded = true;
    #isolate = true;
    #noCodec = false;
    #freeze;
    #heapFactor;
    #guardMax = 0; #guardShed = 0.25; #gcObserver = null; liveHeapFraction = 0;
    #attached = false;
    stats = { l1Hits: 0, l2Hits: 0, misses: 0, sets: 0, deletes: 0, invalidated: 0,
              flushes: 0, sent: 0, rejectedType: 0, rejectedSize: 0 };
    lastError = null;

    // Resolves the decision 4 / decision 5 tension. L2 still stores bytes only
    // (decision 4 holds at the wire and arena boundary). But when a codec is
    // supplied, L1 holds the DECODED value, so an L1 hit skips decoding
    // entirely - which is what decision 5 was actually for. Without a codec the
    // value is already opaque bytes and L1 is optimal as-is.
    constructor(opts = {}) {
        this.#maxValue = native.maxValueBytes();
        this.#keyMax = native.keyMaxBytes();
        instances.add(this);
        this.#l1Max = opts.l1MaxBytes ?? (2 * 1024 * 1024);
        this.#outboxMaxBytes = opts.outboxMaxBytes ?? (1 << 20);
        // `??` not `||`: 0 is a meaningful value (send nothing) and a test that
        // passed 0 to disable sending silently got the 8MB default instead,
        // making a control phase identical to the phase it was controlling.
        this.#maxInFlightBytes = opts.maxInFlightBytes ?? (8 << 20);
        this.#staleMs = opts.primaryStaleMs || 5000;
        // Validated here rather than on the first cache miss: a malformed
        // adapter discovered inside a promise chain, in a process that has been
        // serving for an hour, is the worst place to learn about it.
        if (opts.l3 !== undefined && opts.l3 !== null) this.#l3 = assertAdapter(opts.l3);
        this.#l3FailTtlMs = opts.l3FailTtlMs ?? 5000;
        this.#l3TtlMs = opts.l3TtlMs ?? 60000;
        // See close(): bounds how long shutdown waits for the L3 queue to
        // drain. `??` not `||` -- 0 is a meaningful value (wait without a
        // bound, matching l3TtlMs's own 0-disables convention) and must not
        // silently become the default instead.
        this.#l3CloseTimeoutMs = opts.l3CloseTimeoutMs ?? 5000;
        // Held on the instance as well as handed to the queue: it is the budget
        // for ONE L3 operation, and the read path has no queue to hold it for
        // it. A hung `get` is the same outage as a failed one and has to be
        // bounded by the same number, or the two halves of the adapter contract
        // would give up at different times.
        // A POSITIVE NUMBER, ALWAYS. This is the per-attempt deadline as well
        // as the retry budget, and the deadline helper treats anything <= 0 as
        // "no bound at all" (src/l3/queue.js) -- so `l3RetryMs: 0` silently
        // removed every bound this option exists to impose. The worst of them
        // is `clear`, which retries indefinitely by design: a hung
        // `adapter.clear()` then never settled, the clear generation stayed
        // open, and EVERY process sharing the arena served L3 misses for every
        // key until the worker exited. `0` is documented as a value only for
        // `l3CloseTimeoutMs`, where it means "wait without a bound" and where
        // that is a choice a caller can sensibly make. Here it is not one, so
        // say so rather than substituting a default the caller did not ask for.
        if (this.#l3 && opts.l3RetryMs !== undefined &&
            !(typeof opts.l3RetryMs === 'number' && opts.l3RetryMs > 0 && Number.isFinite(opts.l3RetryMs)))
            throw new Error(`turbokv: l3RetryMs must be a positive number of milliseconds, got ` +
                            `${JSON.stringify(opts.l3RetryMs)}. It bounds EACH adapter call as well as ` +
                            `the retry budget, so a non-positive value leaves a hung adapter unbounded -- ` +
                            `and a hung clear() leaves every process on this arena serving L3 misses. ` +
                            `Only l3CloseTimeoutMs takes 0, where it means "wait without a bound".`);
        this.#l3RetryMs = opts.l3RetryMs ?? 2000;
        // One place holds the listener. It used to be read out of `opts` from
        // inside the queue's callback, which meant a second reporting path grew
        // the moment a second caller needed one -- and a duplicated report is
        // exactly the defect the queue's own onError contract just removed.
        this.#onL3Error = typeof opts.onL3Error === 'function' ? opts.onL3Error : null;
        if (this.#l3) {
            this.#queue = new L3Queue(this.#l3, {
                maxBytes: opts.l3QueueMaxBytes ?? (8 << 20),
                retryMs: this.#l3RetryMs,
                // A background failure must never reach `lastError`: the sync
                // call it belongs to returned long ago, and a caller reading
                // lastError would take it as the reason for a later operation.
                // onError now fires exactly once per operation, and only when
                // the operation is abandoned (see src/l3/queue.js) -- a retry
                // that goes on to succeed reports nothing, so no dedup is
                // needed here any more.
                onError: (e, op) => {
                    if (op.kind === 'set') this.stats.l3SetFailed = (this.stats.l3SetFailed || 0) + 1;
                    else if (op.kind === 'delete') this.stats.l3DeleteFailed = (this.stats.l3DeleteFailed || 0) + 1;
                    this.#reportL3(e, op);
                },
                // A shed op is refused before it is ever sent, so onError above
                // never sees it -- that hook means "abandoned after being sent".
                // Without this, `l3SetFailed`/`l3DeleteFailed` never move for a
                // write this process shed on the spot, and an operator cannot
                // tell "L3 is refusing my writes" from "I am shedding them
                // before they are sent" -- different causes, different fixes.
                onShed: () => { this.stats.l3Shed = (this.stats.l3Shed || 0) + 1; },
            });
            // A live gauge, not a snapshot: bytes currently outstanding (queued
            // plus in flight) in the L3 queue, read straight from it on every
            // access. `stats` is a single persistent object that existing code
            // mutates in place (`this.stats.xxx++`), so this is a getter
            // property on that same object rather than a plain field computed
            // once -- computing it once would go stale the instant the queue's
            // own #bytes changed.
            Object.defineProperty(this.stats, 'l3QueueBytes', {
                get: () => this.#queue.pendingBytes,
                enumerable: true,
            });
        }
        // `|| 0` also mapped an explicit 0 to the primary role. That is only
        // legitimate in the process that actually created the arena.
        this.#id = opts.workerId ?? 0;
        if (this.#id === 0 && opts.attached !== false && !isPrimaryProcess)
            throw new Error('turbokv: workerId 0 is the primary; a worker must use ' +
                            'attachWorker() or open() so cluster assigns its id');
        this.#attached = opts.attached !== false;
        this.#transportOpt = opts.transport || 'shm';
        // 'primitives' mode: accept only string/number/boolean/null. Buys three
        // things the codec mode cannot: byte accounting that is exact rather
        // than a heapFactor estimate, no aliasing hazard (primitives are
        // immutable), and no codec to configure. Costs the caller a decode on
        // every L1 hit if its values are really objects.
        // Storage modes. Each names the tradeoff it accepts:
        //
        //   bytes      - NO CODEC. The native layer encodes the value directly:
        //                 scalars become their byte representation, binary is
        //                 stored verbatim. Anything needing a codec is rejected
        //                 LOUDLY. Exact byte accounting, no aliasing.
        //                 ('primitives' is accepted as a legacy alias, but the
        //                 mode never accepted only primitives once decision 4's
        //                 Buffer/TypedArray values were honoured - bytes are the
        //                 most directly storable thing there is, and routing
        //                 them through a codec costs 5.3x on write, 3.9x on read.)
        //   direct     - value stored as-is with full JS type fidelity
        //                (v8 structured serialization). One serialize +
        //                deserialize per write; reads are free because L1 hands
        //                back the frozen decoded object. Mutation throws.
        //   safe       - everything through JSON. Every read parses, so callers
        //                get a fresh mutable object and cannot corrupt anything.
        //                Cheap writes, and JSON's silent type conversions apply:
        //                Date becomes a string, Map/Set become {}.
        TurboKV.#assertStorageOptions(opts);
        // `values` is documented as a legacy alias for `storage`, and until now
        // it was not one: only 'bytes'/'primitives' did anything, because those
        // happen to coincide with the internal no-codec flag this option also
        // sets. `values: 'direct'` and `values: 'safe'` silently selected BYTES
        // mode -- so a caller following the declaration got objects rejected on
        // every set(). It is a real alias now.
        const preset = opts.storage !== undefined && opts.storage !== null
            ? opts.storage : opts.values;
        if (preset === 'direct') {
            opts = { isolate: true, freeze: true, ...opts,
                     codec: opts.codec || TurboKV.V8_CODEC };
        } else if (preset === 'safe') {
            opts = { ...opts, codec: opts.codec || TurboKV.JSON_CODEC, l1Decoded: false };
        } else if (preset === 'bytes' || preset === 'primitives') {
            opts = { ...opts, values: 'bytes' };
        }
        this.storage = preset === 'primitives' ? 'bytes'
            : (preset || (opts.codec ? 'codec' : 'bytes'));
        // safe mode keeps the ENCODED form in L1 and decodes on every read, so
        // each caller gets its own object. direct keeps the decoded object.
        this.#l1Decoded = opts.l1Decoded !== false;
        this.#noCodec = opts.values === 'bytes' || opts.values === 'primitives';
        this.#codec = this.#noCodec ? null : (opts.codec || null);
        if (this.#codec && opts.allowSlowCodec !== true) TurboKV.assertFastCodec(this.#codec);
        // Safe by default, fast by choice. Without freeze, mutating what get()
        // returned silently corrupts L1 for this worker until eviction, at
        // which point the value reverts to L2's copy - a bug that appears and
        // disappears on its own. Measured: freeze delivers the same
        // immutability guarantee as re-parsing on every get (the bugsee
        // approach) at roughly twice the throughput, 813k vs 413k ops/s.
        this.#freeze = opts.freeze !== false;
        // set() otherwise adopts the caller's own object into L1. Mutating a
        // variable they still hold then corrupts the cache without any call to
        // get(), and the value silently reverts when L1 evicts and L2's
        // pre-mutation bytes come back. Decoding our own encoding costs one
        // parse per set and gives L1 an object the caller has never seen.
        this.#isolate = opts.isolate !== false;
        // A decoded object costs several times its encoded size on the V8 heap,
        // and JS cannot measure that. The budget is in encoded bytes scaled by
        // this factor; it is an estimate, not a guarantee.
        this.#heapFactor = (this.#noCodec || !this.#l1Decoded) ? 1
            : (opts.heapFactor || (this.#codec ? 3 : 1));
        // Per-object size cannot be measured: V8 exposes no such API, and a
        // structural estimate is both less accurate than encodedBytes*3 and far
        // more expensive. So do not try. Bound the thing that actually matters -
        // LIVE heap - instead.
        //
        // used_heap_size sampled at an arbitrary moment includes uncollected
        // garbage, so it rises when you shed and the guard thrashes. Read it
        // immediately after a major GC instead, where it is the live set.
        const g = opts.heapGuard;
        if (g !== false) {
            this.#guardMax = (g && g.maxHeapFraction) || 0.80;
            this.#guardShed = (g && g.shedFraction) || 0.25;
            // ONE observer per process, not one per cache. Each instance used to
            // register its own, so an application that opens caches without
            // closing them accumulated observers as well as instances: 20k opens
            // cost ~24MB. The guard is a process-wide signal; the per-instance
            // part is only the thresholds.
            this.#gcObserver = true;
            gcSubscribe(this, g && g.minIntervalMs);
        }
    }

    // A JSON codec MUST call JSON.stringify/parse with no second argument.
    // Measured on Node 26: a replacer costs 3.51x and 2-space indent 2.11x,
    // because Node 26 sped up the fast path (34%) without speeding up the slow
    // ones - so falling off is worse now than it was on Node 24.
    //
    // The codec comes from the caller, so a source-level lint cannot see it.
    // Probe it instead: if it emits something JSON-shaped that is not byte-
    // identical to canonical JSON.stringify, it is on a slow path. Codecs that
    // are not JSON at all (msgpack, protobuf) do not emit a leading '{' and are
    // left alone. Opt out with allowSlowCodec: true.
    static assertFastCodec(codec) {
        // An identity replacer - JSON.stringify(v, (k, x) => x) - produces
        // byte-identical output, so probing cannot see it, yet it still costs
        // 3.51x. Read the function source instead. Native or bound functions
        // report [native code] and are left alone.
        for (const [which, fn] of [['encode', codec.encode], ['decode', codec.decode]]) {
            let src = '';
            try { src = Function.prototype.toString.call(fn); } catch { continue; }
            if (src.includes('[native code]')) continue;
            const name = which === 'encode' ? 'JSON.stringify' : 'JSON.parse';
            for (const call of callArgCounts(src, name)) {
                if (call.args > 1) {
                    throw new Error(
                        `codec.${which} is not on V8's JSON fast path: ${call.text.slice(0, 60)} ` +
                        `passes ${call.args} arguments. ${name} must take exactly one ` +
                        `(a replacer costs 3.51x and indentation 2.11x on Node 26). ` +
                        `Pass allowSlowCodec: true to override.`);
                }
            }
        }

        const probe = { b: 1, a: 'x', n: [1, 2] };
        let enc;
        try { enc = codec.encode(probe); } catch { return; }
        if (typeof enc !== 'string' || enc[0] !== '{') return;   // not a JSON codec
        const canonical = JSON.stringify(probe);
        if (enc === canonical) return;
        const why = /\n|\n\s/.test(enc) || /: /.test(enc)
            ? 'it indents or spaces its output'
            : 'it filters or reorders keys';
        throw new Error(
            `codec.encode is not on V8's JSON fast path: ${why}. ` +
            `Use JSON.stringify(value) with no replacer and no space argument ` +
            `(a replacer costs 3.51x and indentation 2.11x on Node 26). ` +
            `Pass allowSlowCodec: true to override.`);
    }

    static get JSON_CODEC() { return { encode: JSON.stringify, decode: JSON.parse }; }
    static get V8_CODEC() {
        return { encode: v => v8ser.serialize(v).toString('latin1'),
                 // allocUnsafeSlow, NOT Buffer.from: deserialize does not copy a
                 // typed array out of its input. Node's DefaultSerializer sets
                 // _setTreatArrayBufferViewsAsHostObjects(true), so an
                 // ArrayBufferView is written as a host object and read back as a
                 // VIEW over whatever buffer we pass in. Buffer.from() returns a
                 // slice of the shared 8KB pool, which made every cached typed
                 // array pin a whole pool slab, and left the view's correctness
                 // resting on the runtime deriving its address from a NON-ZERO
                 // byteOffset -- which Deno 2.8.3 gets wrong, adding that offset
                 // twice, so values came back zero-filled or threw RangeError.
                 // An unpooled, exactly-sized buffer has byteOffset 0 (nothing to
                 // double) and is owned outright by the value decoded from it.
                 // latin1 is one byte per code unit, so length is the byte count.
                 decode: s => {
                     const b = Buffer.allocUnsafeSlow(s.length);
                     b.write(s, 'latin1');
                     return v8ser.deserialize(b);
                 } };
    }

    // KNOWN HOLE: Object.freeze throws on an ArrayBuffer view with elements,
    // and JS offers no way to make one immutable (ArrayBuffer
    // transferToImmutable does not exist in Node 24 or 26). So in 'direct' mode
    // the object graph is frozen but typed-array CONTENTS stay writable, and a
    // caller that writes into one corrupts L1 for its own process until the
    // entry is evicted, at which point the arena's copy comes back. Values
    // holding typed arrays want 'safe' mode, or a defensive copy by the caller.
    static deepFreeze(o) {
        if (o === null || typeof o !== 'object' || Object.isFrozen(o)) return o;
        if (ArrayBuffer.isView(o) || o instanceof ArrayBuffer) return o;   // cannot be frozen
        // Object.freeze does not seal INTERNAL SLOTS, so a "frozen" Date, Map or
        // Set still mutates through its own methods: d.setTime(0), m.set(k, v)
        // and s.add(x) all succeeded and corrupted L1 until the entry was
        // evicted. The whole point of freeze is to turn silent corruption into a
        // TypeError, so shadow those mutators with throwing own-properties
        // BEFORE freezing (afterwards the object is non-configurable).
        const mutators = o instanceof Date ? DATE_MUTATORS
                       : o instanceof Map ? MAP_MUTATORS
                       : o instanceof Set ? SET_MUTATORS : null;
        if (mutators) {
            for (const m of mutators) {
                if (typeof o[m] !== 'function') continue;
                Object.defineProperty(o, m, { value: frozenMutator(m), writable: false, configurable: false, enumerable: false });
            }
        }
        Object.freeze(o);
        for (const k in o) TurboKV.deepFreeze(o[k]);
        return o;
    }

    // --- lifecycle -------------------------------------------------------
    // Sizing per DESIGN.md section 7, computed once at startup.
    static autoSize() {
        const os = require('os'), v8m = require('v8');
        const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
        const arena = clamp(Math.floor(os.totalmem() * 0.01), 16 << 20, 128 << 20);
        const l1 = clamp(Math.floor(v8m.getHeapStatistics().heap_size_limit * 0.005), 512 * 1024, 2 << 20);
        // One index slot per ~512 bytes of arena, kept under the 75% load
        // ceiling and rounded up to a power of two for the probe mask.
        let slots = 1 << Math.ceil(Math.log2(Math.max(1024, (arena / 512) / 0.75)));
        return { arenaBytes: arena, l1MaxBytes: l1, indexSlots: Math.min(slots, 1 << 22) };
    }

    // The ordinary entry point: figures out on its own whether this process is
    // the primary (create the arena) or a worker (attach to it), and sizes
    // everything from the machine. createPrimary/attachWorker remain for tests
    // and for callers that want to pin the numbers.
    static open(opts = {}) {
        TurboKV.#assertStorageOptions(opts);   // see createPrimary
        const cluster = require('cluster');
        if (storeReady) {
            // The caller's opts used to be spread AFTER the computed id, so
            // `open({workerId: 0})` in a worker produced a cache that believed it
            // was the primary and took the primary's WRITE path against a
            // read-only mapping -- a SIGBUS on the first set(), not a wedge.
            const id = cluster.isWorker ? cluster.worker.id : 0;
            if (cluster.isWorker && opts.workerId !== undefined && opts.workerId !== id)
                throw new Error(`turbokv: workerId is assigned by cluster in a worker (${id}); ` +
                                `refusing the supplied ${JSON.stringify(opts.workerId)}`);
            const c = new TurboKV({ ...opts, workerId: id });
            // A second cache opened in a worker used to skip the ring entirely and
            // silently run on the slower IPC transport.
            if (cluster.isWorker && submitReady && opts.transport !== 'ipc') c.#useSubmissionRing(submitReady);
            return c;
        }
        const auto = TurboKV.autoSize();
        const arenaBytes = opts.arenaBytes || auto.arenaBytes;
        const indexSlots = opts.indexSlots || auto.indexSlots;
        const o = { l1MaxBytes: auto.l1MaxBytes, ...opts };
        if (cluster.isWorker) {
            // A worker forked BEFORE the primary called open() has no
            // TURBOKV_ARENA, and used to fall through to createPrimary -
            // silently creating a second writable arena and shm_unlinking the
            // primary's. Fail loudly instead.
            if (!process.env.TURBOKV_ARENA)
                throw new Error('turbokv: no arena to attach to. The primary must call ' +
                                'Cache.open()/new Cache() BEFORE forking workers.');
            return TurboKV.attachWorker(process.env.TURBOKV_ARENA, cluster.worker.id, o);
        }
        // A pid-based name leaks: create() unlinks any prior segment, but a
        // crashed run's segment has a name nothing will ever reuse, so it
        // survives until reboot. Deriving the name from the application's
        // identity instead means a restart reclaims its own segment, while two
        // different apps on one host still get different ones.
        const name = opts.name || TurboKV.defaultName();
        process.env.TURBOKV_ARENA = name;      // inherited by workers forked later
        return TurboKV.createPrimary(name, arenaBytes, indexSlots, o);
    }

    // Wire the primary's side of the worker write path. Without this, worker
    // writes never reach L2.
    // Stable per-application shm name, <= 31 chars for darwin's SHM_NAME_MAX.
    // A create() failure is nearly always the backing filesystem being too small
    // rather than anything about the arena itself. On Linux that is /dev/shm,
    // which containers default to 64MB -- so say so instead of "create failed".
    // `storage` and `codec` are one keystroke apart in meaning, and getting them
    // wrong used to cost nothing at construction and everything afterwards: the
    // cache was built, set() reported success, and every get() returned
    // undefined with nothing anywhere to say why. Nine of the ten malformed
    // combinations behaved exactly that way -- `codec: 'direct'` (a string where
    // a codec object belongs, and the single most natural way to write what the
    // author meant), a typo or wrong case in `storage`, a codec missing either
    // half. The tenth failed later, at the first read, with
    // "this[#codec].decode is not a function".
    //
    // Same principle as freeze (decision 26) and the Date/Map/Set mutators
    // (decision 38b): turn silent corruption into a TypeError at the point the
    // mistake was made.
    static #assertStorageOptions(opts) {
        const { codec } = opts;
        const list = STORAGE_MODES.map(m => `'${m}'`).join(', ');

        for (const key of ['storage', 'values']) {
            const v = opts[key];
            if (v === undefined || v === null || STORAGE_MODES.includes(v)) continue;
            const near = typeof v === 'string' &&
                STORAGE_MODES.find(m => m === v.trim().toLowerCase());
            throw new TypeError(
                `turbokv: ${key} must be one of ${list}; got ${JSON.stringify(v)}` +
                (near ? `. Did you mean '${near}'?` : '.'));
        }

        if (codec === undefined || codec === null) return;   // absent: the mode decides

        if (typeof codec === 'string') {
            throw new TypeError(
                `turbokv: codec must be an object with encode and decode functions, ` +
                `but got the string ${JSON.stringify(codec)}. ` +
                (STORAGE_MODES.includes(codec)
                    ? `Did you mean storage: '${codec}'?`
                    : `The storage MODES are named with the storage option: ${list}.`));
        }
        if (typeof codec !== 'object') {
            throw new TypeError(`turbokv: codec must be an object with encode and decode ` +
                                `functions; got ${typeof codec}.`);
        }
        // The declaration says codec is "mutually exclusive with a storage mode
        // that implies one", and it was not enforced: `storage: 'bytes'` plus a
        // codec was accepted and the codec silently discarded -- encode() never
        // called, objects rejected by set(). 'direct' and 'safe' genuinely do
        // take an override (the preset is `opts.codec || <builtin>`); only the
        // no-codec modes contradict one.
        const noCodecMode = ['bytes', 'primitives'];
        for (const key of ['storage', 'values']) {
            if (noCodecMode.includes(opts[key])) {
                throw new TypeError(
                    `turbokv: ${key}: '${opts[key]}' stores values without a codec, ` +
                    `so a codec cannot be used with it. Drop the codec, or use ` +
                    `storage: 'direct' or 'safe' (both accept a codec override).`);
            }
        }

        const missing = ['encode', 'decode'].filter(k => typeof codec[k] !== 'function');
        if (missing.length) {
            throw new TypeError(
                `turbokv: codec is missing ${missing.map(m => `${m}()`).join(' and ')}. ` +
                `A codec is { encode(value) -> string, decode(string) -> value }; ` +
                `TurboKV.JSON_CODEC and TurboKV.V8_CODEC are the built-in ones.`);
        }
    }

    // Cache levels. L1 is this process's Map, L2 the shared arena, L3 the
    // (unbuilt) remote tier. Exposed as constants so callers do not hard-code
    // integers that would change meaning if a level were ever inserted.
    static get L1() { return 1; }
    static get L2() { return 2; }
    static get L3() { return 3; }

    // `minLevel` is a PREFERENCE for the lowest tier a record may occupy, not a
    // contract. An unavailable level is clamped DOWN to the highest one that
    // exists right now, so the data is always stored somewhere -- asking for L3
    // today behaves as L2, and the same call starts using L3 the day it exists.
    // A degraded worker has no L2 at all, and clamps to L1, which is exactly
    // where its writes already go.
    //
    // Clamping is for a level that is valid but not available. A value that is
    // not a level at all is a mistake, and gets the same treatment as every
    // other bad option (decision 48).
    //
    // Levels are NOT contiguous once L3 exists. A worker that has lost its
    // primary has L1 and L3 but no L2, so the clamp cannot be a single ceiling:
    //
    //   state                   L1  L2  L3   minLevel 3 ->   minLevel 2 ->
    //   normal, adapter          o   o   o        3                2
    //   normal, no adapter       o   o   -        2                2
    //   primary dead, adapter    o   -   o        3                1
    //   primary dead, no adapter o   -   -        1                1
    //
    // The rule itself is unchanged: clamp DOWN to the highest level that
    // exists, so data is always stored somewhere.
    #resolveLevel(minLevel) {
        if (minLevel === undefined) return 1;
        if (!Number.isInteger(minLevel) || minLevel < 1 || minLevel > 3) {
            throw new TypeError(
                `turbokv: minLevel must be TurboKV.L1, L2 or L3 (1-3); got ${JSON.stringify(minLevel)}`);
        }
        const hasL2 = !(this.#primaryDead || !storeReady);
        if (minLevel === 3) return this.#l3 ? 3 : (hasL2 ? 2 : 1);
        if (minLevel === 2) return hasL2 ? 2 : 1;
        return 1;
    }

    // Whether a value WRITTEN at this level will occupy a local tier. Level 3
    // is the only level that stores nothing locally (spec 5.2). A degraded
    // handle still caches a write: set() puts the value in L1 before it ever
    // looks at the primary, so a worker that has lost its arena really is
    // holding it.
    #willCacheWrite(level) { return level < 3; }

    // Whether a value READ at this level will occupy a local tier -- a
    // different question, with a different answer on a degraded handle.
    // #promotionBlock refuses EVERY promotion when there is no arena to read
    // the invalidation ring from, so #fillFromL3 is never reached at all, not
    // even for its L1 half. Answering `true` there told the adapter to remember
    // this box for a key it was about to discard -- and the provider this seam
    // exists for turns that into tracking state that is never invalidated,
    // because nothing here ever changes.
    #willCacheRead(level) { return level < 3 && storeReady && !this.#primaryDead; }

    // Spec section 4: `originId` is `native.arenaId()` -- stable per arena,
    // identical in the primary and in every worker mapping it, and changed by a
    // primary restart, which is correct because a restarted primary has an
    // empty cache. It is the loop-suppression field an adapter needs to tell
    // its own writes coming back at it from a subscription apart from another
    // box's, so handing it `undefined` would make `if (originId === mine) skip`
    // silently never fire. `undefined` remains the honest answer for a handle
    // with no arena mapped at all (`attached: false`), where there is no
    // identity to report.
    #originId() {
        if (TurboKV.#arenaId === null) {
            try { TurboKV.#arenaId = native.arenaId(); } catch { return undefined; }
        }
        return TurboKV.#arenaId;
    }

    static #createError(arenaBytes) {
        let hint = '';
        if (process.platform === 'linux') {
            try {
                const st = fs.statfsSync('/dev/shm');
                const total = st.blocks * st.bsize;
                const free = st.bfree * st.bsize;
                if (total < arenaBytes || free < arenaBytes) {
                    const mb = (n) => `${Math.round(n / (1 << 20))}MB`;
                    hint = ` -- /dev/shm holds ${mb(total)} (${mb(free)} free) but the arena needs ` +
                           `${mb(arenaBytes)}. In Docker pass --shm-size=${mb(arenaBytes * 2)}, ` +
                           `or lower the l2Bytes option.`;
                }
            } catch { /* statfs is best-effort; fall back to the bare message */ }
        }
        return `arena create failed${hint}`;
    }

    static defaultName() {
        const crypto = require('crypto');
        const id = (process.argv[1] || process.cwd()) + '|' + (process.env.TURBOKV_ID || '');
        return '/tc-' + crypto.createHash('sha1').update(id).digest('hex').slice(0, 16);
    }

    // Attaches exactly once per worker. Calling this between fork() and the
    // 'online' event previously attached twice, applying every batch twice.
    static install(cluster) {
        // Module-level, not per-call: two install() calls each built their own
        // WeakSet, so both attached a listener to the same worker and every
        // batch was applied twice (one incr became two).
        const wired = installedWorkers;
        const attach = w => {
            if (!w || wired.has(w)) return;
            wired.add(w);
            // One message from this channel, kept so the channel can be named
            // when it goes away. A worker that vanishes mid-clear cannot send
            // the `-` that closes its clear generation, and an open generation
            // makes every process on the box serve L3 misses for every key --
            // so the primary settles what that worker still owed. The MESSAGE
            // rather than its `id`: the id is the caller's to choose and is
            // reused across restarts, so releasing by it settles whatever
            // worker holds that slot NOW, which may be a live successor with a
            // clear of its own in flight. Every message from one attachment
            // names the same attachment, so any one of them will do.
            let last = null;
            w.on('message', m => {
                if (m && m.t === RING_MSG) { TurboKV.drainSubmissions(); return; }
                if (TurboKV.isCacheMessage(m)) {
                    // The NAME, not the message. `m.b` holds the batch --
                    // every key and every encoded value in it -- and keeping
                    // the message would pin the largest batch this worker ever
                    // sent for the life of the channel. Two fields is all
                    // reconciliation reads.
                    last = { id: m.id, n: m.n };
                    TurboKV.applyBatch(m);
                }
            });
            // Both, and idempotent by construction: a disconnect usually
            // precedes an exit, and releaseWorker settles nothing the second
            // time. A worker that merely disconnects can no longer deliver a
            // `-` either. The honest cost of settling on disconnect is in
            // decision 70: a graceful rolling restart disarms the guard while
            // that worker's clear may still be landing in L3.
            const gone = () => { if (last) TurboKV.releaseWorker(last); };
            w.on('exit', gone);
            w.on('disconnect', gone);
        };
        cluster.on('online', attach);
        cluster.on('fork', attach);
        for (const id in cluster.workers) attach(cluster.workers[id]);
    }

    // Drain the submission rings, then keep draining while work remains. Bounded
    // per turn on purpose: draining is synchronous work on the primary's event
    // loop, so an unbounded drain would just move the stall from the worker to
    // the primary -- which is the whole thing this replaces.
    static drainSubmissions(budget = 4096) {
        if (!submitName && !storeReady) return 0;
        let n = 0;
        try { n = native.submitDrain(budget); } catch { return 0; }
        // The primary's OWN L1 must follow the writes it just applied, exactly as
        // applyBatch does for the IPC path. Without this the primary served stale
        // values indefinitely after any worker write -- and the regression test
        // that was supposed to catch it drove applyBatch directly, so it passed
        // while the default path regressed underneath it.
        if (n > 0) TurboKV.#primaryInvalidate();
        if (n >= budget && !TurboKV.#drainScheduled) {
            TurboKV.#drainScheduled = true;
            setImmediate(() => { TurboKV.#drainScheduled = false; TurboKV.drainSubmissions(budget); });
        }
        return n;
    }
    static #drainScheduled = false;

    // Drain the arena's invalidation ring on the PRIMARY. Records carry the
    // writerId that produced them, so entries the primary wrote itself are
    // skipped: its L1 is already correct for those, and dropping them would make
    // its own cache useless for every key it writes.
    static #primaryCursor = 0;
    static #primaryInvalidate() {
        for (let round = 0; round < 64; round++) {
            let r;
            try { r = native.ringRead(TurboKV.#primaryCursor, 1024); } catch { return; }
            if (!r) return;                        // store detached underneath us
            if (r.wrapped) {                       // fell too far behind: flush wholesale
                // Cached values only, exactly as the worker drain does and for
                // the same reason: "I do not know what changed" is a reason to
                // drop what these instances HOLD, never a reason to forget the
                // removals they made themselves. #deletedAt is what stops an
                // in-flight read handing back a key this process deleted.
                for (const c of instances) c.#dropCachedValues();
                TurboKV.#primaryCursor = r.head;
                return;
            }
            const n = r.hashes.length;
            for (let i = 0; i < n; i++) {
                if (r.hashes[i] === 'ffffffffffffffff') { for (const c of instances) c.clearLocal(); continue; }
                if (r.writers[i] === 0) continue;  // our own write
                for (const c of instances) c.#dropByHash(r.hashes[i]);
            }
            TurboKV.#primaryCursor = r.head;
            if (n < 1024) return;                  // caught up
        }
    }

    // Reachable by design, and the only member that is. gcNotify() is a
    // module-scope function declared above the class, so it cannot call a
    // #private -- this is the bridge. Prefixed to say plainly that it is not
    // API, matching the addon's __unsafe* hooks.
    __internalOnGc(used, limit) { this.#onGc(used, limit); }

    // Test-only. Named to say so: level resolution is a pure function of state
    // that is otherwise only observable through a cache miss.
    __unsafeResolveLevel(minLevel) { return this.#resolveLevel(minLevel); }
    __unsafeForcePrimaryDead() { this.#primaryDead = true; }
    // Test-only, and named to say so: whether this handle is still polling for
    // an L2 cap is not otherwise observable, and "a closed cache owns no timer"
    // is a claim that should be checked rather than asserted in a comment.
    // Test-only: the bookkeeping a wrapped ring leaves behind. None of it is
    // observable from outside, and "the marks reconcile, a slice at a time" is
    // a claim that should be checked rather than asserted in a comment.
    // BY KIND, because the whole point of the split is that a delete mark and
    // a write mark are different things: a test that could only see "some
    // mark exists" could not tell the two apart, which is exactly the
    // confusion this state replaced.
    __unsafeMarkState() {
        return {
            pending: this.#pendingDel.size, owed: this.#reconcileOwed,
            deletedAt: this.#deletedAt.size,
            // WHICH entries survived, not just how many: an eviction that kept
            // the newest and one that kept the oldest are the same size and
            // opposite policies.
            deletedAtKeys: [...this.#deletedAt.keys()],
            pendingKeys: [...this.#pendingDel.keys()],
            written: this.#pendingWrite.size,
            writtenKeys: [...this.#pendingWrite.keys()],
        };
    }
    __unsafeCapState() {
        return { caps: this.#l3Caps.size, timer: this.#capTimer !== null };
    }
    // Test-only: runs exactly one retire tick, so a test can drive the guard
    // rather than wait on a 20ms interval it does not control.
    __unsafeRunCaps() { this.#runCaps(); }
    // Test-only: stops the automatic poll so a test can drive it a tick at a
    // time with nothing racing it. Only #noteCap re-arms the timer, so a test
    // that caps nothing after this call keeps it stopped.
    __unsafePauseCaps() { this.#stopCapTimer(); }
    // Test-only: the ring position recorded for one key's removal. See
    // #deletedAt -- the position is the whole content of that guard, and a
    // wrong one is invisible from outside.
    __unsafeDeletedAtOf(key) { return this.#deletedAt.get(key); }
    // Test-only: is a cap outstanding for THIS key. The count alone is fragile
    // in any test that also writes other keys through a failing L3, which is
    // most of them -- every such write defers a cap of its own.
    __unsafeHasCap(key) { return this.#l3Caps.has(key); }
    // Test-only: what the L3 QUEUE still owes for this key, which is the first
    // thing #promotionBlock asks. A test aimed at one of the reasons further
    // down needs to prove the queue is no longer answering, or it pins nothing
    // -- the earlier reason would keep the test green through any change to
    // the later one. See review5_regression_test.js.
    __unsafeOutstandingKind(key) { return this.#queue === null ? undefined : this.#queue.outstandingKind(key); }

    // Test and shutdown helper: resolves when this process has no L3 work left.
    drainL3() { return this.#queue ? this.#queue.drain() : Promise.resolve(); }

    #dropByHash(hash) {
        const k = this.#byHash.get(hash);
        if (k !== undefined) { this.#l1Drop(k); this.#byHash.delete(hash); this.stats.invalidated++; }
    }

    // --- primary death and recovery --------------------------------------
    //
    // A worker whose primary died used to degrade to L1-only PERMANENTLY, even
    // once a new primary was running. Two things force the shape of the fix:
    //
    //   1. A new primary is a NEW SEGMENT. create() unlinks and re-creates, so
    //      the mapping a degraded worker still holds is an orphan that will
    //      never receive another update. Detecting a new primary therefore means
    //      re-opening BY NAME, not watching the header we already hold.
    //   2. The worker must let go FIRST. On Windows CreateFileMappingA fails
    //      with ERROR_ALREADY_EXISTS while any process holds a handle, so a
    //      worker clinging to a dead arena prevents a new primary from ever
    //      starting. Detaching is mandatory, not hygiene - and free, since a
    //      degraded worker serves L1 only and never touches the arena.
    //
    // State is on the CLASS, not the instance: `native` is process-global, so
    // one detach/attach drives every cache in the process.
    static #degraded = false;
    static #recoverTimer = null;
    static #lastHb = -1;

    static #degrade(age) {
        if (TurboKV.#degraded || isPrimaryProcess) return;
        TurboKV.#degraded = true;
        for (const c of instances) {
            c.#setDead(true, `primary heartbeat is ${age === -2 ? 'dated in the future' : age + 'ms old'}; serving L1 only`);
        }
        // Give the ring slot back before unmapping, or it stays owned by this pid
        // in a segment nobody will reclaim.
        try { native.submitRelease(); } catch { /* not using the ring */ }
        try { native.submitDestroy(); } catch { /* not created */ }
        submitReady = null;
        try { native.detach(); } catch { /* already gone */ }
        storeReady = false;
        TurboKV.#lastHb = -1;
        if (TurboKV.#recoverTimer || !attachedName) return;
        TurboKV.#recoverTimer = setInterval(() => TurboKV.#tryRecover(), 1000);
        if (TurboKV.#recoverTimer.unref) TurboKV.#recoverTimer.unref();
    }

    static #tryRecover() {
        if (!attachedName) return;
        if (!native.attach(attachedName)) return;              // no primary yet
        // A plausible age proves nothing: a dead primary's last stamp still looks
        // recent until staleMs elapses, and a freshly created arena starts with a
        // fresh one. Require the heartbeat to ADVANCE between two polls, which
        // only a live writer can do. The primary stamps every 500ms, so a healthy
        // one passes within two ticks.
        const hb = native.heartbeatRaw();
        const age = native.heartbeatAgeMs();
        if (hb === TurboKV.#lastHb || age < 0 || age > TurboKV.#staleMsFor()) {
            TurboKV.#lastHb = hb;
            try { native.detach(); } catch {}
            storeReady = false;
            return;
        }
        clearInterval(TurboKV.#recoverTimer);
        TurboKV.#recoverTimer = null;
        TurboKV.#degraded = false;
        storeReady = true;
        const id = native.arenaId();
        const sameArena = TurboKV.#arenaId !== null && id === TurboKV.#arenaId;
        TurboKV.#arenaId = id;
        for (const c of instances) c.#recovered(sameArena);
    }

    // Half the staleness budget, so "alive" is a stricter test than "dead" was.
    // The asymmetry is what stops a primary that stalls periodically from
    // flapping every worker's L1 back and forth.
    static #staleMsFor() {
        for (const c of instances) return c.#ownStaleMs() / 2;
        return 2500;
    }
    static #arenaId = null;

    #setDead(dead, msg) {
        this.#primaryDead = dead;
        // Drop the ring index with it. Without this a degraded set() still went
        // to submitSet, failed, and overwrote lastError with "submission ring
        // full" -- so the one signal that a worker was serving L1 only vanished
        // on its very next write, and the shed counter blamed backpressure for
        // what was actually a dead primary.
        if (dead) this.#ringIdx = -1;
        if (msg) this.lastError = msg;
    }

    /** Whether this handle has lost its primary and is serving L1 only. */
    get primaryDead() { return this.#primaryDead; }
    #ownStaleMs() { return this.#staleMs; }
    #usesRing() { return this.#ringIdx >= 0; }
    #recovered(sameArena) {
        // Everything this worker believed about the arena is now suspect: it
        // missed every invalidation while detached, and an unapplied delete is a
        // lost write rather than a pending one.
        this.clearLocal();
        this.#cursor = native.ringHead();      // not 0: replaying a ring we already flushed for is waste
        this.#ringIdx = -1;
        if (this.#transportOpt !== 'ipc' && attachedName) this.#useSubmissionRing(attachedName + '_sub');
        this.#primaryDead = false;
        this.lastError = null;
        this.stats.recoveries = (this.stats.recoveries || 0) + 1;
        this.stats.lastRecovery = { sameArena, at: Date.now() };
    }

    /** Guard cadence: evaluations performed, finalizer signals dropped by the
     *  debounce, and the interval in force. */
    static heapGuardPace() { return gcPace(); }

    static submitStats() { try { return native.submitStats(); } catch { return null; } }

    static createPrimary(name, arenaBytes, indexSlots, opts = {}) {
        // BEFORE any native call. The constructor validates too, but by the
        // time it runs, create()/attach() have already made a shm segment that
        // nothing in the throw path unlinks -- verified: the arena outlived the
        // process that threw. Validation must precede the side effect.
        TurboKV.#assertStorageOptions(opts);
        if (!native.create(name, arenaBytes, indexSlots, 2)) throw new Error(TurboKV.#createError(arenaBytes));
        // Compression is off unless the caller explicitly asks AND the addon was
        // built with LZ4. Measured a bad trade (see DESIGN.md), so it is neither
        // the default nor a build dependency.
        if (opts.compress === true) {
            if (!native.hasLz4())
                throw new Error('turbokv: compress:true requires an addon built with ' +
                                'LZ4 (node-gyp configure build --turbokv_lz4=1)');
            native.setCompressMin(opts.compressMinBytes || 1024, opts.compressAccel || 1);
        } else {
            native.setCompressMin(1 << 30, 1);          // effectively never
        }
        storeReady = true;
        isPrimaryProcess = true;
        // Submission rings. One per worker slot; a worker claims a slot by CAS on
        // the ring's owner field, so slots are ASSIGNED rather than passed in --
        // which makes a worker/primary id collision structurally impossible
        // instead of merely rejected.
        if (opts.transport !== 'ipc') {
            const rings = opts.submitRings || 32;
            const ringBytes = opts.submitRingBytes || (1 << 20);
            if (!native.submitCreate(name + '_sub', rings, ringBytes))
                throw new Error('turbokv: submission ring segment could not be created');
            submitName = name + '_sub';
        }
        const c = new TurboKV({ ...opts, workerId: 0 });
        c.#startMaintenance(opts);
        return c;
    }
    static attachWorker(name, workerId, opts = {}) {
        TurboKV.#assertStorageOptions(opts);   // see createPrimary
        // Worker ids must be >= 1. `#id === 0` is how every method recognises the
        // primary, so a worker attached as 0 takes the primary's WRITE path and
        // calls into the native writer against a read-only mapping. That does not
        // throw and does not crash -- it wedges the process permanently on the
        // first set(), with the event loop blocked and no diagnostic. Zero-based
        // worker ids are the natural thing for a caller to write, so this has to
        // be a loud error rather than a documented footnote.
        // Coerce numeric strings: ids routinely arrive from environment
        // variables. Everything else must already be a positive integer.
        const wid = (typeof workerId === 'string' && /^[0-9]+$/.test(workerId)) ? Number(workerId) : workerId;
        if (!Number.isInteger(wid) || wid < 1)
            throw new Error(`workerId must be an integer >= 1 (0 is reserved for the primary), got ${JSON.stringify(workerId)}`);
        if (!native.attach(name)) throw new Error('arena attach failed');
        storeReady = true;
        attachedName = name;
        const c = new TurboKV({ ...opts, workerId: wid });
        if (opts.transport !== 'ipc') c.#useSubmissionRing(name + '_sub');
        return c;
    }

    // --- L1 --------------------------------------------------------------
    // `ownAt` marks an entry this WORKER wrote and whose submission the primary
    // has not applied yet, AND SAYS WHERE THE RING WAS when it was written.
    // See #promotionBlock: it is the self-mark for an ordinary set, symmetric
    // with what #pendingDel does for a delete, and it lives ON THE ENTRY
    // deliberately. The mark exists only to stop an overlapping L3 read
    // overwriting this value, so it is worth exactly as long as the value is:
    // the invalidation record for our own write drops the entry (#drain does
    // not skip our own records) and takes the mark with it, an eviction or an
    // expiry takes it too -- and in both of those cases there is no longer
    // anything of ours to protect. A separate map would outlive the value and,
    // for a write the ring then shed, would outlive it forever, blocking every
    // later promotion of that key in this worker.
    //
    // A POSITION RATHER THAN A FLAG, for the reason the removal marks carry
    // one: the entry used to be dropped by ANY record for its hash, including
    // one written BEFORE our write and not yet drained here. `set(k, NEW)`
    // then `get(k)` on the same worker then returned another process's OLDER
    // value -- deterministically, whenever somebody else had written that key
    // since this worker last drained -- because the drain that read ran found
    // the earlier record, dropped our entry, and refilled L1 from the arena,
    // which is exactly the value our write supersedes. -1 means "not ours".
    #l1Put(key, v, hash, encodedLen, expiresAt = 0, ownAt = -1) {
        // In primitives mode the cost is known exactly; otherwise it is the
        // encoded length scaled by heapFactor, which is only an estimate.
        const bytes = this.#noCodec
            ? native.primBytes(v) + native.primBytes(key) + 64
            : (key.length + encodedLen + 64) * this.#heapFactor;
        const prev = this.#l1.get(key);
        if (prev) this.#l1Bytes -= prev.bytes;
        this.#l1.set(key, { v, bytes, hits: 1, exp: expiresAt, hash, ownAt });
        this.#byHash.set(hash, key);
        this.#l1Bytes += bytes;

        // FIFO with second chance: Map preserves insertion order, so the oldest
        // entry is first. An entry that has been read again gets one reprieve.
        //
        // The reprieve MUST be budgeted. A re-queue does not free any bytes, so
        // it does not advance the loop condition: on a workload where most
        // residents have been re-read, one insert walks the entire map putting
        // every entry to the back before it can evict anything. Measured O(n) in
        // L1 entry count -- 3141ns at 2MB, 8257ns at 8MB, 23545ns at 32MB, then
        // 464ns at 128MB where the set fits and eviction never runs. The native
        // arena bounds its equivalent loop (store_ops.h g_secondChanceBudget);
        // this one did not. Past the budget an entry is evicted despite its bit.
        let reprieves = L1_SECOND_CHANCE_BUDGET;
        while (this.#l1Bytes > this.#l1Max) {
            const oldest = this.#oldestEntry();
            if (oldest === undefined) break;
            const [k, e] = oldest;
            this.#l1.delete(k);
            if (e.hits > 1 && reprieves > 0) {
                reprieves--; e.hits = 1; this.#l1.set(k, e); continue;      // re-queue
            }
            this.#l1Bytes -= e.bytes;
            if (e.hash !== undefined) this.#byHash.delete(e.hash);          // was leaked
        }
    }
    // Oldest live entry, in insertion order.
    //
    // This used to be `this.#l1.entries().next()`. V8's OrderedHashMap does not
    // compact on delete -- it tombstones and only rehashes later -- so a FRESH
    // iterator must skip the entire accumulated run of holes at the front on
    // every single call. Using a Map as a FIFO queue that way is O(n) per
    // eviction. Measured in isolation (no cache involved), popping the oldest
    // key from a steady-size Map: 1671ns at 8k entries, 6173ns at 32k, 20018ns
    // at 128k with a fresh iterator, against 89/94/115ns with a retained one --
    // 174x at 128k. That was the whole reason a cold-read workload got *slower*
    // as L1 grew (2832ns at 2MB, 23664ns at 32MB) and then snapped back to
    // 395ns at 128MB, where the set fits and eviction never runs.
    //
    // A retained iterator is safe here: Map iterators are live, so entries
    // appended at the tail after it was created are still visited, and entries
    // deleted ahead of it are skipped. It only needs recreating once exhausted.
    #oldestEntry() {
        for (let attempt = 0; attempt < 2; attempt++) {
            if (this.#l1Iter === null) this.#l1Iter = this.#l1.entries();
            const r = this.#l1Iter.next();
            if (!r.done) return r.value;
            this.#l1Iter = null;          // ran off the end; restart from the front
        }
        return undefined;                 // genuinely empty
    }

    // Runs right after a GC, so used_heap_size is the LIVE set, not live+garbage.
    // The byte budget is an estimate; this is not.
    #onGc(used, limit) {
        if (used === undefined) {
            const h = v8.getHeapStatistics();
            used = h.used_heap_size; limit = h.heap_size_limit;
        }
        this.liveHeapFraction = used / limit;
        if (this.liveHeapFraction < this.#guardMax) return;
        const target = this.#l1Bytes * (1 - this.#guardShed);
        for (const [k, e] of this.#l1) {
            if (this.#l1Bytes <= target) break;
            this.#l1Bytes -= e.bytes;
            this.#l1.delete(k);
            if (e.hash !== undefined) this.#byHash.delete(e.hash);
        }
        this.stats.heapShed = (this.stats.heapShed || 0) + 1;
    }

    #l1Drop(key) {
        const e = this.#l1.get(key);
        if (!e) return;
        this.#l1Bytes -= e.bytes;
        this.#l1.delete(key);
        if (e.hash !== undefined) this.#byHash.delete(e.hash);   // was leaked
    }

    // --- coherence -------------------------------------------------------
    // The primary is the sole writer, so its own L1 is authoritative and it has
    // nothing to drain. Workers check one hot counter, which is usually
    // unchanged, before paying for a real drain.
    // Is the primary still alive? Returns true if this call degraded.
    //
    // Separate from #drain because WRITES must run it too. It used to live
    // inside #drain, which only get() and has() call, so a worker that only
    // ever writes -- a cache warmer, a populate-only job, a perfectly ordinary
    // shape -- never noticed its primary had died. It kept pushing into an
    // orphaned segment, set() kept returning true, stats.sent kept climbing,
    // and because nothing degraded it there was no path by which it could ever
    // recover. Nothing is cheap enough to do per-write except a clock read, so
    // the native heartbeat call stays gated at 500ms.
    #checkPrimary() {
        if (this.#id === 0 || this.#primaryDead) return false;
        const t = monoMs();
        if (t - this.#lastStaleCheck < 500) return false;
        this.#lastStaleCheck = t;
        const age = native.heartbeatAgeMs();
        // -1 is "never stamped", not "dead". A primary running with
        // maintenance:false never stamps, and treating that as death degraded
        // every worker permanently on its second read. Say so once, then stop
        // asking -- without a heartbeat there is no liveness signal to act on.
        if (age === -1) {
            if (!this.#noHeartbeatWarned) {
                this.#noHeartbeatWarned = true;
                this.lastError = 'the primary has never stamped a heartbeat ' +
                                 '(maintenance:false?); primary-death detection is off';
            }
            return false;
        }
        if (age === -2 || age > this.#staleMs) { TurboKV.#degrade(age); return true; }
        return false;
    }

    #drain() {
        if (this.#id === 0) return;
        // Degraded means the arena is UNMAPPED (we must let go so a new primary
        // can create one), so every native arena call below would return
        // undefined and the ring read would then dereference it. The recovery
        // timer owns re-attaching; until it succeeds this cache is L1-only.
        if (this.#primaryDead || !storeReady) return;
        // A dead primary cannot invalidate anything, so the arena is frozen and
        // increasingly stale. Degrade to L1-only rather than serve it silently.
        // Staleness is a question about TIME, so check it on a clock rather than
        // every 256 drains. Tied to the operation count, a worker doing three
        // reads a second took 85 seconds to notice a dead primary, and one that
        // went quiet and came back served stale data on its first read. monoMs()
        // is ~21ns against the native ringHead() call this function already
        // makes, so the check is free at any call rate.
        if (this.#checkPrimary()) return;          // just degraded; the arena is unmapped
        // Before the early-out below, not after it: a reconciliation left over
        // from a wrap has to make progress even on the drains that find the
        // ring quiet, which is most of them.
        // native.ringHead(), not this.#cursor: the record path computes the
        // exact position of the record it is retiring (cursor + i + 1), and a
        // slice running here -- BEFORE that scan -- would otherwise stamp a
        // strictly earlier position on the same kind of mark, preempting the
        // precise path with a looser answer. The head is read on the next line
        // anyway, so this costs nothing.
        const head = native.ringHead();
        if (this.#reconcileOwed > 0) this.#reconcileRemovals(head);
        if (this.#writeOwed > 0) this.#reconcileWrites();
        if (head === this.#cursor) return;
        const r = native.ringRead(this.#cursor, 512);
        if (!r) return;                            // detached mid-drain
        if (r.wrapped) {                       // fell too far behind: flush wholesale
            // CACHED VALUES ONLY, and then RECONCILE what this process owes.
            //
            // A wrapped ring means "I do not know what changed", which is a
            // reason to drop what this process is HOLDING and not a reason to
            // forget what it itself REMOVED: clearing #pendingDel here handed a
            // worker whose own delete was still pending straight back to L2's
            // copy of that key, which is a stale value -- the one failure mode
            // this design does not accept, and reachable now that an absent-key
            // delete appends a record too, since a fan-out invalidation
            // workload takes an idle worker from zero ring traffic to full rate
            // and ~0.33s of that laps a 16MB arena's ring.
            //
            // But KEEPING them blindly is the other half of the same mistake,
            // because the record that would have cleared a mark is exactly what
            // the wrap discarded: a delete the primary HAD applied stayed
            // marked forever, and that key then answered undefined from every
            // tier, L3 included, with the adapter never asked -- F5 again,
            // through a different door, and more reachable than before for the
            // same reason.
            //
            // So ask the arena, which is the one authority a wrap does not
            // destroy. A key it no longer holds is a removal that HAS been
            // applied: drop the mark, and record where it landed so a read
            // already in flight is still refused (see #deletedAt). A key it
            // still holds is a removal still outstanding: keep the mark. Costs
            // one has() -- which copies no value -- per mark, bounded by
            // PENDING_MARK_MAX, on an event that is rare by construction.
            //
            // The residue is narrow and in the safe direction: a key deleted,
            // applied, and then RECREATED by someone else inside the wrapped
            // region reads as "still held", so its mark survives and this
            // worker misses on it until the next record for that key. A miss,
            // never a stale value.
            //
            // THAT QUESTION IS THE REMOVAL MARK'S, AND ONLY ITS. Asked of a
            // WRITE mark it reads exactly backwards: a write's landing IS
            // "the key is held", so `has() === true` kept the mark of a write
            // that had already been applied, and that key then answered
            // undefined from every tier -- L3 included, never asked -- on this
            // worker, permanently. `has() === false` says no less: the arena
            // holds nothing for the key, so there is no older value for the
            // mark to protect anyone from and a read misses anyway.
            //
            // SO A WRITE MARK IS NOT ASKED OF THE ARENA AT ALL -- it is asked
            // of the SUBMISSION RING'S CONSUMER INDEX, which answers the
            // precise question instead of a proxy for it. The ring is SPSC
            // with this worker as the producer, so the primary's `tail` past
            // our own record means it drained and applied that record, and
            // nothing else does. Released iff it did; KEPT otherwise -- and a
            // kept mark is re-examined on every later drain (see
            // #reconcileWrites), so it cannot become the permanently
            // unreadable key above. A wrap used to release them all, which is
            // where the residue below came from.
            //
            // THE RESIDUE THAT IS LEFT, and the honest size of it. The IPC
            // fallback has no consumer index: a batch handed to process.send
            // is acked by nothing, so "did the primary apply my write" is
            // genuinely unanswerable there and the mark is released, exactly
            // as it used to be for both transports. A `minLevel: 2` write an
            // IPC worker has submitted and the primary has not applied can
            // therefore be read from L2 at its PREVIOUS value after a wrap --
            // and it is not one read: it persists until something else
            // replaces or invalidates that key. The threshold is `ringCap`
            // records appended by the primary, NOT 65536: store.h sizes the
            // ring as min(65536, 4% of the arena / 16 bytes), rounded down to
            // a power of two with a floor of 8192 -- so 32768 at the 16MB
            // default and 8192 on a 2MB arena. No failure is needed to reach
            // it: a synchronous write burst on the primary turns no event
            // loop, so its doorbell never fires and its 500ms backstop never
            // runs, and ~8300 records on a small arena is enough. README says
            // the same thing where it lists what to watch for.
            //
            // #deletedAt and #l3Caps need no reconciliation: the first is
            // compared against ring POSITIONS, which are monotonic and which
            // #promotionBlock refuses outright while the ring is wrapped, and
            // the second resolves against the arena on its own timer, which a
            // wrap tells it nothing about.
            this.#dropCachedValues();
            this.#writeOwed = this.#pendingWrite.size;
            if (this.#writeOwed) this.#reconcileWrites();
            this.#reconcileOwed = this.#pendingDel.size;
            if (this.#reconcileOwed) this.#reconcileRemovals(r.head);
            this.#cursor = r.head;
            return;
        }
        for (let i = 0; i < r.hashes.length; i++) {
            if (r.hashes[i] === 'ffffffffffffffff') {     // clearAll sentinel
                this.clearLocal(); this.#cursor = r.head; return;
            }
            // Own records are NOT skipped. The old "our own write, L1 is already
            // correct" shortcut was false whenever L1 had been refilled from L2
            // between queuing and apply - a worker that deleted a key then read
            // it in the same tick kept serving the deleted value forever. The
            // cost of dropping our own entry is one L2 refetch.
            // AT OR PAST THE POSITION THE MARK WAS TAKEN AT. A record earlier
            // than that cannot be our operation -- it was already on the ring
            // when we marked -- and clearing on it served the deleted value.
            // See PendingMarks.
            const pos = this.#cursor + i;
            // EACH KIND ASKED SEPARATELY, and answered differently. A removal
            // that has landed has to be handed to #deletedAt so an L3 read
            // already in flight cannot resurrect it; a WRITE that has landed
            // is not a removal and must not go there -- doing so made an
            // in-flight getAsync answer undefined and counted the set as
            // `l3DeletedWhileReading`, a counter named for the opposite event.
            const pk = this.#pendingDel.matchAt(r.hashes[i], pos);
            if (pk !== undefined) {              // the primary applied our delete; L2 is authoritative
                this.#pendingDel.release(pk);
                // The mark stops answering here, so the promotion guard needs
                // the position it stopped at: a read that started before this
                // record must still be refused, or it would promote -- and
                // return -- the value this worker removed. See #deletedAt.
                this.#noteDeleted(pk, pos + 1);
            }
            const wk = this.#pendingWrite.matchAt(r.hashes[i], pos);
            if (wk !== undefined) this.#pendingWrite.release(wk);   // our write landed; L2 is authoritative
            const k = this.#byHash.get(r.hashes[i]);
            if (k !== undefined) {
                // A RECORD OLDER THAN OUR OWN WRITE LEAVES IT ALONE. Dropping
                // the entry here refills L1 from the arena on the next read,
                // and for a write of ours the primary has not applied yet
                // that is precisely the value our write supersedes. See
                // #l1Put's `ownAt`.
                const e = this.#l1.get(k);
                if (e !== undefined && e.ownAt >= 0 && pos < e.ownAt) continue;
                this.#l1Drop(k); this.#byHash.delete(r.hashes[i]); this.stats.invalidated++;
            }
        }
        this.#cursor = r.head;
    }

    // Opt into the shared-memory write path. Falls back silently to IPC when the
    // segment is absent (a primary from before this existed) or when every ring
    // is already claimed -- the cache still works, just on the slower transport.
    #useSubmissionRing(segName) {
        try {
            if (!native.submitOpen(segName)) return false;
            const idx = native.submitClaim();
            if (idx < 0) { this.lastError = 'no free submission ring; falling back to IPC'; return false; }
            this.#ringIdx = idx;
            this.#ringMaxValue = native.submitMaxValue();
            submitReady = segName;
            // Seed the arena identity at ATTACH. Without this the first
            // recovery always reported sameArena:false -- including for a plain
            // stall of the very same primary, which is the most common outage
            // and precisely the case the field exists to make readable.
            if (TurboKV.#arenaId === null) {
                try { TurboKV.#arenaId = native.arenaId(); } catch { /* not attached */ }
            }
            return true;
        } catch { return false; }
    }

    get transport() { return this.#ringIdx >= 0 ? 'shm' : 'ipc'; }

    // --- public API (synchronous) ---------------------------------------
    get(key, opts) {
        if (!isStringKey(key)) return undefined;
        // Resolved before the L1 lookup so an invalid value throws whether or not
        // the key happens to be resident.
        const minLevel = opts === undefined ? 1 : this.#resolveLevel(opts.minLevel);
        this.#drain();
        // An operation of our own has not reached the arena yet; serving L2
        // here would hand back the value this process just deleted or just
        // replaced. EITHER KIND, and #unappliedHere says so in as many words.
        if (hasLoneSurrogate(key)) return undefined;   // cannot have been stored
        if (this.#unappliedHere(key)) return undefined;
        const e = this.#l1.get(key);
        // TTL must be enforced in L1 too. The arena expires lazily on read, but
        // an L1 hit never reaches the arena, so without this an expired value
        // is served indefinitely from L1.
        if (e !== undefined && e.exp && e.exp <= monoMs()) { this.#l1Drop(key); }
        else if (e !== undefined) {
            e.hits++; this.stats.l1Hits++;
            // Binary is copied on every read (decision 7): it is mutable, and
            // L1 hands out the same entry to every caller in this process.
            if (Buffer.isBuffer(e.v)) return Buffer.from(e.v);
            // l1Decoded: hand back the cached object (free, but shared/frozen).
            // Otherwise decode per read, giving each caller a fresh mutable one.
            return this.#l1Decoded ? e.v : this.#codec.decode(e.v);
        }
        if (this.#primaryDead) { this.stats.misses++; return undefined; }
        const raw = native.get(key);
        if (raw === undefined) { this.stats.misses++; return undefined; }
        // A value L3 refused, whose L2 cap this worker has asked for and the
        // primary has not applied yet, past the deadline that cap named. See
        // #capExpired: serving it here would be serving, past its own expiry,
        // the value decision 68 promises the box converges away from.
        if (this.#l3Caps.size && this.#capExpired(key, raw)) { this.stats.misses++; return undefined; }
        this.stats.l2Hits++;
        // Carry the arena entry's expiry into L1. Without this the refilled L1
        // entry had no TTL at all, so any expiring value read once through L2
        // became immortal in that worker.
        const rem = native.lastTtlRemainingMs();
        const expMs = rem ? monoMs() + rem : 0;
        // minLevel > L1 means "do not let this occupy my L1". The value is still
        // returned; only its residency changes. Measured worth having: one scan
        // of a cold keyspace evicts 93.7% of a hot working set, and costs 3.8x
        // more than the same scan that does not promote.
        if (this.#codec && !this.#l1Decoded) {          // safe mode: cache the encoded form
            if (minLevel === 1) this.#l1Put(key, raw, native.hashKey(key), raw.length, expMs);
            return this.#codec.decode(raw);
        }
        let v = raw;
        if (this.#codec) { v = this.#codec.decode(raw); if (this.#freeze) TurboKV.deepFreeze(v); }
        if (minLevel === 1) this.#l1Put(key, v, native.hashKey(key), this.#noCodec ? 0 : raw.length, expMs);
        // The L2 path used to return the very object it just placed in L1, so a
        // caller mutating a binary result corrupted the cached copy.
        return Buffer.isBuffer(v) ? Buffer.from(v) : v;
    }

    // L1 -> L2 -> L3, filling downward per minLevel. Simultaneous misses on one
    // key inside this process share a single L3 request, so the herd is bounded
    // by the number of processes rather than by the request rate.
    async getAsync(key, opts) {
        const local = this.get(key, opts);
        if (local !== undefined) return local;
        if (!this.#l3) return undefined;
        // `get` already rejected a non-string key by returning undefined; going
        // on to L3 with it would hand the adapter -- and hashKey -- something
        // neither of them accepts.
        if (!isStringKey(key)) return undefined;
        // A key THIS process deleted, whose removal some tier below has not
        // applied yet. `get` answers undefined for it deliberately; going to L3
        // would both resurrect the key locally and make the two forms disagree
        // about a delete this caller was already told had succeeded.
        if (this.#deletedHere(key)) return undefined;
        const level = opts === undefined ? 1 : this.#resolveLevel(opts.minLevel);
        // Keyed by LEVEL AND KEY, not by key alone. The herd only shares a
        // request when the requests are the same request: two concurrent reads
        // of one key at different `minLevel`s differ in what they fill and in
        // the `willCache` they tell the adapter, so joining the second to the
        // first silently gave it the first caller's placement -- a
        // `minLevel: L3` read promoting into L1 because someone else asked
        // first, or the reverse. ` ` cannot appear in a level, so the two
        // parts cannot run together into an ambiguous id.
        // A clear -- from THIS instance or a sibling sharing the same arena
        // (decision 64) -- is on its way to L3 but has not landed yet. Reading
        // through would hand back, and promote into the SHARED L2, exactly the
        // value the clear was meant to remove, and every instance in the
        // process would see the resurrection, not only the one that happened
        // to read it. Miss instead; that is the failure mode this system is
        // built around, a resurrected value is not. Process-wide, not
        // per-instance: see l3ClearsInFlight -- and CLUSTER-wide, via the
        // arena's clear generation, because the arena this would promote into
        // is shared with processes whose own counter says nothing about a
        // clear issued over here.
        //
        // ASKED BEFORE THE HERD SHARING BELOW, and not inside #fetchFromL3.
        // Sharing hands this caller a promise created for an EARLIER read,
        // whose own guard ran before the clear was issued -- so a getAsync
        // issued AFTER clearAll() joined a read issued before it and was
        // answered with the pre-clear value, while `get` for the same key
        // answered undefined. Decision 70's "until it lands this process
        // serves misses" has no exception for a caller that happened to
        // arrive while someone else was already asking.
        if (l3ClearsInFlight > 0 || this.#sharedClearsInFlight() > 0) {
            this.stats.l3Misses = (this.stats.l3Misses || 0) + 1; return undefined;
        }
        const shareId = level + ' ' + key;
        const shared = this.#inflight.get(shareId);
        if (shared !== undefined) return shared;

        const p = this.#fetchFromL3(key, level).finally(() => this.#inflight.delete(shareId));
        this.#inflight.set(shareId, p);
        return p;
    }

    async #fetchFromL3(key, level) {
        // Marked BEFORE the await: everything appended to the invalidation ring
        // from here on happened while this read was in flight.
        const mark = storeReady && !this.#primaryDead ? native.ringHead() : -1;
        // Sampled for the same reason and at the same moment: a clear that both
        // begins and lands during the await leaves the in-flight count back at
        // zero, and only the generation still shows it happened.
        const clearMark = mark < 0 ? 0 : native.l3ClearGen();
        let rec;
        // Bounded, not merely awaited. An adapter that neither resolves nor
        // rejects would otherwise leave this promise pending forever -- and
        // because getAsync shares it through #inflight, the `.finally` that
        // removes the entry would never run either, so EVERY later read of this
        // key in this process would be handed the same dead promise even after
        // L3 came back. A hang is the same outage as a throw; it takes the same
        // path.
        try { rec = await this.#queue.deadline(this.#l3.get(key, { willCache: this.#willCacheRead(level) }), this.#l3RetryMs, 'get'); }
        catch (e) {
            this.stats.l3Misses = (this.stats.l3Misses || 0) + 1;
            this.#reportL3(e, { kind: 'get', key });
            return undefined;
        }
        if (rec === undefined || rec === null) { this.stats.l3Misses = (this.stats.l3Misses || 0) + 1; return undefined; }
        // DECODED BEFORE ANYTHING IS STORED. The arena took whatever bytes the
        // adapter handed over and the decode ran afterwards, on the way back to
        // the caller -- so an adapter returning a value this cache cannot
        // represent (a Buffer where the codec expects its own encoding, say)
        // left those bytes in the SHARED L2 and then threw. Every later
        // synchronous `get` of that key, in every process on the box, threw the
        // decode error, and went on throwing until the entry aged out. Validate
        // first: an unusable value is an L3 miss, reported through the same
        // listener as any other adapter failure, and nothing is written.
        let accepted;
        try { accepted = this.#acceptFromL3(rec.value); }
        catch (e) {
            this.stats.l3Misses = (this.stats.l3Misses || 0) + 1;
            this.stats.l3BadValues = (this.stats.l3BadValues || 0) + 1;
            this.#reportL3(e, { kind: 'get', key });
            return undefined;
        }
        this.stats.l3Hits = (this.stats.l3Hits || 0) + 1;
        const why = this.#promotionBlock(key, mark, clearMark);
        if (why) {
            this.stats[why] = (this.stats[why] || 0) + 1;
            // TWO of the reasons change the ANSWER, not merely the placement,
            // and they change it for the same cause: a removal this cluster has
            // already committed to and L3 has not applied yet.
            //
            // A delete of OUR OWN is the first: `get` already says undefined
            // for this key, so handing back the value the delete is on its way
            // to removing would make the two forms disagree about this
            // process's own state. A clear is the second, and it is that same
            // disagreement one step wider -- the clear emptied L1 and L2, here
            // and in every other process, so `get` says undefined for EVERY
            // key; a `getAsync` that answered with what L3 still holds would
            // contradict it, and would contradict the entry guard above, which
            // has been serving misses for those same keys since the clear was
            // issued. "Until it lands this process serves misses" (decision 70)
            // does not get an exception for reads that happened to start first.
            // Every other reason leaves the caller
            // with what L3 returned -- blocking changes only what is stored
            // locally, never what this caller observes.
            if (why === 'l3DeletedWhileReading' || why === 'l3ClearedWhileReading') return undefined;
            return this.#handOut(accepted.value);
        }
        this.#fillFromL3(key, rec, level, accepted);
        return this.#handOut(accepted.value);
    }

    // Is this process still holding an L3 operation for `key` that L3 has not
    // applied yet -- a delete, specifically? Two places can be holding one and
    // neither covers the other: a WORKER's delete waits in #pendingDel until
    // the primary applies it to the arena, while on the PRIMARY the arena is
    // updated immediately and there is no #pendingDel at all -- the delete is
    // outstanding only in the L3 queue, which is where the answer has to come
    // from. Free when there is no adapter: #queue is null and no lookup runs.
    //
    // THE REMOVAL MARK ONLY. Its callers -- getAsync and hasAsync -- refuse to
    // ask L3 at all, which is right for a key this caller was just told is
    // gone and wrong for one it has just WRITTEN: L3 is where that write is
    // going, and the write mark reading as a delete here is what made a
    // `minLevel: 2` set unreadable through the async forms.
    #deletedHere(key) {
        if (this.#pendingDel.has(key)) return true;
        return this.#queue !== null && this.#queue.outstandingKind(key) === 'delete';
    }

    // THE ONE QUESTION THAT GENUINELY WANTS EITHER MARK: can this process read
    // L2 for `key` at all? A removal of ours that has not landed means the
    // arena still holds the value we removed; a write of ours that has not
    // landed means it still holds the value we replaced. Both are values this
    // process has already told its caller are gone, so both must miss -- and
    // a miss is the failure mode this system is built around.
    //
    // Named, rather than two `has` calls at each site, so that "either" is a
    // statement someone made on purpose and not the accident of a single
    // overloaded mark.
    #unappliedHere(key) {
        return this.#pendingDel.has(key) || this.#pendingWrite.has(key);
    }

    // WHERE THE INVALIDATION RING IS, RIGHT NOW, for a mark about to be taken
    // or for the self-mark on an L1 entry. Read BEFORE the operation is
    // published, never after: the primary can drain our submission and append
    // its record between the push and this read, and a mark stamped past its
    // own record is one nothing can ever clear.
    //
    // Zero when there is no head to read -- degraded, detached. Every record
    // then answers for the mark, which is the behaviour the position replaced
    // and the safe direction: a mark dropped early costs one possible stale
    // read, a mark never dropped makes the key unreadable from every tier.
    #ringHeadNow() {
        if (!storeReady || this.#primaryDead) return 0;
        try { return native.ringHead(); } catch { return 0; }
    }

    // See the wrapped branch of #drain. Keeps only the REMOVAL marks the arena
    // still justifies, and hands the rest to #deletedAt so a read already in
    // flight cannot promote what they were protecting. Write marks have their
    // own pass below, because they have their own question.
    #reconcileRemovals(head) {
        // A BUDGETED SLICE, resumed on later drains. #drain() runs from get()
        // AND has(), and a ring that keeps lapping between reads makes every
        // read wrap again -- so an unbudgeted pass over PENDING_MARK_MAX marks,
        // measured at 0.28ms, would be paid PER READ: ~28% of a core at 1k
        // reads/s. This is the only unbudgeted arena loop left, and #runCaps
        // already established the shape.
        //
        // A mark examined and KEPT is moved to the back of the set, unchanged
        // (see touch), so the next slice continues where this one stopped
        // rather than re-examining the same head. `#reconcileOwed` carries the remaining count across
        // drains: the record that would have cleared these marks is the one
        // the wrap threw away, so stopping at the budget and never coming back
        // would leave the rest leaked exactly as before.
        let budget = PENDING_MARK_SCAN;
        const slice = [];
        for (const key of this.#pendingDel.keys()) {
            slice.push(key);
            if (slice.length >= budget) break;
        }
        for (const key of slice) {
            this.#reconcileOwed--;
            if (!this.#pendingDel.has(key)) continue;   // released under us
            let held;
            try { held = native.has(key); } catch { this.#reconcileOwed = 0; return; }
            if (held) {                             // not applied yet: still ours to hold
                this.#pendingDel.touch(key);        // to the back of the order, unchanged
                continue;
            }
            this.#pendingDel.release(key);
            this.#noteDeleted(key, head);
        }
        if (this.#reconcileOwed < 0 || this.#pendingDel.size === 0) this.#reconcileOwed = 0;
    }

    // The same job for WRITE marks after a wrap, and it asks a different thing
    // of a different place.
    //
    // A removal's question is "does the arena still hold the key", because a
    // removal that landed removed it. A write's landing is "the key is held",
    // which the arena also says for a write that has NOT landed and whose
    // predecessor is still resident -- so the arena cannot answer this one at
    // all, in either direction. That is why a wrap used to simply drop every
    // write mark, and why a `minLevel: 2` write the primary had not applied
    // went back to reading the value it had replaced.
    //
    // THE SUBMISSION RING ANSWERS IT EXACTLY. It is SPSC with this worker as
    // the producer, so its consumer index is the primary saying, in its own
    // words, how far it has got; the position just past our own record was
    // recorded when the record was pushed (see PendingMarks' `sub`). Past it
    // means applied -- SubmitDrain publishes the index with a release store
    // after the arena write -- so the mark has done its job and goes. Short of
    // it means NOT applied, so L2 still holds the value our write supersedes
    // and the mark stays.
    //
    // A KEPT MARK IS NOT A STUCK MARK, which is the failure this must not
    // reintroduce. It is re-examined on every later drain until the index
    // passes it, so the only way it outlives the process is a primary that is
    // alive, holding our record, and never draining -- the same condition
    // under which a removal mark persists, and one that misses rather than
    // lying.
    //
    // A `sub` of -1 is UNKNOWABLE, not "not yet": the IPC fallback, or a push
    // with no ring claimed. Released, which is what both transports did
    // before, and the safe direction between one stale read and a key nothing
    // can make readable again. Same for a tail we cannot read at all.
    #reconcileWrites() {
        const tail = this.#submitTailNow();
        // Budgeted and resumed exactly like #reconcileRemovals, for the same
        // reason: a ring that keeps lapping between reads makes every read
        // wrap again, and this runs on the read path.
        const slice = [];
        for (const key of this.#pendingWrite.keys()) {
            slice.push(key);
            if (slice.length >= PENDING_MARK_SCAN) break;
        }
        for (const key of slice) {
            const sub = this.#pendingWrite.submittedAt(key);
            if (sub === undefined) continue;                  // released under us
            if (sub >= 0 && tail >= 0 && tail < sub) {
                this.#pendingWrite.touch(key);                // still in flight: keep it
                continue;
            }
            this.#pendingWrite.release(key);
        }
        // WHAT IS LEFT, not "what this pass has not looked at yet". A removal
        // is judged once and for all -- the arena either still holds the key
        // or does not -- so #reconcileRemovals counts DOWN as it examines. A
        // write's answer can be "not yet": the primary had not reached our
        // record when we asked, and the record that would have told us so was
        // the one the wrap discarded. Counting down would retire such a mark
        // from reconciliation after a single look and leave it held forever,
        // which is the unreadable key this is here to avoid. So the pass stays
        // armed until nothing is outstanding.
        //
        // It disarms on its own: an ordinary drain releases marks by their own
        // records, so the set empties and this goes quiet. Until it does, a
        // drain costs one extra native read and at most PENDING_MARK_SCAN map
        // operations -- paid only by a worker that has both lapped the ring
        // and written at `minLevel` 2 or 3, and it buys back the marks the
        // primary has already applied, which are misses this worker would
        // otherwise keep serving.
        this.#writeOwed = this.#pendingWrite.size;
    }

    // HOW FAR THE PRIMARY HAS CONSUMED OUR OWN SUBMISSION RING, or -1 when
    // that cannot be known: no ring claimed (IPC, the primary itself, a
    // detached worker), or the addon refusing. Never zero as a stand-in --
    // zero is a real position, and reading "unknowable" as "nothing consumed"
    // would keep every mark forever.
    //
    // TRUSTED, and that is a widening of the trust model rather than an
    // oversight: the submit segment is mapped read-write by every worker, so a
    // hostile one can forge this and cost us one stale read after a wrap. It
    // used to be able to cost only its own writes. See the TRUST note at the
    // top of submit.h and the limitation in README.
    #submitTailNow() {
        if (this.#ringIdx < 0) return -1;
        try { return native.submitTail(); } catch { return -1; }
    }

    // The position just past the record we have only now pushed, for the mark
    // that is about to be taken for it. Read AFTER the push, unlike the
    // invalidation-ring position beside it, which is read before: this one is
    // our own producer index, which nobody else advances, so there is no race
    // to lose -- and a position read before the push would name our
    // predecessor's record instead of ours.
    #submitPosNow() {
        if (this.#ringIdx < 0) return -1;
        try { return native.submitHead(); } catch { return -1; }
    }

    // Record that `key` was removed, and where on the invalidation ring that
    // removal became visible. See #deletedAt. Same wholesale-flush bound and
    // the same reasoning: a dropped entry costs one blocked promotion, not
    // correctness, and the entry is useless anyway once the ring head has
    // passed it.
    #noteDeleted(key, at) {
        if (this.#queue === null) return;        // no adapter: nothing promotes, nothing to guard
        // EVICT THE OLDEST, never wipe. These entries DO have a useful order --
        // ring positions are monotonic, so the oldest is the one most likely to
        // be behind every live read's mark already and so the least useful --
        // and a wholesale clear at the bound would throw away the guard that
        // stops a caller being handed a value it deleted. That was reachable in
        // one step once a wrapped-ring reconcile could push up to
        // PENDING_MARK_MAX entries in at once. Map iteration is insertion order,
        // so the first key is the oldest.
        // DELETE BEFORE SET. `Map.set` on a key already present keeps its
        // FIRST-insertion slot, so a key deleted repeatedly would carry a
        // fresh position in an old slot and be evicted first -- the exact
        // inversion of the rule above, for exactly the keys most likely to
        // need the guard.
        this.#deletedAt.delete(key);
        if (this.#deletedAt.size >= PENDING_MARK_MAX) {
            const oldest = this.#deletedAt.keys().next();
            if (!oldest.done) this.#deletedAt.delete(oldest.value);
        }
        this.#deletedAt.set(key, at);
    }

    // How many clears the CLUSTER has handed to L3 and not seen land, read
    // from the arena header (Header::l3ClearGen). This is the half of the
    // clear guard that crosses a process boundary; l3ClearsInFlight above is
    // the half that covers this process before the primary has applied its
    // clear. Zero when there is no arena to ask -- a degraded worker cannot
    // promote into L2 at all, and its own clears are still covered by the
    // module counter.
    //
    // Only ever called from the L3 read path, so a cache with no adapter never
    // reaches the addon for it.
    #sharedClearsInFlight() {
        if (!storeReady || this.#primaryDead) return 0;
        return native.l3ClearsInFlight();
    }

    // Did anything invalidate THIS key while the read was in flight? Checking
    // for the key's own hash rather than "did the head move at all" matters:
    // under load the head always moves, so the conservative version would never
    // promote and L3 hits would never reach L2.
    //
    // Returns '' to promote, or the name of the counter to bump. The reasons are
    // unrelated events and an operator watching one is misled by the other:
    // `l3PromotionsBlocked` is contention from SOMEONE ELSE -- this key changed
    // and the ring caught it, or the ring cannot rule out that it did --
    // `l3PromotionsBlockedSelf` is this SAME process's own outstanding write
    // (see below): both are "blocked, but the value handed back", so folding
    // them together was tempting, but an operator watching for contention on a
    // hot key would see their own write traffic counted alongside it, with no
    // way to tell which fraction is which -- the same reason `l3UnhashableKeys`
    // and `l3DeletedWhileReading` were split out rather than folded into
    // `l3PromotionsBlocked` too. `l3UnhashableKeys` is a key that can
    // never live in L1 or L2 at all, whatever the ring says, and
    // `l3DeletedWhileReading` is a prevented resurrection, the one reason that
    // also changes the answer the caller gets (see #fetchFromL3), and
    // `l3ClearedWhileReading` is a clear SOMEWHERE IN THE CLUSTER that was
    // handed to L3 while this read was in flight and has not been applied
    // there yet -- an operator seeing that number wants to know a flush is
    // still landing, not to have it counted as per-key contention. It is a
    // COUNT OF BLOCKED READS, deliberately not named after the arena gauge
    // `native.l3ClearsInFlight()`, which measures a level rather than
    // counting events.
    #promotionBlock(key, mark, clearMark) {
        // WHAT THIS PROCESS ITSELF STILL OWES L3, asked first and asked of the
        // queue rather than the ring.
        //
        // The ring answers "did anyone else change this key". It cannot answer
        // "did I already change it": an operation of ours that is queued or on
        // the wire has by definition not reached L3, so L3 still serves the
        // value it supersedes, and no ring record exists for a change L3 has
        // not made. This check also has to come BEFORE the degraded-worker
        // bail-out below, because it holds whether or not there is an arena to
        // read a ring from.
        //
        // A delete is a resurrection: promoting here would put a removed value
        // back into the SHARED arena, for a full TTL, visible to every process
        // on the box. A set is the older value winning: the write already
        // evicted the local copies and told the adapter `willCache: false`, so
        // promoting L3's pre-write value would reinstate a copy no provider
        // will ever invalidate. Both must refuse; only the delete also changes
        // what the caller is told.
        const owed = this.#queue === null ? undefined : this.#queue.outstandingKind(key);
        // THE REMOVAL MARK ONLY. This reason changes the ANSWER, not just the
        // placement: the caller is told `undefined`, because `get()` already
        // says that for a key we removed. A pending WRITE of ours is the
        // opposite situation -- the key exists, we just replaced it -- and
        // reading its mark here answered `undefined` for a live key and
        // counted the set in a counter named for prevented resurrections.
        // Its own reason is below, with the other placement-only ones.
        if (owed === 'delete' || this.#pendingDel.has(key)) return 'l3DeletedWhileReading';
        // A REMOVAL OF OURS THAT HAS ALREADY COMPLETED, but completed after this
        // read started. Neither check above can see it: the queue let go of the
        // operation when L3 acknowledged it, and #pendingDel was cleared when
        // the arena applied it. The ring still carries the record, but a hash
        // match only says "this key changed", which blocks the promotion and
        // leaves the caller holding the value it deleted while get() answers
        // undefined. Remembering where our own removal landed is what tells the
        // two apart. See #deletedAt.
        if (this.#deletedAt.size) {
            const at = this.#deletedAt.get(key);
            if (at !== undefined && at > mark) return 'l3DeletedWhileReading';
        }
        // THE CLEAR REASONS COME BEFORE THE SELF-SET REASON, and the order is
        // not cosmetic: only one of the two changes what the caller is TOLD.
        // `l3PromotionsBlockedSelf` blocks the placement and still hands back
        // what L3 returned, while `l3ClearedWhileReading` answers undefined.
        // Asked the other way round, a read that started before a clearAll()
        // and happened to overlap a set of this process's own was handed the
        // very value the clear was removing, while `get()` for the same key
        // correctly said undefined -- decision 70's "no exception for reads
        // that started first", undone by a guard that stopped at the first
        // reason it matched rather than at the strongest one.
        //
        // A CLEAR THIS PROCESS ISSUED, asked before the degraded bail-out
        // below rather than after it. l3ClearsInFlight needs no arena, and on
        // a degraded worker it is the ONLY clear guard left -- decision 70
        // says so in as many words -- but it sat under a bail-out that answers
        // 'l3PromotionsBlocked', which blocks the PLACEMENT and still hands
        // the caller what L3 returned. So a degraded worker's in-flight read
        // answered with the value its own clearAll() was erasing, and every
        // later read of that key joined the same answer through the herd map.
        if (l3ClearsInFlight > 0) return 'l3ClearedWhileReading';
        // A CLEAR ANY PROCESS ON THIS BOX HANDED TO L3 AND L3 HAS NOT APPLIED.
        // Guarded on having an arena rather than sitting under the bail-out
        // below, so that the self-set reason underneath it stays reachable on
        // a degraded worker -- where it is the only guard of its kind left.
        if (mark >= 0 && storeReady && !this.#primaryDead &&
            (this.#sharedClearsInFlight() > 0 || native.l3ClearGen() !== clearMark))
            return 'l3ClearedWhileReading';
        // Counted apart from `l3PromotionsBlocked`: this is not the ring
        // reporting that someone else changed the key, it is this same
        // process's own write still on its way to L3. Folding it into the
        // shared counter would put self-inflicted traffic in the same number
        // an operator watches for contention from elsewhere. See the counter
        // note above #promotionBlock.
        if (owed === 'set') return 'l3PromotionsBlockedSelf';
        // AND THIS WORKER'S OWN WRITE THAT L3 HAS ALREADY ACKNOWLEDGED, which
        // the queue check above cannot see: it released the SET at the ack,
        // and the arena has no record for it either, because the primary has
        // not drained the submission ring. In that gap an L3 GET issued before
        // the write -- and answered from the pre-write value, which is
        // ordinary for a snapshot-reading store -- promoted the old value
        // straight over the new one in this worker's own L1. The caller had
        // awaited `setAsync === true` and then read the old value back from
        // the instance it had just written through. See #l1Put's `own`.
        if (this.#id !== 0) {
            const mine = this.#l1.get(key);
            if (mine !== undefined && mine.ownAt >= 0) return 'l3PromotionsBlockedSelf';
            // The same write, at `minLevel: 2`, where there IS no L1 entry to
            // carry the mark: the value was deliberately kept out of L1 and
            // lives only in the submission the primary has not applied. The
            // overloaded mark used to cover this case by accident, through the
            // delete reason above, which answered the caller `undefined` for a
            // key it had just successfully written. Blocking the placement is
            // what was actually wanted.
            if (this.#pendingWrite.has(key)) return 'l3PromotionsBlockedSelf';
        }
        // A degraded worker has no arena to read the ring from, so it cannot
        // know what happened and must refuse. Refusing costs a promotion, not
        // correctness.
        if (mark < 0 || !storeReady || this.#primaryDead) return 'l3PromotionsBlocked';
        // THE OTHER HALF OF THE CLEAR GUARD IS ABOVE.
        //
        // The ring cannot answer this one either, for the mirror-image reason
        // the queue check above exists: a clear issued in ANOTHER process puts
        // its flush marker on the ring when the arena is emptied, which is
        // BEFORE the adapter has applied it -- so a read that starts after
        // that marker sees a quiet ring, reads the value the clear is still
        // erasing, and writes it back into the arena every process shares,
        // with a fresh TTL. That is decision 70's guarantee being undone for
        // the very process that issued the clear. The header's generation is
        // the only signal that crosses a process boundary; `clearMark` catches
        // a clear that both began and settled inside this one read, where the
        // in-flight count has already gone back to zero.
        //
        // BOTH HALVES, exactly as the entry guard asks them. The header lags a
        // worker by one flush (see #openClearGen), and a read this worker
        // already had in flight when it called clearAll() lands inside that
        // lag: asking only the header there returns the value this process's
        // own clear is erasing -- no resurrection into the arena, but a direct
        // contradiction of "no exception for reads that started first", and of
        // the entry guard, which has been answering misses since the call.
        // THE RING IS ONLY AS COMPLETE AS THE PRIMARY HAS MADE IT.
        //
        // Every check from here down is the ring answering "did anyone else
        // change this key". It cannot see a WORKER's operation that is still
        // sitting in its submission ring, because a record for it exists only
        // once the primary has drained and applied it -- and the primary is
        // the one process that can do something about that. The architectural
        // rule made the primary the only writer of L3-derived data on the
        // ground that its writes are ordered against its own; they were
        // ordered against a worker's undrained submission by nothing, so the
        // primary promoted the pre-write value over a write a worker had
        // already been told succeeded, and over a delete it had already been
        // told succeeded. Draining first is the same lever applyBatch pulls
        // before it applies a cap, for the same reason, and it turns "no
        // record exists yet" into "a record exists, at or past the mark".
        //
        // ONLY THE PRIMARY, and only when it owns a submission segment: a
        // worker cannot drain anything, and an IPC-transport worker's batch
        // has no equivalent lever on this side at all -- see the report and
        // decision 69. The cost is one drain per promotion, which is work
        // this process was going to do on the next doorbell regardless.
        if (this.#id === 0 && submitName !== null) {
            let guard = 0;
            while (TurboKV.drainSubmissions(8192) > 0 && ++guard < 512);
        }
        const h = native.hashKey(key);
        // A key the arena cannot even hash -- a lone surrogate -- has no ring
        // record to compare against, and `get` already treats it as never
        // stored. Promoting it would file an unreachable L1 entry under the
        // hash `undefined`, which every such key would then share.
        if (h === undefined) return 'l3UnhashableKeys';
        let cursor = mark;
        // Batched, not truncated. ringRead returns at most `max` records, so a
        // single call covering only the first 1024 of the 8192 a ring holds
        // would miss an invalidation sitting at 1025 and promote over it -- the
        // very defect the guard exists to prevent, just harder to hit.
        for (;;) {
            let r;
            try { r = native.ringRead(cursor, 1024); } catch { return 'l3PromotionsBlocked'; }
            if (!r || r.wrapped) return 'l3PromotionsBlocked';   // fell too far behind to know
            for (let i = 0; i < r.hashes.length; i++) {
                // The flush marker is not a key hash -- it means EVERYTHING changed,
                // so it matches every key. A guard that only compared hashes would
                // let a value the clear removed straight back in.
                if (r.hashes[i] === 'ffffffffffffffff' || r.hashes[i] === h) return 'l3PromotionsBlocked';
            }
            if (r.head <= cursor || r.head >= r.ringHead) return '';
            cursor = r.head;
        }
    }

    // Fill downward per minLevel. The L2 half goes through #publishL3Derived,
    // which is the ONE route L3-derived data takes into the arena and refuses
    // to take it from anyone but the primary -- see that method.
    #fillFromL3(key, rec, level, accepted) {
        const cap = this.#l3Ttl(rec);
        if (level <= 2) TurboKV.#publishL3Derived(key, accepted.enc, cap, this);
        if (level === 1) {
            const v = this.#codec && !this.#l1Decoded ? accepted.enc : accepted.value;
            this.#l1Put(key, v, native.hashKey(key), this.#noCodec ? 0 : accepted.enc.length,
                        cap ? monoMs() + cap : 0);
        }
    }

    // How long a value from L3 may live locally.
    //
    // `rec.ttlMs` is the adapter's, so it is untrusted input like any other
    // option this class takes. A naive `PTTL` passthrough returns -1 for "no
    // expiry" and -2 for "no key", and the old expression took -1 as truthy,
    // min'd it against l3TtlMs to -1, and handed that to the arena -- which
    // stored the entry with NO expiry at all, so the one bound that exists to
    // stop an L3 value outliving its provider was bypassed by the very reply
    // it was meant to bound. Anything that is not a positive finite number
    // means "L3 named no deadline", which is what l3TtlMs is for. The upper
    // clamp is set()'s: uint32 milliseconds from the arena epoch.
    #l3Ttl(rec) {
        const t = rec.ttlMs;
        const recTtl = typeof t === 'number' && t > 0 && t <= 0x7fffffff ? t
            : (typeof t === 'number' && t > 0x7fffffff ? 0x7fffffff : 0);
        return this.#l3TtlMs > 0
            ? (recTtl ? Math.min(recTtl, this.#l3TtlMs) : this.#l3TtlMs)
            : recTtl;
    }

    // Turn an adapter's value into the pair this cache actually uses: `enc` is
    // what L2 stores, `value` is what a caller is handed. THROWS if the value
    // cannot be stored at all -- see the call site in #fetchFromL3 for why that
    // has to happen before anything is written.
    //
    // This is also where decisions 7 and 26 are honoured on the read-through
    // path. `get()` has always obeyed them at its L1 and L2 returns; getAsync
    // handed back the object it had just put in L1, so a Buffer was shared with
    // both the cache and the adapter's own store (mutating the result corrupted
    // both), and a `direct`-mode object came back unfrozen while the same value
    // read synchronously a moment later was frozen. Two forms of one operation
    // cannot differ in whether the caller may mutate the result.
    #acceptFromL3(raw) {
        if (!this.#codec) {
            // bytes mode stores the value directly, so what is acceptable here
            // is exactly what set() accepts -- and binary is COPIED, because it
            // is mutable and the adapter still holds its own reference to it.
            if (ArrayBuffer.isView(raw) || raw instanceof ArrayBuffer) {
                const b = Buffer.from(ArrayBuffer.isView(raw)
                    ? new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength)
                    : new Uint8Array(raw));
                return { enc: b, value: b };
            }
            const t = typeof raw;
            if (t !== 'string' && t !== 'number' && t !== 'boolean' && t !== 'bigint' && raw !== null)
                throw new TypeError(`turbokv: the l3 adapter returned a ${t}; bytes mode accepts ` +
                    `string/number/boolean/bigint/null or binary (Buffer/TypedArray/ArrayBuffer/DataView)`);
            return { enc: raw, value: raw };
        }
        if (typeof raw !== 'string')
            throw new TypeError(`turbokv: the l3 adapter returned a ${typeof raw}, but this cache ` +
                `stores codec-encoded values, which are strings`);
        const value = this.#codec.decode(raw);
        // Same rule as get()'s L2 return: the decoded object is the cache's,
        // shared with whatever goes into L1, so it is frozen before anyone can
        // reach it. In `safe` mode L1 keeps the encoded form and every read
        // decodes afresh, so there is nothing shared to protect.
        if (this.#l1Decoded && this.#freeze) TurboKV.deepFreeze(value);
        return { enc: raw, value };
    }

    // A caller never receives the object L1 holds. Strings and frozen objects
    // are safe to share; a Buffer is not (decision 7).
    #handOut(v) { return Buffer.isBuffer(v) ? Buffer.from(v) : v; }

    // The single reporting path for a background L3 failure. The listener is
    // the caller's, so it can throw; that is not our failure to propagate.
    #reportL3(e, op) {
        if (this.#onL3Error) { try { this.#onL3Error(e, op); } catch { /* not ours */ } }
    }

    // The L3 half of a write, shared by `set` and `setAsync`. The sync caller
    // ignores the promise; the async caller awaits it. Identical effects.
    #queueSet(key, enc, ttlMs, level) {
        if (!this.#queue) return Promise.resolve(true);
        this.stats.l3Sets = (this.stats.l3Sets || 0) + 1;
        const p = this.#queue.push({
            kind: 'set', key, value: enc, ttlMs,
            willCache: this.#willCacheWrite(level),
            originId: this.#originId(),
            // The same measurement set() makes for the arena bound, rather than
            // a second one that disagrees with it. Both arms of the old
            // expression were `enc.length`, so a multi-byte string undercounted
            // -- and a bytes-mode number or boolean, which has no `.length` at
            // all, made this NaN, and `NaN + op.bytes > maxBytes` is false
            // forever: the queue's byte bound stopped existing for that op and
            // for every op behind it.
            bytes: encodedBytes(enc) + key.length + 48,
        });
        // Decision 68 and the spec's failure matrix: a write L3 did not accept
        // is KEPT locally, under a SHORT TTL -- serve through the outage, then
        // converge. Without the cap, a write that failed once diverges from L3
        // for as long as its own TTL says, which for the common `ttlMs: 0` is
        // forever: exactly the split brain `l3FailTtlMs` was added to bound.
        // Both abandonment routes settle false and so both land here: an op
        // that outlived `l3RetryMs`, and one shed on the spot past
        // `l3QueueMaxBytes`.
        //
        // Nothing to cap at level 3: nothing was written locally to begin with.
        if (this.#l3FailTtlMs > 0 && level < 3) {
            // `p` itself is what the caller gets; this is a branch off it, so
            // the caller still observes the queue's own outcome unchanged.
            p.then((landed) => { if (!landed) this.#capAfterL3Failure(key, enc, ttlMs); },
                   () => { /* push() never rejects; this is belt and braces */ });
        }
        return p;
    }

    // Cap what the local tiers hold for `key` at `l3FailTtlMs`.
    //
    // SHORTENING ONLY, IN BOTH TIERS. An entry that already expires sooner
    // keeps its own deadline, and a key that is no longer resident is left
    // alone rather than resurrected -- the point is to bound how long this box
    // may disagree with L3, never to put anything back and never to give
    // anything a longer life than its caller asked for.
    #capAfterL3Failure(key, enc, ttlMs) {
        const cap = ttlMs > 0 ? Math.min(ttlMs, this.#l3FailTtlMs) : this.#l3FailTtlMs;
        // Counted once per failed write that actually re-timed something, so
        // the counter means "a local copy was capped", not "a write failed" --
        // stats.l3SetFailed already says the latter.
        let applied = false;
        const e = this.#l1.get(key);
        if (e !== undefined) {
            const deadline = monoMs() + cap;
            if (!e.exp || e.exp > deadline) { e.exp = deadline; applied = true; }
        }
        // 'capped' / 'moot' / 'unable' -- informational here. The failure
        // buckets are counted where the failure is known (see #countCapLost);
        // 'moot' is not a failure at all, because the arena no longer holds
        // the value this cap was taken against.
        if (this.#capL2AfterL3Failure(key, enc, cap) === 'capped') applied = true;
        if (applied) this.stats.l3FailTtlApplied = (this.stats.l3FailTtlApplied || 0) + 1;
    }

    // Remember a cap for THIS WORKER'S OWN READS, and make sure something comes
    // back to retire it.
    //
    // This map is no longer a queue of work: the cap itself has already been
    // handed to the primary by the time this is called. What it holds is the
    // read guard -- between the moment the cap falls due and the moment the
    // primary applies it, the capped L1 entry has expired and a read would
    // fall straight through to the uncapped L2 copy, so the worker would serve
    // the value L3 refused past its own deadline. See #capExpired.
    //
    // The timer is unref'd -- convergence is not work a process should be kept
    // alive for -- and only runs while an entry is outstanding.
    #noteCap(key, enc, cap) {
        // A failed write's promise can settle AFTER close(): the queue keeps
        // retrying until it is told to stop, and #closeL3 tells it only after a
        // bounded drain. Arming a timer then is low harm -- it is unref'd and
        // it terminates itself on the first tick, because a closed cache has no
        // arena -- but a closed cache should own no timer at all.
        // A GUARD IT CANNOT KEEP IS COUNTED, not dropped in silence. The
        // request goes out either way (#retimeOutbox is called next), and
        // without an entry nothing can ever judge whether it landed -- so a
        // sustained outage against a hand-wired primary under-counted from the
        // 4097th outstanding cap onward, which is precisely the scale at which
        // the number matters.
        if (this.#closePromise !== null || this.#l3Caps.size >= L3_CAP_MAX) {
            this.#countCapLost('l3FailTtlUnconfirmed');
            return;
        }
        // WHERE THE INVALIDATION RING WAS. At retirement this is what tells a
        // cap that was never applied apart from one that was applied and then
        // had the same bytes written over it by somebody else -- see
        // #judgeRetiredCaps. -1 when there is no ring to read, which makes the
        // verdict `unconfirmed` rather than a guess.
        let at = -1;
        if (storeReady && !this.#primaryDead) { try { at = native.ringHead(); } catch { at = -1; } }
        const now = monoMs();
        // Retired one window PAST the deadline, which is the only stretch in
        // which the guard can bite: before it the capped L1 entry answers, and
        // by the end of it the primary has either applied the cap it was sent
        // or is not coming back.
        this.#l3Caps.set(key, { enc, cap, at, deadline: now + cap, until: now + cap + L3_CAP_WINDOW_MS });
        if (this.#capTimer !== null) return;
        this.#capTimer = setInterval(() => this.#runCaps(), L3_CAP_POLL_MS);
        if (this.#capTimer.unref) this.#capTimer.unref();
    }

    #stopCapTimer() {
        if (this.#capTimer === null) return;
        clearInterval(this.#capTimer); this.#capTimer = null;
    }

    // THE ONE PLACE A LOST CAP BECOMES A NUMBER -- literally one, so the claim
    // can be checked rather than believed -- and once per cap.
    //
    // `l3FailTtlApplied` counts the failed write whose LOCAL copy was capped,
    // which on a worker is L1 -- so it moves whether or not the L2 half ever
    // lands, and on its own it reads as success. The silence on the other side
    // is the dangerous half: the value L3 refused sits in the SHARED arena
    // with no expiry, box-wide, for every process, forever.
    //
    // TWO BUCKETS, because they answer different questions and folding them
    // together is what made the first version of this counter claim a
    // precision it did not have:
    //
    //   l3FailTtlUnapplied  -- PROVEN not applied. Nothing was ever handed
    //     over, or it was handed over and the arena still holds exactly the
    //     capped bytes, unbounded, and the invalidation ring says nobody else
    //     wrote the key since. Every one of these is a real divergence.
    //   l3FailTtlUnconfirmed -- handed over, outcome UNKNOWABLE from here.
    //     The worker degraded or closed before the window ran out; the ring
    //     had lapped past the mark so it cannot say whether anyone wrote the
    //     key; or no guard entry could be kept at all (past L3_CAP_MAX). It
    //     may have landed perfectly. It is counted because "we stopped being
    //     able to tell" is itself something an operator needs to see, and
    //     because putting it in the other bucket would make that bucket lie.
    //
    // A cap that is MOOT -- superseded by a newer write, or wiped by a clear
    // -- is in neither: there is nothing left to bound. Those sites say so.
    #countCapLost(bucket) {
        this.stats[bucket] = (this.stats[bucket] || 0) + 1;
    }
    // Deduplicated through the guard entry, and REQUIRING one: a cap whose
    // guard is already gone has either been judged at retirement or was never
    // recorded (in which case #noteCap counted it as unconfirmed on the spot).
    // Counting it again here is what let a `process.send` callback erroring
    // after the window double-count a cap the retirement check had settled.
    #capLost(key, bucket) {
        const c = this.#l3Caps.get(key);
        if (c === undefined || c.counted) return;
        c.counted = true;
        this.#countCapLost(bucket);
    }
    #capUnapplied(key) { this.#capLost(key, 'l3FailTtlUnapplied'); }
    // By entry rather than by key, for the wholesale drops (degrade, close)
    // that are walking the map already.
    #capEntryUnconfirmed(c) {
        if (c.counted) return;
        c.counted = true;
        this.#countCapLost('l3FailTtlUnconfirmed');
    }

    // One pass over the outstanding read-guard entries: retire the ones whose
    // window has closed, and stop the timer once none is left.
    //
    // This used to be where the cap was retried against the arena until the
    // compare became meaningful, at up to 64 entries a tick because each one
    // COPIED A VALUE OUT of the arena. The cap is a conditional operation the
    // primary applies now (see #capArena), so there is nothing to retry and no
    // arena work here at all -- only a timestamp compare.
    #runCaps() {
        // THE ARENA IS GONE, so nothing here can ever be judged. Every guard
        // still outstanding was a request that was sent and whose fate this
        // worker will now never learn -- which is what `unconfirmed` means.
        if (!storeReady || this.#primaryDead) {
            for (const c of this.#l3Caps.values()) this.#capEntryUnconfirmed(c);
            this.#l3Caps.clear(); this.#stopCapTimer(); return;
        }
        const now = monoMs();
        let retiring = null;
        for (const [key, c] of this.#l3Caps) {
            if (now >= c.until) (retiring || (retiring = [])).push([key, c]);
        }
        if (retiring !== null) {
            this.#judgeRetiredCaps(retiring);
            for (const [key] of retiring) this.#l3Caps.delete(key);
        }
        if (this.#l3Caps.size === 0) this.#stopCapTimer();
    }

    // DID THESE CAPS EVER LAND? Asked once, when the guard's window closes and
    // the primary has either applied the request or is not going to.
    //
    // The cheap half first, and it settles almost everything: a key the arena
    // no longer holds, or holds with different bytes, or holds with a
    // deadline, is a cap that landed or was superseded. Only bytes that are
    // still exactly ours AND still unbounded are suspect.
    //
    // THE BYTES ALONE ARE NOT PROOF, though, and that is what the first
    // version of this check got wrong. The primary can apply the cap, the
    // entry can then expire or be evicted, and ANOTHER PROCESS can write
    // byte-identical bytes with no TTL -- at which point the suspect test says
    // "never applied" about a fresh, L3-agreed write. A same-process rewrite
    // cannot do this (#cancelCaps drops the guard), so the false positive was
    // exactly cross-process: in the cluster this counter exists for.
    //
    // So ask the invalidation ring the question it is for -- *did anyone else
    // write this key since I took the cap* -- which is the same question
    // #promotionBlock asks it. A record from ANOTHER writer at or past the
    // mark means somebody rewrote the key, so nothing is lost. OUR OWN
    // records are skipped by writer id, because the write being capped is
    // itself one of them and would otherwise suppress every verdict.
    //
    // ONE ring pass for the whole retiring batch, from the OLDEST mark, so a
    // burst of 4096 retiring caps costs one walk rather than 4096. The hash
    // set that produces covers a slightly WIDER span than any single cap's
    // mark, so a record that predates a given cap can suppress it: that
    // direction under-counts, never over-counts, which is the direction this
    // counter has to err in.
    #judgeRetiredCaps(retiring) {
        const suspect = [];
        for (const [key, c] of retiring) {
            if (c.counted) continue;
            let cur;
            try { cur = native.get(key); } catch { cur = undefined; }
            if (cur === undefined || !sameStored(cur, c.enc)) continue;
            let rem = 0;
            try { rem = native.lastTtlRemainingMs(); } catch { continue; }
            if (rem !== 0) continue;                       // capped, or bounded by something
            suspect.push([key, c]);
        }
        if (suspect.length === 0) return;
        let from = -1;
        for (const [, c] of suspect) if (c.at >= 0 && (from < 0 || c.at < from)) from = c.at;
        const touched = from < 0 ? null : this.#ringWritersSince(from);
        for (const [key, c] of suspect) {
            // No mark, or a ring that has lapped past it: the question cannot
            // be answered from here.
            if (touched === null || c.at < 0) { this.#capEntryUnconfirmed(c); continue; }
            if (touched.all) continue;                     // a clear: everything changed
            let h;
            try { h = native.hashKey(key); } catch { h = undefined; }
            if (h === undefined) { this.#capEntryUnconfirmed(c); continue; }
            if (touched.other.has(h)) continue;            // somebody else wrote it; not lost
            // A RECORD THAT COULD BE OURS, AND COULD BE SOMEBODY ELSE'S. See
            // #ringWritersSince: the two writer-id spaces overlap, so this is
            // the honest answer rather than a skip that would hide a real
            // rewrite.
            if (touched.mine.has(h)) { this.#capEntryUnconfirmed(c); continue; }
            this.#capUnapplied(key);
        }
    }

    // Hashes written at or past `from`, split by whether the writer id on the
    // record is ours. Null when the ring cannot say (lapped, or detached
    // mid-walk).
    //
    // THE SPLIT IS NOT A SKIP, and that distinction is the whole of this
    // method. The write being capped is one of OUR records and would suppress
    // every verdict if it counted as "somebody wrote it" -- but a writer id
    // cannot actually identify us. The primary stamps a shared-memory
    // submission with its RING SLOT plus one and an IPC batch with the
    // sender's WRITER ID, and nothing keeps those two spaces apart: a shm
    // worker holding slot 0 (`mine` = 1) cannot tell its own record from one
    // written by a `transport: 'ipc'` worker that happens to have been given
    // id 1. Treating that as ours HIDES a genuine cross-process rewrite, which
    // is the exact false fire this oracle was added to stop, reached one step
    // further in. Treating it as somebody else's hides every real loss.
    //
    // So neither: a hash seen ONLY on records bearing our own id is reported
    // as ambiguous, and the cap is counted `unconfirmed` rather than proven
    // either way. It fails safe, which is what a counter whose whole value is
    // being believed needs, and it costs the `unapplied` bucket only the cases
    // where this worker's own write for the key landed INSIDE the cap's
    // window -- everything drained before the mark is not in the walk at all.
    //
    // The alternatives were a transport tag on the record (a TCS_LAYOUT bump
    // and a C++ change, for a diagnostic counter) or comparing on the pair
    // (the same, since the pair is not in the record).
    //
    // BOUNDED, like #primaryInvalidate's walk: `wrapped` is the ordinary exit,
    // but a producer faster than this loop could otherwise keep it going, and
    // this runs on a timer with the event loop to itself.
    //
    // AND AN EXHAUSTED BOUND RETURNS null, exactly as `wrapped` does. This is
    // the one thing a bound must not do quietly: a partial walk returned as a
    // result reads as "no record for this key anywhere", so a rewrite sitting
    // past the unscanned tail became a PROVEN loss -- the same false fire the
    // ambiguity answer above exists to prevent, through the door the bound
    // itself opened. An incomplete scan is indistinguishable from a lapped
    // ring and must land in the same bucket.
    //
    // The bound is deliberately not tight against the geometry by accident:
    // `ringCap` is capped at 65536 records (store.h) and each round takes
    // 1024, so 64 rounds cover a FULL ring. A static ring therefore always
    // completes, and exhaustion means only one thing -- the primary appended
    // faster than this synchronous walk could read, sustained over 65536
    // records -- which is exactly the case where the answer is unknown.
    // test/review3_regression_test.js walks a nearly full ring to keep that
    // relationship honest.
    #ringWritersSince(from) {
        const mine = this.#ringIdx >= 0 ? this.#ringIdx + 1 : this.#id;
        const other = new Set(), own = new Set();
        let cursor = from, all = false, complete = false;
        for (let round = 0; round < 64; round++) {
            let r;
            try { r = native.ringRead(cursor, 1024); } catch { return null; }
            if (!r || r.wrapped) return null;
            for (let i = 0; i < r.hashes.length; i++) {
                if (r.hashes[i] === 'ffffffffffffffff') { all = true; continue; }
                if (r.writers[i] === mine) own.add(r.hashes[i]);
                else other.add(r.hashes[i]);
            }
            if (r.head <= cursor || r.head >= r.ringHead) { complete = true; break; }
            cursor = r.head;
        }
        if (!complete) return null;
        return { other, mine: own, all };
    }

    // Is the arena's copy of `key` one this worker has already asked to be
    // re-timed, and is that deadline already past? Then this worker knows the
    // value L3 refused is still there with a longer life than it should have,
    // and must not serve it. Decision 68 promises the box converges after a
    // failed write; until the primary applies the cap, this is what makes that
    // true HERE, at exactly the deadline, rather than whenever the ring
    // happens to be drained.
    //
    // Only ever reached when a cap is outstanding, which is only on a worker
    // and only after an L3 write failed.
    #capExpired(key, raw) {
        const c = this.#l3Caps.get(key);
        return c !== undefined && monoMs() >= c.deadline && sameStored(raw, c.enc);
    }

    // The L2 half of the cap. Returns 'capped', 'moot' (nothing left to bound)
    // or 'unable' (no way to bound it) -- see #capAfterL3Failure.
    //
    // A WORKER NEVER WRITES L2 HERE, OR ANYWHERE. It used to: it compared the
    // arena itself and then submitted a re-write of the old value through the
    // ring or the outbox, and the compare and the write were separated by a
    // hop. Anything the primary applied inside that hop -- a newer write, a
    // delete -- was overwritten by the cap when it landed, resurrecting a
    // deleted key or replacing a newer value box-wide for l3FailTtlMs. The
    // compare was never wrong; it was asked in the wrong PROCESS. So the
    // worker asks the primary to apply the cap CONDITIONALLY, and the primary
    // -- which writes the arena synchronously -- compares and writes with
    // nothing in between. See #capArena and applyBatch's 'r'.
    #capL2AfterL3Failure(key, enc, cap) {
        // Counted HERE rather than by the caller, so that #countCapLost really
        // is the only site that touches these numbers -- "one counting site"
        // was not true while the caller incremented on 'unable' as well.
        if (!storeReady || this.#primaryDead) {
            this.#countCapLost('l3FailTtlUnapplied');      // no arena: certainly not applied
            return 'unable';
        }
        // NO MARK IS CONSULTED HERE, AND THAT IS THE FIX, NOT AN OMISSION.
        //
        // This used to read "our own delete is on its way to the arena, there
        // is nothing left to re-time" off #pendingDel -- which also carried
        // every `minLevel: 2` SET. So a worker's minLevel-2 write that L3 then
        // refused answered 'moot' before the cap was ever taken: no request to
        // the primary, no guard entry, no counter -- while `l3FailTtlApplied`
        // still moved for the L1 half and reported success. The value L3 had
        // refused sat in the SHARED arena with no expiry, box-wide. It is the
        // ORDINARY case during an outage, not a race: a shed queue settles
        // `false` in a microtask, long before any record can clear the mark,
        // so once the queue backs up every subsequent minLevel-2 write takes
        // this path. The spec's failure-matrix row -- "Queue over
        // l3QueueMaxBytes -> keep, capped at l3FailTtlMs" -- was simply false
        // for them.
        //
        // A genuine pending DELETE needs no check here either, because the
        // question is answered where it can be answered SYNCHRONOUSLY: the
        // primary runs #capArena, which compares the arena against the value
        // the cap was taken against, and applyBatch has drained every
        // submission ring to empty first -- so our delete has provably landed
        // and the compare says 'moot' on its own. Asking on the worker, a hop
        // earlier, could only ever guess.
        if (this.#id === 0) {
            const r = TurboKV.#capArena(key, enc, cap);
            // Decision 64's family: the primary skips its own ring records, so
            // a sibling instance keeps an L1 copy carrying the expiry this
            // call just SHORTENED -- and goes on serving, past the cap, the
            // very value L3 refused.
            if (r === 'capped') TurboKV.#dropOthers(key, this);
            return r;
        }
        // The read guard is this worker's own half and is recorded whether or
        // not the request reaches the primary: until the cap lands, this
        // worker must stop serving the uncapped L2 copy at the deadline.
        this.#noteCap(key, enc, cap);
        // QUEUED, NOT CONFIRMED, so there is no failure to report here: the
        // push into the outbox cannot fail, and everything that can go wrong
        // afterwards is counted by flush() or by the guard's retirement. This
        // used to test a return value that was always true, so the branch
        // under it was unreachable.
        this.#retimeOutbox(key, enc, cap);
        return 'capped';
    }

    // THE CONDITIONAL RE-TIME, and the only place a cap is ever written.
    //
    // Runs ON THE PRIMARY -- either for its own failed write or on behalf of a
    // worker (applyBatch's 'r') -- because only there are the compare and the
    // write one synchronous step with nothing able to land between them.
    //
    // Two conditions, and both are load-bearing:
    //
    //   - the arena must still hold the value the cap was taken against. A
    //     later write superseded it, or a delete removed it: re-writing would
    //     put an older value back, and a stale value is the one failure mode
    //     this system does not accept.
    //   - the entry's REMAINING LIFE must be longer than the cap. The cap is a
    //     CEILING on how long this box may disagree with L3, not a new lease:
    //     a blind rewrite with `ttlMs = cap` measured from now EXTENDED an
    //     entry whose own TTL expired sooner, so `set(k, v, {ttlMs: 2000})`
    //     whose L3 write failed at 1513ms had its remaining life jump back to
    //     5000ms and outlived what the caller asked for. The remaining life is
    //     readable here, synchronously, right after the compare's own get().
    static #capArena(key, enc, cap) {
        let cur;
        try { cur = native.get(key); } catch { return 'unable'; }
        if (cur === undefined || !sameStored(cur, enc)) return 'moot';
        const rem = native.lastTtlRemainingMs();
        if (rem > 0 && rem <= cap) return 'moot';
        return TurboKV.#retimeArena(key, enc, cap) ? 'capped' : 'unable';
    }

    // Local first, then L3. L3-first was rejected: when the remote is
    // unreachable it writes NOTHING, not even locally, turning a remote outage
    // into a local write outage. The promise still carries the guarantee.
    async setAsync(key, value, opts) {
        const ok = this.set(key, value, opts);
        if (!ok) return false;
        const p = this.#lastQueued;
        this.#lastQueued = null;
        return p ? p : true;
    }

    // Always returns a boolean and never throws, so a caller may ignore the
    // result. Because that makes failure quiet, every rejection also bumps a
    // stats counter and records lastError.
    set(key, value, opts) {
        const minLevel = opts === undefined ? 1 : this.#resolveLevel(opts.minLevel);
        this.#checkPrimary();
        this.stats.sets++;
        if (!isStringKey(key)) {
            this.stats.rejectedKey = (this.stats.rejectedKey || 0) + 1;
            this.lastError = `key must be a string, got ${typeof key}`;
            return false;
        }
        // The options object is the caller's, so reading it can throw.
        let ttlOpt = 0;
        try { ttlOpt = (opts && opts.ttlMs) || 0; }
        catch (e) { this.lastError = `reading options failed: ${e.message}`; return false; }
        // uint32 milliseconds from the arena epoch is ~49 days of range; clamp
        // rather than overflow (ttlMs near INT32_MAX used to overflow the
        // seconds conversion and expire immediately).
        const ttlMs = Math.max(0, Math.min(ttlOpt, 0x7fffffff));
        // Keys used to be silently truncated at 512 bytes, so distinct keys
        // collided and returned each other's values. Reject instead.
        if (Buffer.byteLength(key) > this.#keyMax) {
            this.stats.rejectedKey = (this.stats.rejectedKey || 0) + 1;
            this.lastError = `key of ${Buffer.byteLength(key)} bytes exceeds the ${this.#keyMax}-byte limit`;
            return false;
        }
        // An empty key is rejected by the submission ring's validator, and a
        // rejected record stops that ring permanently -- so a single set('') from
        // a worker silently killed all of its later writes. Reject it up front,
        // in every process, so the two paths agree on what a legal key is.
        if (key.length === 0) {
            this.stats.rejectedKey = (this.stats.rejectedKey || 0) + 1;
            this.lastError = 'key must not be empty';
            return false;
        }
        if (hasLoneSurrogate(key)) {
            this.stats.rejectedKey = (this.stats.rejectedKey || 0) + 1;
            this.lastError = 'key contains an unpaired surrogate';
            return false;
        }
        if (this.#noCodec) {
            const t = typeof value;
            // Binary values are accepted alongside the primitives: decision 4
            // lists Buffer/Uint8Array/ArrayBuffer, and they are byte-shaped
            // rather than object-shaped, so they need no codec.
            const isBinary = ArrayBuffer.isView(value) || value instanceof ArrayBuffer;
            // BigInt is a primitive too, and immutable, so it belongs here.
            if (!isBinary && t !== 'string' && t !== 'number' && t !== 'boolean' && t !== 'bigint' && value !== null) {
                this.stats.rejectedType++;
                this.lastError = `bytes mode accepts string/number/boolean/bigint/null ` +
                    `or binary (Buffer/TypedArray/ArrayBuffer/DataView), got ${t}`;
                return false;
            }
            if (t === 'string') {
                // A V8 SlicedString keeps its parent alive: caching a 1MB
                // substring of an 8MB document retains all 8MB (measured).
                // Flattening costs ~42ns and makes the accounting honest.
                value = native.flatten(value);
            } else if (isBinary) {
                // Decision 7: binary values are mutable, so the cache keeps its
                // own copy rather than sharing the caller's.
                value = Buffer.from(ArrayBuffer.isView(value)
                    ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
                    : new Uint8Array(value));
            }
        }
        // Encode once: L2 needs bytes regardless, so this is not extra work.
        // A codec can throw on values it cannot represent (JSON on a BigInt or
        // a cycle). set() promises never to throw, so that surfaces as false.
        let enc;
        try { enc = this.#codec ? this.#codec.encode(value) : value; }
        catch (e) {
            this.stats.rejectedType++;
            this.lastError = `codec.encode failed: ${e.message}`;
            return false;
        }
        // JSON.stringify returns undefined (rather than throwing) for a
        // function, a symbol or undefined itself, so a successful encode is not
        // proof of a usable result.
        if (this.#codec && typeof enc !== 'string') {
            this.stats.rejectedType++;
            this.lastError = `codec.encode produced ${typeof enc}, not a string (value type ${typeof value})`;
            return false;
        }
        let l1Value = value;
        if (this.#codec && !this.#l1Decoded) l1Value = enc;             // safe: keep the encoded form
        else if (this.#codec && this.#isolate) l1Value = this.#codec.decode(enc);
        // Freeze only ever applies to an object the cache owns. Freezing the
        // caller's object would be a side effect on something they still hold.
        if (this.#codec && this.#freeze) TurboKV.deepFreeze(l1Value);
        const keyHash = native.hashKey(key);
        // set() reports whether the pipeline ACCEPTED, serialised and queued the
        // value - not that it is durably in L2. A worker's write is applied by
        // the primary a tick later, so the size must be checked here; otherwise
        // an oversized value would be queued, silently dropped by the primary,
        // and reported as success.
        // UTF-8 BYTES, not UTF-16 units: the old check accepted values the
        // primary then rejected, destroying the previous value silently.
        const encLen = encodedBytes(enc);
        if (encLen + key.length + 48 > this.#maxValue) {
            this.stats.rejectedSize++;
            this.lastError = `value ${encLen}B exceeds the ${this.#maxValue}B arena limit`;
            return false;
        }
        // A value can fit the arena and still be too large for a submission ring
        // record. That is a PERMANENT condition, not the transient "ring full"
        // backpressure, so it must be reported as a rejection here rather than as
        // an endless stream of successful-looking writes that never reach L2.
        if (this.#ringIdx >= 0 && this.#ringMaxValue > 0 && encLen > this.#ringMaxValue) {
            this.stats.rejectedSize++;
            this.lastError = `value ${encLen}B exceeds the ${this.#ringMaxValue}B submission-ring limit ` +
                             `(raise submitRingBytes on the primary)`;
            return false;
        }
        // A WRITE SUPERSEDES BOTH OF OUR OWN MARKS: the key is no longer
        // removed, and whatever earlier write of ours had not landed is not
        // the value anybody will read either. Released HERE, after the two
        // size rejections above and before anything is published, because a
        // write that is REJECTED changes nothing at all -- releasing a removal
        // mark for it would hand this worker straight back to L2's copy of a
        // key it has been told is deleted. That is the ordering `#cancelCaps`
        // had to learn four times over; see #publishOutbox.
        this.#pendingDel.release(key);
        this.#pendingWrite.release(key);
        // WHERE THE INVALIDATION RING IS, BEFORE THIS WRITE IS PUBLISHED. Used
        // by both the L1 self-mark below and the write mark the minLevel >= 2
        // branches take: only a record at or past this position can be ours.
        // The primary reads no ring records of its own (#drain is a no-op
        // there) and never marks, so it does not pay for this.
        const at = this.#id === 0 ? 0 : this.#ringHeadNow();
        if (minLevel === 1) {
            // SELF-MARKED ON A WORKER. This write is in L1 now and in the
            // submission ring, and the ring record has not come back -- so an
            // L3 read this worker already had in flight, whose adapter reply
            // predates the write, would otherwise promote the OLD value
            // straight over it. Neither existing guard can see that: the queue
            // let go of the SET the moment L3 acknowledged it, #pendingDel and
            // #deletedAt cover only removals, and the ring carries no record
            // for a write the primary has not drained. So the caller who
            // awaited `setAsync === true` read the old value back from its own
            // instance. See #l1Put and #promotionBlock.
            this.#l1Put(key, l1Value, keyHash, this.#noCodec ? 0 : enc.length,
                        ttlMs > 0 ? monoMs() + ttlMs : 0, this.#id === 0 ? -1 : at);
        } else {
            // Bypassing L1 must EVICT any copy already there. Leaving it would
            // make this option produce stale reads -- the caller asked for the
            // value not to live here, and would keep being served the old one.
            this.#l1Drop(key);
            // On the primary that is enough: native.set below is synchronous, so
            // the next get() reads the new value straight from L2. In a WORKER
            // the write is still in flight, so a local read would find L2's
            // PREVIOUS value -- wrong, not merely stale. It is marked pending,
            // the same way a queued delete is, so local reads MISS until the
            // ring confirms it landed. A miss is the failure mode this system
            // is built around; a stale value is not.
            //
            // NOT MARKED HERE, for either level. A mark is only ever honest
            // once something has actually been submitted: the record that
            // clears it is the submission's own, so a mark taken beside a
            // write that is then SHED -- by a full ring, by a dead primary --
            // is one nothing can ever clear, and a marked key is unreadable
            // from every tier, L3 included, for the life of the worker.
            // `minLevel: 3` already took its mark at the point where it knew;
            // `minLevel: 2` did not, and had exactly that hole. Both now mark
            // inside the submission branch below.
        }
        // `minLevel: L3` means NOTHING is written locally -- spec 5.2, and the
        // read path has always agreed (`#fillFromL3` fills only at `level <= 2`).
        // The write path did not: it split `minLevel === 1` from everything
        // else and then ran the L2 write unguarded, so the value landed in the
        // shared arena while the adapter was told `willCache: false`. That half
        // is the dangerous one: a remote store registers no interest for a key
        // this box is in fact holding, so it sends no invalidation for it and
        // the stale read is permanent and cross-box.
        //
        // Any copy ALREADY in L2 has to go, for exactly the reason the L1 copy
        // above does: the caller asked for this value not to live here, and
        // leaving the previous one resident would serve it as though it were
        // current -- this option would produce stale reads instead of misses.
        if (minLevel === 3) {
            if (this.#id === 0) {
                TurboKV.#publishArenaDel(key, 0);
                TurboKV.#dropOthers(key, this);
            } else if (!this.#primaryDead) {
                // MARKED ONLY WHEN THE EVICTION IS ACTUALLY SUBMITTED. The mark
                // exists so this worker's reads miss until the eviction comes
                // back around the ring, and only a submitted eviction ever
                // produces that record. An eviction shed by a full ring, or
                // skipped because the primary is gone, leaves a mark nothing
                // can clear -- and a marked key is unreadable from every tier,
                // L3 included, for the life of the worker.
                // A WRITE MARK, EVEN THOUGH WHAT IS SUBMITTED IS A DELETE.
                // The mark names what the OPERATION is, not what the record
                // says, and this operation is a SET: the value is not going
                // away, it is going one tier down, and the delete only takes
                // L2's superseded copy out of the way. It was marked as a
                // removal, and #deletedHere -- which is right to refuse L3 for
                // a key we deleted -- then refused to ask L3 for a key that by
                // construction lives ONLY in L3. The write was acked and
                // unreadable through every form, `get` and `getAsync` alike,
                // with the adapter never called.
                //
                // Everything the mark is actually for still holds: local reads
                // MISS on L2's older copy until the eviction lands (a write
                // mark is half of #unappliedHere, exactly as `minLevel: 2` is),
                // nothing records this as a removal for the promotion guard,
                // and the wrapped-ring reconciliation asks the submission
                // ring's consumer index -- which answers "has the primary
                // applied my record" for a delete record and a set record
                // alike, so the kind no longer decides where the answer comes
                // from. See #reconcileWrites.
                if (this.#ringIdx >= 0) {
                    if (this.#publishRingDel(key)) this.#pendingWrite.mark(key, keyHash, at, this.#submitPosNow());
                    else {
                        this.stats.writesShed = (this.stats.writesShed || 0) + 1;
                        this.lastError = 'submission ring full; L2 eviction shed';
                    }
                } else {
                    this.#pendingWrite.mark(key, keyHash, at);
                    this.#publishOutbox('d', key, null, 0, key.length + 48);
                }
            }
            this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
            return true;
        }
        if (this.#id === 0) {
            const ok = TurboKV.#publishArenaSet(key, enc, 0, ttlMs);
            if (!ok) { this.stats.rejectedSize++; this.lastError = 'value does not fit the arena'; this.#l1Drop(key); }
            // Our own ring record is skipped on the primary, so nothing else
            // invalidates the copies other instances in THIS process hold.
            else TurboKV.#dropOthers(key, this);
            if (ok) this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
            return ok;
        }
        // Shared-memory submission: a memcpy into this worker's own ring, which
        // the primary already has mapped. The IPC path is kept as a fallback for
        // when the ring segment is unavailable (older primary, claim failed).
        if (this.#primaryDead) {
            this.stats.writesShed = (this.stats.writesShed || 0) + 1;
            this.lastError = 'primary is not available; the write is in L1 only';
            this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
            return true;
        }
        if (this.#ringIdx >= 0) {
            if (this.#publishRingSet(key, enc, ttlMs)) {
                this.stats.sent++;
                // MARKED ONLY NOW, and only at minLevel 2 -- see the else
                // branch above. The record that clears this mark is the one
                // the push just made; a mark taken before the push and left
                // behind by the shed below made the key unreadable from every
                // tier, L3 included, for the life of the worker.
                //
                // A WRITE MARK. This is a value going IN, not a key going
                // away: its landing is "the arena holds it", the cap must not
                // read it as a removal in flight, and an L3 read that overlaps
                // it is blocked from PLACING an older value rather than told
                // the key is deleted. `at` was read before the push above --
                // the primary can drain and append our record between the two,
                // and a mark stamped past its own record never clears. The
                // SUBMISSION position is read the other way round, after the
                // push, because it has to name our own record; see
                // #submitPosNow, and #reconcileWrites for what reads it.
                if (minLevel !== 1) this.#pendingWrite.mark(key, keyHash, at, this.#submitPosNow());
                this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
                return true;
            }
            // Ring full. Same contract as a shed IPC write: the value is in this
            // worker's L1, it just has not reached L2, so other workers see a
            // miss rather than a wrong value. Counted, never silent.
            this.stats.writesShed = (this.stats.writesShed || 0) + 1;
            this.lastError = 'submission ring full; L2 write shed';
            this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
            return true;
        }
        // The IPC fallback can still be shed later, inside flush(), which is
        // why flush() unmarks what it drops -- the mark cannot be deferred to
        // the send the way the ring's can, because the push IS the handover.
        // And no submission position: there is no ring, and a batch on the
        // cluster channel is acked by nothing, so a wrap cannot tell whether
        // this landed. See #reconcileWrites.
        if (minLevel !== 1) this.#pendingWrite.mark(key, keyHash, at);
        this.#publishOutbox('s', key, enc, ttlMs, encLen + key.length + 48);
        this.#lastQueued = this.#queueSet(key, enc, ttlMs, minLevel);
        return true;                      // queued; capacity is decided by the primary
    }

    // Edge-triggered doorbell. The primary only needs waking when its rings go
    // from empty to non-empty: while it is already draining, every extra
    // notification is pure waste, and under load the ring is almost never empty
    // so this fires rarely. A timer instead would either burn wakeups finding
    // nothing or add latency waiting for the next tick.
    #ringDoorbell() {
        if (this.#doorbellPending) return;
        this.#doorbellPending = true;
        // A one-field message, not a batch: this is a notification, not a
        // transport. The payload that used to freeze the event loop for
        // 0.49-1.15ms per send now travels through shared memory instead.
        setImmediate(() => {
            this.#doorbellPending = false;
            if (process.connected) { try { process.send({ t: RING_MSG, id: this.#id }); } catch { /* shutting down */ } }
        });
    }

    // Batching normally waits for the next tick, but a worker doing a long
    // SYNCHRONOUS burst never turns the event loop, so setImmediate never fires
    // and the outbox grows without bound - measured at 50MB of worker heap for
    // 60k sets. Flush eagerly once it exceeds a byte cap: process.send can be
    // called at any time, the tick is only there to batch.
    #schedule(addedBytes) {
        this.#outboxBytes += addedBytes;
        if (this.#outboxBytes >= this.#outboxMaxBytes) { this.flush(); return; }
        if (this.#flushScheduled) return;
        this.#flushScheduled = true;
        setImmediate(() => this.flush());
    }

    // Existence check only: no decode, no promotion into L1, not counted as a
    // hit, and it deliberately leaves the CLOCK reference bit alone.
    has(key) {
        if (!isStringKey(key)) return false;
        this.#drain();
        // Degraded means the arena is unmapped, so native.has returns undefined.
        // The declared return type is boolean; L1 is all we can answer from.
        if (this.#primaryDead) return this.#l1.has(key);
        if (hasLoneSurrogate(key)) return false;
        // EITHER MARK, exactly as get() -- has() must not answer `true` from
        // L2 for a key get() answers `undefined` for. See #unappliedHere.
        if (this.#unappliedHere(key)) return false;
        const e = this.#l1.get(key);
        if (e !== undefined) {
            if (!e.exp || e.exp > monoMs()) return true;
            this.#l1Drop(key);
        }
        // has() deliberately avoids copying a value, but a cap this worker is
        // still waiting on can only be judged by comparing one -- so on that
        // rare path it defers to get(), which already knows. Answering `true`
        // here for a value get() reports as gone would put the two forms back
        // into disagreement, which is the whole point of the guard.
        if (this.#l3Caps.size && this.#l3Caps.has(key)) return this.get(key) !== undefined;
        return native.has(key);
    }

    // has(), then L3 if the local tiers do not have it. Identical effects to
    // has(); the only difference is that this one can wait for L3.
    async hasAsync(key) {
        if (this.has(key)) return true;
        // Process-wide AND cluster-wide, same as #fetchFromL3's guard: a
        // sibling instance's clear in flight must block this instance's L3
        // reads too, since they share the arena the clear is emptying -- and
        // so must another PROCESS's, since they share the same arena and the
        // same L3. Answering `true` here for a key a clear is erasing is the
        // existence-shaped form of the same resurrection.
        if (!this.#l3 || l3ClearsInFlight > 0 || this.#sharedClearsInFlight() > 0) return false;
        // `has` already rejected a non-string key by returning false; going on
        // to L3 with it would hand the adapter something the contract does
        // not accept.
        if (!isStringKey(key)) return false;
        // A key THIS process deleted, whose removal L3 has not applied yet
        // (the delete is queued or on the wire, but has not landed). `has`
        // already answers false for it -- on a worker because of #pendingDel,
        // on the primary because the arena was updated synchronously -- so
        // going to L3 here would say `true` for a key this caller was just
        // told is gone. Same reasoning as the guard in #fetchFromL3, for a
        // read that answers existence rather than a value.
        if (this.#deletedHere(key)) return false;
        // Bounded for the same reason the read path is: a hung adapter must
        // answer "not here" within the operation budget rather than never.
        try {
            if (typeof this.#l3.has === 'function')
                return await this.#queue.deadline(this.#l3.has(key), this.#l3RetryMs, 'has') === true;
            // No has() on the adapter: fall back to a read. Correct, and it
            // transfers the value needlessly -- which is why has() is in the
            // contract as an optional member at all.
            const rec = await this.#queue.deadline(this.#l3.get(key, { willCache: false }), this.#l3RetryMs, 'get');
            return rec !== undefined && rec !== null;
        } catch (e) { this.#reportL3(e, { kind: 'has', key }); return false; }
    }

    delete(key) {
        this.#checkPrimary();
        if (!isStringKey(key)) { this.lastError = `key must be a string, got ${typeof key}`; return false; }
        this.stats.deletes++;
        if (this.#id === 0) {
            const had = TurboKV.#publishArenaDel(key, 0);
            this.#l1Drop(key);
            TurboKV.#dropOthers(key, this);
            // The removal is applied to the arena synchronously here, so its
            // ring position is exactly the head that follows it -- and that is
            // true whether or not the key was present, because the removal is
            // published either way (see storeDelete). A read already in flight
            // marked an earlier head, so the comparison in #promotionBlock
            // tells the two apart.
            if (this.#queue !== null && storeReady && !this.#primaryDead) {
                try { this.#noteDeleted(key, native.ringHead()); } catch { /* detached */ }
            }
            this.#lastQueued = this.#queue
                ? this.#queue.push({ kind: 'delete', key, originId: this.#originId(), bytes: key.length + 48 })
                : null;
            return had;
        }
        // A worker's delete is applied a tick later, so report whether the key
        // was present at call time. Returning an unconditional true meant a
        // worker and the primary disagreed about the same absent key.
        const had = this.#l1.has(key) || native.has(key);
        this.#l1Drop(key);
        // A worker's delete is applied by the primary a tick later, so a get()
        // in between refilled L1 straight from L2 and served the value this
        // worker just deleted. Remember the key until the invalidation for it
        // comes back around. Bounded: if the primary is not applying our
        // deletes, dropping the record only costs us a stale read, whereas
        // growing without bound costs the process.
        const keyHash = native.hashKey(key);
        // MARKED ONLY WHEN SOMETHING WILL ACTUALLY PUBLISH THE REMOVAL. A
        // DEGRADED worker submits nothing -- the arena is unmapped and the
        // primary that would apply it is gone -- so the mark would be one
        // nothing can ever clear, and a marked key is unreadable from every
        // tier, L3 included: `deleteAsync('r')` followed by `getAsync('r')`
        // answered undefined forever, without the adapter being asked once.
        // Nothing local can serve a stale value while degraded anyway: the
        // #l1Drop above emptied L1 and `get` answers from L1 only. The mark is
        // taken again if this handle recovers and deletes again; a delete
        // issued DURING the outage is a lost write rather than a pending one,
        // which is what #recovered() already says in as many words.
        if (!this.#primaryDead) this.#pendingDel.mark(key, keyHash, this.#ringHeadNow());
        // The L3 half, shared by every worker path below. Same contract as
        // #queueSet: the sync caller ignores the promise, deleteAsync awaits
        // it. L3 is the store of record for a worker's delete -- there is no
        // local tombstone once the outage ends, unlike a set's short-TTL
        // revert.
        this.#lastQueued = this.#queue
            ? this.#queue.push({ kind: 'delete', key, originId: this.#originId(), bytes: key.length + 48 })
            : null;
        if (this.#ringIdx >= 0) {
            if (this.#publishRingDel(key)) { /* published */ }
            else {
                this.stats.writesShed = (this.stats.writesShed || 0) + 1;
                this.lastError = 'submission ring full; L2 delete shed';
                // NOTHING WAS SUBMITTED, so no invalidation record will ever
                // come back to clear the mark -- and a marked key is
                // unreadable from every tier including L3, permanently, while
                // its entry counts toward the wholesale flush that then drops
                // real pending deletes. The honest state is the one a shed
                // write already has: the removal did not reach L2, which is
                // counted and reported rather than hidden behind a local miss
                // that never ends.
                this.#pendingDel.release(key);
            }
            return had;
        }
        this.#publishOutbox('d', key, null, 0, key.length + 48);
        return had;
    }

    // Local delete, then L3. If L3 is UNREACHABLE, reads cannot refetch
    // either, so the delete holds for the whole outage -- there is no local
    // tombstone to fall back on the way a set() has a short-TTL revert. The
    // only case where the old value returns is reads succeeding while the
    // DEL failed, and the caller resolved false and knows it.
    //
    // WHICH IS WHY THIS RESOLVES THE L3 OUTCOME, not local presence. Spec 9 and
    // 5.4 and decision 70 all rest on "the caller resolved false and knows it",
    // and this promise is the only signal the design offers: there IS no local
    // tombstone to inspect afterwards, and `delete` already returned the local
    // answer synchronously to anyone who wanted it. Resolving on presence made
    // the one guarantee false in both directions -- a failed L3 delete resolved
    // `true` while L3 still held the key, and an L3-only key whose delete
    // succeeded everywhere resolved `false`.
    //
    // With no adapter there is no outcome to report and nothing changes: the
    // local answer is the whole answer, exactly as before.
    async deleteAsync(key) {
        // A rejected key (delete() returns early, before touching #lastQueued)
        // must not fall through to reading it below: #lastQueued would then
        // be whatever an earlier, unrelated operation left there, and this
        // call would await a promise that has nothing to do with it.
        if (!isStringKey(key)) { this.lastError = `key must be a string, got ${typeof key}`; return false; }
        const had = this.delete(key);
        const p = this.#lastQueued;
        this.#lastQueued = null;
        if (!p) return had;
        return await p === true;
    }

    // Drops only this process's L1. The shared arena is untouched, so the next
    // read simply repopulates it.
    clearLocal() {
        this.#dropCachedValues();
        // A clear is the case where forgetting IS right: the arena has been
        // emptied, so a pending delete has nothing left to be pending against,
        // a recorded removal has nothing left to protect, and a cap has no
        // entry to re-time. Contrast the wrapped-ring branch in #drain, which
        // looks identical and must not do this.
        //
        // The caps are dropped WITHOUT COUNTING, deliberately: a cap whose key
        // the clear has removed is MOOT, not lost. There is nothing left in
        // L2 to bound, so neither failure bucket applies. Same for a cap a
        // newer write supersedes -- see #cancelCap.
        this.#pendingDel.clear(); this.#pendingWrite.clear(); this.#deletedAt.clear();
        this.#l3Caps.clear(); this.#stopCapTimer();
    }

    // The L1 half of a flush: everything this process is HOLDING, and nothing
    // about what it owes.
    #dropCachedValues() {
        this.#l1.clear(); this.#byHash.clear(); this.#l1Bytes = 0; this.#l1Iter = null;
    }

    // Wipes the shared arena AND every worker's L1, via a flush record on the
    // invalidation ring. Deliberately not called clear(): this is a
    // cluster-wide side effect and the name should say so.
    clearAll() {
        this.clearLocal();
        // With an adapter attached the tiers are ONE cache: a clear that only
        // emptied the local tiers would silently undo itself, because the
        // very next read refills everything straight back out of L3. Until
        // this lands, L3 reads must miss rather than serve the values the
        // clear was meant to remove -- see #fetchFromL3. That has to hold
        // for every instance sharing this process's arena (decision 64), not
        // only this one, so the flag l3ClearsInFlight counts against is
        // module-scope. A count, not a boolean: two overlapping clears
        // (this instance calling clearAll() twice before the first lands, or
        // two sibling instances each clearing) must not have the first one's
        // completion unblock reads while the second is still outstanding.
        // The queue never sheds a clear (src/l3/queue.js): flushing twice is
        // harmless, never flushing is not.
        // And it has to hold for every PROCESS sharing the arena, which a
        // module-scope counter cannot express: see l3ClearsInFlight and
        // #openClearGen.
        if (this.#queue) {
            l3ClearsInFlight++;
            this.#openClearGen();
            this.#lastQueued = this.#queue.push({ kind: 'clear', bytes: 0 })
                .then((r) => { l3ClearsInFlight--; this.#settleClearGen(); return r; });
        }
        if (this.#id === 0) {
            TurboKV.#publishArenaClear(0);
            // The flush marker on the invalidation ring reaches other in-process
            // instances too, but only whenever #primaryInvalidate next runs after
            // a drain -- so between this call and that drain a sibling instance
            // keeps serving values that no longer exist. Same bug as set/delete,
            // same family, no key to pass this time.
            TurboKV.#clearOthers(this);
            return;
        }
        // A WORKER'S CLEAR CANCELS EVERY INSTANCE'S CAPS, not just this one's.
        // clearLocal() above dropped ours; a sibling's cap is a submitSet still
        // to come, and the primary drains the rings before it applies a `c`
        // (see applyBatch), so a cap landing in that gap is written into an
        // arena the clear has just emptied. Narrow, and the same one-line shape
        // as every other site in this family -- the primary branch above gets
        // it through #clearOthers, which reaches each sibling's clearLocal().
        this.#publishOutboxOp('c', true);
    }

    // Open this clear's generation in the ARENA HEADER, so that every OTHER
    // process serves L3 misses until it lands -- not only this one. The
    // counter this sits beside is module-scope, and the arena is not.
    //
    // ONLY THE PRIMARY MAY WRITE THE HEADER. A worker maps the arena
    // PROT_READ, and a store through that mapping is a SIGBUS the process
    // cannot catch, not an exception it can handle -- so a worker's
    // generation travels the way its clearAll already does, as an op in the
    // IPC batch the primary applies, and the header therefore lags a worker
    // by one flush. That lag is exactly why l3ClearsInFlight stays: it covers
    // THIS process for that window. No other process can be harmed during it
    // either, because the clear the primary has not applied yet has not
    // emptied L2 yet -- anything promoted before it lands is wiped by it, and
    // anything promoted after it sees the generation already open.
    //
    // A separate op rather than a flag on 'c': a clearAll with no adapter owes
    // L3 nothing and must keep behaving exactly as it did.
    #openClearGen() {
        if (this.#id === 0) { TurboKV.#l3ClearBegin(); return; }
        this.#publishOutboxOp('+', false);
    }

    // Close it again -- and it has to actually LEAVE this process, because
    // while it sits in the outbox every process in the cluster is serving L3
    // misses for every key.
    //
    // It is scheduled like any other op, and then CHASED. flush() defers a
    // whole batch whenever the IPC send window is full, and nothing re-arms
    // the flush until the next write -- which, for a worker that has just
    // flushed its cache, is exactly the "nothing else to say" case: the `-`
    // sat in the outbox with no timer behind it, stalled until traffic that
    // might never come. So while it is still queued, an unref'd timer keeps
    // trying. Unref'd because a settle must never be the reason a process
    // cannot exit, and it stops the moment the outbox drains or the channel
    // is gone -- after which the primary's own reconciliation (see
    // releaseWorker) is what settles what this process could not.
    //
    // Reached after close() too: a clear retries indefinitely, but close()
    // stops the retry loop and the queue settles the op (see #closeL3).
    #settleClearGen(resend = false) {
        if (this.#id === 0) { TurboKV.#l3ClearSettle(); return; }
        if (!resend) this.#publishOutboxOp('-', false);
        this.flush();
        if (this.#outbox.length === 0 || !process.connected) return;
        const t = setTimeout(() => this.#settleClearGen(true), 50);
        if (t.unref) t.unref();
    }

    // The header writes themselves, on the primary. Wrapped because both are
    // reached from places where the arena may already be gone -- close() tears
    // it down before the L3 half of shutdown settles a clear that outlived it,
    // and applyBatch is a public entry point a non-primary could call. A
    // settle into an arena that no longer exists is a no-op, not an error
    // worth throwing out of a promise chain nobody awaits.
    static #l3ClearBegin() { try { native.l3ClearBegin(); } catch { /* no arena to guard */ } }
    static #l3ClearSettle() { try { native.l3ClearSettle(); } catch { /* no arena to guard */ } }

    // WHAT EACH WORKER HAS OPENED AND NOT SETTLED, on the primary: writerId ->
    // count. Two jobs, and neither is bookkeeping for its own sake.
    //
    // 1. A worker that VANISHES mid-clear -- SIGKILL, a crash, a container
    //    stop -- never sends its `-`. Without this the generation stays open
    //    for the life of the arena and every process on the box serves L3
    //    misses for every key, permanently. The primary settles what the
    //    worker still owed when the channel goes away (see releaseWorker).
    // 2. `-` is a lever that DISARMS a cluster-wide guard, and it arrives over
    //    the same IPC channel any worker holds. Before this, a worker could
    //    only wipe; now it could un-guard someone else's clear. Counting per
    //    writer makes that an accounting rule rather than a new permission: a
    //    worker can settle exactly as many generations as it opened, and one
    //    whose `-` does not match a `+` of its own changes nothing.
    //
    // The primary's own clears are NOT tracked here. They do not travel over
    // IPC, so neither job applies: there is no channel to lose and no message
    // to forge, and if the primary dies the arena dies with it.
    //
    // Keyed by ATTACHMENT (the sender's nonce), never by writer id: ids are
    // caller-supplied and are meant to be reused across restarts, and keying
    // on one let a dead worker's reconciliation settle its successor's clear.
    // See ATTACH_NONCE.
    static #clearGensOwed = new Map();

    // The sender's attachment, or undefined when the message names none.
    //
    // THERE IS NO FALLBACK TO THE WRITER ID, and that is the whole point. An
    // id-keyed fallback reads as a kindness to an older worker and is in fact
    // the original defect wearing a different hat: a mixed-version rolling
    // restart is precisely the stable-slot deployment that made ids collide,
    // so the fallback re-opened, silently, the hole the nonce closes. A
    // generation must never be opened under a name that a DIFFERENT process
    // can also answer to.
    static #attachmentOf(msg) { return msg && typeof msg.n === 'string' && msg.n ? msg.n : undefined; }

    // Loud rather than silent: an unguarded clear is a correctness event, and
    // an operator watching `lastError` is the only person who can act on it.
    // Reported on every cache in this process, the way applyBatch already
    // touches every instance -- they all share the arena this affects.
    static #refuseUnidentifiedClear(op) {
        const why = `turbokv: ignored a clear-generation op ('${op}') from a batch that names no ` +
                    `attachment; that worker's L3 clear is not guarded across processes ` +
                    `(version skew, or a hand-built batch)`;
        for (const c of instances) c.lastError = why;
    }

    static #l3ClearOpenFor(who) {
        TurboKV.#clearGensOwed.set(who, (TurboKV.#clearGensOwed.get(who) || 0) + 1);
        TurboKV.#l3ClearBegin();
    }

    // Returns false when this attachment has nothing open -- a `-` that
    // matches no `+` of its own settles nothing at all.
    static #l3ClearSettleFor(who) {
        const owed = TurboKV.#clearGensOwed.get(who) || 0;
        if (owed <= 0) return false;
        if (owed === 1) TurboKV.#clearGensOwed.delete(who);
        else TurboKV.#clearGensOwed.set(who, owed - 1);
        TurboKV.#l3ClearSettle();
        return true;
    }

    // A worker is gone: settle every clear generation THAT ATTACHMENT still
    // owed, so its disappearance does not leave the cluster serving L3 misses
    // forever. Returns how many were settled.
    //
    // `who` is a cache message from the worker that has gone away -- any one
    // of them; they all name the same attachment. install() keeps the last one
    // per channel for exactly this. A plain writer id is accepted too, but it
    // identifies only a sender that never gave a nonce (a hand-rolled batch):
    // a real worker cannot be released by its id, deliberately, because ids
    // are reused across restarts and releasing by one settles the successor's
    // clear rather than the dead worker's.
    //
    // Public for the same reason applyBatch is: a primary that wires the IPC
    // channel itself has to be able to do the whole job, and reconciling a
    // dead worker is part of the job. Idempotent: a second call settles
    // nothing, so 'exit' after 'disconnect' is free.
    static releaseWorker(who) {
        // The old shape -- a writer id -- is now a MISTAKE, not a deprecated
        // spelling, and it must not fail quietly. It would settle nothing (ids
        // name no attachment any more), so a primary that routes IPC itself
        // and upgrades would keep compiling, see no warning, and leak a
        // generation on every worker replacement: a cluster-wide L3 outage,
        // arrived at by doing nothing wrong except not reading a changelog.
        if (typeof who !== 'object' || who === null) {
            throw new TypeError(
                `turbokv: releaseWorker() takes a cache message from the worker that is gone, ` +
                `not a writer id (got ${typeof who === 'number' ? who : JSON.stringify(who)}). ` +
                `Ids are reused across restarts, so releasing by one could settle a LIVE ` +
                `successor's clear; a message names the attachment instead. Keep the last ` +
                `message you routed to applyBatch, or let install() do the whole job.`);
        }
        const key = TurboKV.#attachmentOf(who);
        // A message that names no attachment opened nothing either (see
        // applyBatch), so there is genuinely nothing to settle -- this zero is
        // the truth about that sender, not a silent failure.
        if (key === undefined) return 0;
        let n = 0;
        while (TurboKV.#l3ClearSettleFor(key)) n++;
        return n;
    }

    // clearAll(), then L3. Identical effects to clearAll(); the only
    // difference is that this one can wait for L3. Unlike a set or a delete,
    // a clear is never shed and retries indefinitely (decision 70), so
    // ABSENT A close(), this promise always eventually resolves true. close()
    // is the one thing that can end that early: it tells the queue it is
    // closed (see L3Queue#close and decision 71), which ends the retry loop
    // at its NEXT FAILED ATTEMPT rather than trying forever against a cache
    // that no longer exists -- so a clear still outstanding when close() is
    // called can resolve false instead, same as an abandoned set or delete.
    // An attempt already succeeding when close() lands still resolves true;
    // only a failing one is cut short.
    async clearAsync() {
        this.clearAll();
        const p = this.#lastQueued;
        this.#lastQueued = null;
        return p ? p : true;
    }

    // Whether this addon build can compress. Compression is optional at build
    // time so the default build has no external dependencies.
    static hasCompression() { return native.hasLz4(); }

    get size() {
        const st = native.stats();
        return st ? st.live : 0;
    }

    static arenaStats() { return native.stats(); }

    // Enumerate the keys the arena holds, newest-slot order.
    // O(index slots); intended for operations and debugging, not the hot path.
    *keys({ limit = 1000, batch = 512 } = {}) {
        let cursor = 0, yielded = 0;
        for (;;) {
            const r = native.scanKeys(cursor, batch);
            if (!r) return;
            for (const k of r.keys) {
                if (yielded++ >= limit) return;
                yield k;
            }
            if (r.done) return;
            cursor = r.cursor;
        }
    }

    // The primary stamps a heartbeat and reclaims expired entries on a timer.
    // Expiry was lazy only, so an expired entry held its index slot and arena
    // bytes until the tail reached it; and heartbeatNs was never written, so a
    // worker could not tell a live arena from one whose primary had died.
    #startMaintenance(opts = {}) {
        if (this.#id !== 0 || opts.maintenance === false) return;
        const everyMs = opts.maintenanceMs || 500;
        // Size the slice so a full pass over the index completes in a bounded
        // time regardless of index size. A fixed slice covered only ~3% of a
        // 1M-slot index per second, so expired entries lingered for minutes.
        const fullPassMs = opts.sweepFullPassMs || 30000;
        const slots = (native.stats() || {}).indexSlots || 65536;
        const slice = opts.sweepSlots ||
            Math.max(1024, Math.ceil(slots / Math.max(1, fullPassMs / everyMs)));
        native.heartbeat();
        this.#timer = setInterval(() => {
            native.heartbeat();
            // Backstop for a lost doorbell. A worker rings RING_MSG after pushing
            // to its submission ring, and that is the ONLY thing that makes the
            // primary drain -- its own get() does not (see #drain, which returns
            // immediately for id 0). The doorbell is swallowed when the channel
            // is gone (`if (process.connected)` and a catch), so a lost one would
            // otherwise leave those records in the ring until the next write:
            // not merely stale invalidation, but data that never reaches L2.
            // Workers self-heal because they drain on every operation; this gives
            // the primary the same property, bounded to one maintenance interval.
            //
            // Measured on an idle ring: 29.1ns per call, so 5ms of CPU per DAY at
            // this cadence. Cheap enough that not having a backstop was the only
            // real cost.
            if (submitName) TurboKV.drainSubmissions(8192);
            const r = native.sweepExpired(this.#sweepCursor, slice);
            if (!r) return;
            this.#sweepCursor = r.cursor;
            this.stats.expired = (this.stats.expired || 0) + r.removed;
        }, everyMs);
        if (this.#timer.unref) this.#timer.unref();   // never holds the process open
    }

    // Milliseconds since the primary last stamped the arena, or -1 if it never
    // has. A worker seeing a large value stops trusting L2.
    static primaryAgeMs() { return native.heartbeatAgeMs(); }

    // Races `promise` against a bound. Never rejects -- not even if `promise`
    // does: both call sites today pre-swallow their own rejections (drain()
    // never rejects at all; the adapter-close IIFE catches internally), but
    // this is a general helper and the next caller to reuse it with a
    // promise that CAN reject must not get an unhandled-rejection warning
    // for free. A rejection settles the race exactly like a resolution
    // does: this helper's whole contract is "did it finish or did the bound
    // expire", not what it finished with. On expiry it simply resolves,
    // same as the promise winning on its own. The timer is unref'd and
    // cleared as soon as one side settles, so a slow drain can never itself
    // become the reason the process stays alive -- the exact failure mode
    // this exists to prevent.
    #withTimeout(promise, ms) {
        return new Promise((resolve) => {
            let done = false;
            const timer = setTimeout(() => { if (!done) { done = true; resolve(); } }, ms);
            if (timer.unref) timer.unref();
            const settle = () => { if (!done) { done = true; clearTimeout(timer); resolve(); } };
            promise.then(settle, settle);
        });
    }

    // close() is idempotent, via #closePromise below: a second call must not
    // re-invoke adapter.close() or re-run the drain. close() is exactly the
    // method that gets called from more than one place -- a shutdown hook, a
    // signal handler, a test's own teardown -- and two of those firing is
    // ordinary, not exotic. Returning the SAME promise from every call
    // (rather than a fresh already-resolved one after the first) means a
    // caller awaiting any of them observes the real outcome at the same
    // time as every other caller, not a synthetic "done" ahead of it.
    close() {
        if (this.#closePromise === null) this.#closePromise = this.#doClose();
        return this.#closePromise;
    }

    // close() NEVER REJECTS.
    //
    // The teardown below used to be the whole of close(), synchronous, and it
    // threw to its caller: `stopGuard()`, the `#usesRing()` loop and the
    // unwrapped `native.destroy()` can all raise. Making close() return a
    // promise turned every one of those throws into a REJECTION -- of a promise
    // that every caller in this codebase, and every shutdown hook, signal
    // handler and test teardown outside it, deliberately ignores. Under Node
    // 18's default that is an unhandled rejection, which terminates the
    // process: a failure to release a ring slot would kill the process it was
    // trying to shut down cleanly, and the widened return type of decision 71
    // was supposed to be backwards compatible for exactly those callers.
    //
    // So a failure is REPORTED rather than propagated -- `lastError` for the
    // caller who looks, `onL3Error` with the already-declared `close` kind for
    // the listener who is watching -- and close() resolves either way. The two
    // halves are caught separately so a local teardown that fails does not also
    // skip the adapter's own close().
    async #doClose() {
        try { this.#closeLocal(); }
        catch (e) {
            this.lastError = `close failed: ${e && e.message ? e.message : e}`;
            this.#reportL3(e, { kind: 'close' });
        }
        try { await this.#closeL3(); }
        catch (e) {
            this.lastError = `close failed: ${e && e.message ? e.message : e}`;
            this.#reportL3(e, { kind: 'close' });
        }
    }

    #closeLocal() {
        // Everything below is synchronous, exactly as it was before close()
        // gained an L3 half -- deliberately. It runs to completion before
        // this function's first `await`, so a caller that calls `close()`
        // and does not await it (the overwhelmingly common case, and every
        // caller before this task) still gets the local teardown -- ring
        // slot released, arena destroyed on the primary -- synchronously,
        // in the same tick, exactly as before. L3 shutdown never touches
        // any of this state (queued writes go straight to the adapter, not
        // through the arena), so nothing here depends on it and it does not
        // need to wait for it.
        this.stopGuard();
        if (this.#timer) { clearInterval(this.#timer); this.#timer = null; }
        // Unref'd already, so it was never holding the process open -- but a
        // closed cache must not go on poking the arena it has released either.
        //
        // COUNTED ON THE WAY OUT. Each of these is a cap request that was sent
        // and whose window this handle will not be around to close, so nothing
        // will ever judge it. That is `unconfirmed`, not silence -- a process
        // shutting down mid-outage is exactly when an operator wants to know
        // how much was left hanging.
        for (const c of this.#l3Caps.values()) this.#capEntryUnconfirmed(c);
        this.#l3Caps.clear(); this.#stopCapTimer();
        instances.delete(this);
        if (instances.size === 0 && TurboKV.#recoverTimer) {
            clearInterval(TurboKV.#recoverTimer); TurboKV.#recoverTimer = null;
            TurboKV.#degraded = false;      // a timer left running re-attached with no instances
        }
        this.#ringIdx = -1;
        // The ring slot is PROCESS-wide, so release it only once no live
        // instance in this process is still using it. Releasing on the first
        // close() left every sibling holding a #ringIdx for a slot it no longer
        // owned: their pushes failed, were counted as shed, and set() still
        // returned true -- silent, permanent write loss for the rest of the
        // process's life.
        let stillUsingRing = false;
        for (const c of instances) if (c.#usesRing()) { stillUsingRing = true; break; }
        if (!stillUsingRing) {
            try { native.submitRelease(); } catch { /* transport not in use */ }
            submitReady = null;
        }
        if (this.#id === 0 && storeReady) {
            try { native.submitDestroy(); } catch { /* not created */ }
            submitName = null; isPrimaryProcess = false;
            native.destroy(); storeReady = false;
        }
    }

    async #closeL3() {
        // An open L3 connection keeps the event loop alive, so shutdown also
        // has to wait for the queue and then hand the adapter its own
        // close. This is the only reason close() returns a promise at all;
        // a caller that ignores it is unaffected.
        //
        // The wait on the queue is BOUNDED. A `clear` is never shed and
        // retries indefinitely by design (see L3Queue): until it lands, this
        // process must serve misses rather than the values the clear was
        // meant to remove. So if L3 is unreachable, the queue's pending
        // count never returns to zero and drain() never resolves --
        // awaiting it unconditionally would hang close() forever during
        // exactly the outage where an operator most wants the process to
        // exit. `l3CloseTimeoutMs` (default 5000ms; 0 waits without a
        // bound, for a caller that would rather hang than risk dropping
        // work) caps the wait, and on expiry close() proceeds anyway -- a
        // caller that asked to close gets to close. The honest cost: any
        // work still queued at that point keeps retrying in the background,
        // against an adapter this process is about to hand its own close()
        // to, and gets no second chance to be waited on -- it either lands
        // silently, is abandoned and reported through the usual
        // `onL3Error`/stats path once it exceeds `l3RetryMs` (irrelevant for
        // a `clear`, which never gives up on its own), or is still retrying
        // when the process itself exits.
        if (this.#queue) {
            const drained = this.#queue.drain();
            await (this.#l3CloseTimeoutMs > 0 ? this.#withTimeout(drained, this.#l3CloseTimeoutMs) : drained);
            // The bound above makes close() RETURN during an outage. It does
            // not make the queue STOP: a `clear` retries indefinitely by
            // design, so after close() returned, a backoff timer went on
            // rescheduling itself forever against a cache that no longer
            // exists -- the process could not exit, which is the failure
            // decision 71 exists to prevent, reached by a route its bounded
            // waits did not cover. Telling the queue it is closed ends the
            // retry loop on its next attempt; the queue's own unref'd backoff
            // (see L3Queue) keeps that last wait from holding the loop either.
            // After the drain, not before: work already accepted from a caller
            // still gets its bounded chance to land first.
            this.#queue.close();
        }
        // The adapter's own close() is bounded too, separately from the
        // drain above and by the same l3CloseTimeoutMs ceiling. Its
        // duration is entirely up to the adapter -- a real client's close()
        // can itself wait on a graceful-shutdown handshake -- and nothing
        // about draining the queue first guarantees it returns promptly.
        // Worst case this method now takes up to roughly 2x
        // l3CloseTimeoutMs (once for the drain, once for the adapter's own
        // close), which is the honest price of never hanging outright.
        if (this.#l3 && typeof this.#l3.close === 'function') {
            const closed = (async () => {
                try { await this.#l3.close(); } catch (e) { this.#reportL3(e, { kind: 'close' }); }
            })();
            await (this.#l3CloseTimeoutMs > 0 ? this.#withTimeout(closed, this.#l3CloseTimeoutMs) : closed);
        }
        // In-flight L3 reads (getAsync's herd-sharing map) may still be
        // pending against the adapter just closed above -- possibly
        // forever, if the hang is the adapter's own connection. Nothing
        // here can reach into the adapter and cancel them; clearing the map
        // only drops THIS instance's reference to them, so a stalled read
        // cannot keep a cache close() has already torn down reachable for
        // the life of that hang.
        this.#inflight.clear();
    }

    get l1Size() { return this.#l1.size; }

    stopGuard() { if (this.#gcObserver) { this.#gcObserver = null; gcUnsubscribe(this); } }

    flush() {
        this.#flushScheduled = false;
        if (!this.#outbox.length) return;
        // Do not push into a congested channel. process.send() queues into
        // libuv, which is UNBOUNDED: under sustained write load the worker's
        // RSS grew past 485MB while its JS heap stayed flat, because the
        // backlog lives outside the heap. Wait for the drain callback instead.
        // The bound must be on BYTES IN FLIGHT, not on "is one message
        // outstanding". `false` from process.send only means libuv's buffer is
        // above its high-water mark right now -- it does not mean the channel is
        // saturated. Treating it as a stop-everything flag allowed exactly one
        // message in flight and discarded everything produced while it was
        // pending, which capped delivery at ~60k writes/s. Measured with no cache
        // in the way, the same channel carries 439 MB/s under JSON and 1738 MB/s
        // under 'advanced' -- equivalent to 1.8M and 7.3M writes/s. The ceiling
        // was this policy, not the transport.
        if (this.#inFlightBytes >= this.#maxInFlightBytes) {
            if (this.#outboxBytes < this.#outboxMaxBytes) return;   // keep batching
            // Window full AND our own buffer is full: shed rather than grow
            // without bound. The value stays in this worker's L1, it just does
            // not reach L2, so other workers see a miss, never a wrong value.
            //
            // A CLEAR IS NOT SHED WITH THE WRITES, NOR ARE ITS GENERATION
            // OPS. Shedding a write costs one key its place in L2 and the
            // value is still in L1. Shedding a `c` drops a clearAll() on the
            // floor while the bookkeeping around it succeeds -- the guard
            // opens and cleanly settles, and L2 goes on serving values L3 no
            // longer has, which is the resurrection this whole mechanism
            // exists to prevent, reached through congestion instead of
            // through a race. Shedding a `-` leaves every process in the
            // cluster serving L3 misses for every key until this worker
            // happens to write again. All three carry no key and no value, so
            // keeping them cannot be what grows this buffer: one `c` and one
            // `+`/`-` pair per clearAll, and a clear is not a hot-path
            // operation.
            //
            // AND WHAT IS SHED IS UNMARKED -- through #releaseBatchMarks, the
            // one place a dropped batch lets go of what it was carrying, so
            // the three OTHER ways a batch can be dropped cannot forget it
            // (they did; see that method).
            //
            // An `r` is a cap the primary will now never apply, which is what
            // l3FailTtlUnapplied means.
            this.#releaseBatchMarks(this.#outbox, markSeq);
            const keep = [];
            let shed = 0;
            for (let i = 0; i < this.#outbox.length; i += 4) {
                const op = this.#outbox[i];
                if (op === 'c' || op === '+' || op === '-') { keep.push(op, '', null, 0); continue; }
                if (op === 'r') continue;               // counted through #capUnapplied below
                shed++;
            }
            if (shed) this.stats.writesShed = (this.stats.writesShed || 0) + shed;
            this.#dropOutboxCaps();
            this.#outbox = keep;
            this.#outboxBytes = 0;
            this.lastError = 'IPC send window full; L2 writes shed';
            return;
        }
        const batch = this.#outbox;
        const batchBytes = this.#outboxBytes;
        // Taken WITH the batch: from here on these caps live or die with it,
        // and a later flush must not be blamed for them.
        const capKeys = this.#outboxCapKeys;
        this.#outboxCapKeys = null;
        // Taken WITH the batch for the same reason, and used by every path
        // below that drops it: a mark taken AFTER this point belongs to a
        // submission this batch knows nothing about. See #releaseBatchMarks.
        const batchSeq = markSeq;
        this.#outbox = [];
        this.#outboxBytes = 0;
        this.stats.flushes++;
        this.stats.sent += batch.length / 4;
        // The channel can already be gone: a scheduled flush firing after the
        // primary exited threw EPIPE and killed the worker with an unhandled
        // 'error' event. Losing a batch during shutdown is acceptable; crashing
        // the worker over it is not.
        // AND A CAP IN IT IS COUNTED, not silently dropped. Without this the
        // stats say a failed write was capped (`l3FailTtlApplied`, from the L1
        // half) with nothing at all on the other side, while the value L3
        // refused sits in the shared arena with no expiry for every process on
        // the box.
        if (!process.connected) {
            this.stats.flushDropped = (this.stats.flushDropped || 0) + 1;
            this.#releaseBatchMarks(batch, batchSeq);
            if (capKeys) for (const k of capKeys) this.#capUnapplied(k);
            return;
        }
        // The write fails ASYNCHRONOUSLY, so try/catch cannot see it; without a
        // callback Node emits an unhandled 'error' event that kills the process.
        // Passing a callback routes the failure here instead - and tells us when
        // the message actually reached the channel, which is our drain signal.
        const self = this;
        // Declared OUTSIDE the try: the catch below reads it, and a `let` inside
        // the try block is not in scope there. It was, which made the catch throw
        // `ReferenceError: sendThrew is not defined` instead of returning the
        // reserved bytes -- so the very wedge the comment below describes still
        // happened, plus an unexpected error out of flush(). Never executed by
        // any test until one was written for it.
        let sendThrew = true;
        try {
            // Reserve AFTER the call cannot throw synchronously. process.send
            // throws for a value the serializer cannot represent (a BigInt under
            // JSON serialization), and reserving first meant the catch below
            // never returned those bytes -- eight such batches wedged the worker
            // for its lifetime while every set() still reported success.
            this.#inFlightBytes += batchBytes;
            const accepted = process.send({ t: MSG, id: this.#id, n: ATTACH_NONCE, b: batch }, err => {
                self.#inFlightBytes -= batchBytes;
                if (self.#inFlightBytes < 0) self.#inFlightBytes = 0;
                if (!err) return;
                self.stats.flushDropped = (self.stats.flushDropped || 0) + 1;
                self.lastError = `flush failed: ${err.code || err.message}`;
                self.#releaseBatchMarks(batch, batchSeq);
                if (capKeys) for (const k of capKeys) self.#capUnapplied(k);
            });
            // false means the backlog is above libuv's high-water mark.
            // Informational only now: a `false` return is normal backpressure and
            // the window, not this flag, decides whether we keep sending.
            sendThrew = false;
            if (accepted === false) this.stats.congested = (this.stats.congested || 0) + 1;
        } catch (e) {
            // Synchronous throw: the callback will never run, so return the bytes
            // here or the window shrinks permanently and the worker stops writing.
            if (sendThrew) {
                this.#inFlightBytes -= batchBytes;
                if (this.#inFlightBytes < 0) this.#inFlightBytes = 0;
            }
            this.stats.flushDropped = (this.stats.flushDropped || 0) + 1;
            this.lastError = `flush failed: ${e.code || e.message}`;
            this.#releaseBatchMarks(batch, batchSeq);
            if (capKeys) for (const k of capKeys) this.#capUnapplied(k);
        }
    }

    // A BATCH THAT IS DROPPED LETS GO OF WHAT IT WAS CARRYING.
    //
    // A `d` (a delete, or a minLevel-3 eviction) and an `s` at minLevel >= 2
    // leave a mark behind so this worker's own reads miss until the operation
    // comes back around the invalidation ring. The record that clears the mark
    // is the one THIS batch was carrying, so dropping the batch without
    // dropping the mark leaves a key that is unreadable from EVERY tier, L3
    // included, for the life of the worker -- and whose entry counts against
    // the bound that then evicts real marks.
    //
    // There are FOUR ways a batch is dropped and only the shed branch used to
    // do this. `process.send` throws SYNCHRONOUSLY for a value the serializer
    // cannot represent -- one bigint under JSON serialization throws for the
    // whole batch -- so a delete travelling beside it left a permanent mark.
    // The asynchronous error callback and the disconnected-channel return are
    // the same shape. One helper, called from all four, is why a fifth cannot
    // forget.
    //
    // BY OP, so each mark is released from the set that took it: a `d`
    // released a removal, an `s` released a write, and nothing releases the
    // other kind by accident.
    //
    // UP TO `seq`, because the send can fail long after this worker wrote the
    // same key again: a newer mark belongs to a submission that is still
    // perfectly live, and releasing it would serve L2's older value.
    #releaseBatchMarks(ops, seq) {
        for (let i = 0; i < ops.length; i += 4) {
            const op = ops[i];
            if (op === 'd') this.#pendingDel.releaseUpTo(ops[i + 1], seq);
            else if (op === 's') this.#pendingWrite.releaseUpTo(ops[i + 1], seq);
        }
    }

    // Every cap request still sitting in the outbox is gone. Used by the shed
    // branch of flush(), which keeps only the keyless clear ops.
    #dropOutboxCaps() {
        if (this.#outboxCapKeys === null) return;
        for (const k of this.#outboxCapKeys) this.#capUnapplied(k);
        this.#outboxCapKeys = null;
    }

    // Primary side: apply a worker's batch to L2.
    // Applied on the PRIMARY. The primary's #drain is a no-op (it is the sole
    // writer of its own records), so a worker's write would otherwise never
    // invalidate the primary's L1 - it kept serving its own stale value even
    // after a worker overwrote or deleted the key.
    static applyBatch(msg) {
        // set/delete travel through the shared-memory ring while clearAll
        // still travels over IPC. A worker pushes to its ring synchronously and
        // sends the IPC message afterwards, so draining the rings to empty here
        // is what keeps one worker's operations in order -- without it a
        // clearAll() was observed leaving 3808 keys that had been written
        // before it.
        if (submitName) { let guard = 0; while (TurboKV.drainSubmissions(8192) > 0 && ++guard < 512); }
        const b = msg.b;
        for (let i = 0; i < b.length; i += 4) {
            const op = b[i], key = b[i + 1];
            if (op === 's') { TurboKV.#publishArenaSet(key, b[i + 2], msg.id, b[i + 3]); TurboKV.#localDrop(key); }
            else if (op === 'd') { TurboKV.#publishArenaDel(key, msg.id); TurboKV.#localDrop(key); }
            // A WORKER'S CONDITIONAL RE-TIME (decision 68). The worker cannot
            // write L2 at all, so it names the value its cap was taken against
            // and the primary applies the short TTL only if the arena still
            // holds exactly that -- compare and write in one synchronous step,
            // here, with nothing able to land between them. Every submission
            // ring was drained to empty above, so the worker's own write for
            // this key has already been applied and a 'moot' answer means it
            // really was superseded rather than merely not arrived.
            else if (op === 'r') {
                if (TurboKV.#capArena(key, b[i + 2], b[i + 3]) === 'capped') TurboKV.#localDrop(key);
            }
            else if (op === 'c') { TurboKV.#publishArenaClear(msg.id); for (const c of instances) c.clearLocal(); }
            // The two halves of a WORKER's L3 clear generation. The worker
            // cannot write the header itself (PROT_READ; a store there is a
            // SIGBUS), so it says so here and the primary -- the arena's sole
            // writer -- does it. '+' arrives in the same batch as the 'c' it
            // belongs to and immediately before it; '-' arrives once that
            // worker's adapter has actually applied the clear.
            //
            // A batch that names no attachment gets NEITHER. Opening a
            // generation for a sender that cannot be identified means opening
            // one that nothing can reliably settle: its own '-' names nothing
            // either, and reconciliation has no handle to release. The wipe
            // itself ('c', above) still applies -- it can only REMOVE data,
            // which every worker on this channel can already ask for, and
            // refusing it would leave L2 serving values that worker's L3
            // clear is removing, a permanent divergence rather than one
            // unguarded round trip. See decision 70.
            else if (op === '+' || op === '-') {
                const who = TurboKV.#attachmentOf(msg);
                if (who === undefined) TurboKV.#refuseUnidentifiedClear(op);
                else if (op === '+') TurboKV.#l3ClearOpenFor(who);
                else TurboKV.#l3ClearSettleFor(who);
            }
        }
    }

    static #localDrop(fullKey) { for (const c of instances) c.#dropExact(fullKey); }
    #dropExact(fullKey) { this.#l1Drop(fullKey); }

    // Every instance keeps its own L1, and the primary skips the ring records it
    // wrote itself, so a write through one instance leaves the others holding
    // the old value. `instances` is normally a set of one, so this costs a
    // branch per write in the common case.
    // CALLED ONLY WHERE A WRITE HAS ACTUALLY REACHED THE ARENA OR THE RING.
    // A cancel is a claim that something newer now exists for this key; a
    // rejected write is not newer, it is nothing. Cancelling before the size
    // checks and before the ring-full shed branch -- which is where this used
    // to sit -- meant a sibling's oversized set(), which writes nothing at all
    // and returns false, threw away a cap that was about to bound a value L3
    // had refused, and the arena then held it with no expiry. That is the very
    // failure the cap exists to prevent, reintroduced by the fix for the one
    // below it.
    //
    // A NEW VALUE FOR `fullKey` HAS BEEN PUBLISHED, so the read guard held for
    // that key is finished: it exists to make this worker MISS on the value
    // L3 refused, and that value is no longer the one anybody is going to
    // read. Leaving the entry would make the key miss locally past the cap's
    // deadline even though a newer value is on its way.
    //
    // This cancel used to carry a second, heavier job -- stopping a cap that
    // would otherwise be SUBMITTED behind the new write and re-apply the old
    // value on top of it. That hazard is gone: a cap is a conditional
    // operation the primary applies, and a primary that no longer holds the
    // capped value refuses it (see #capArena). The cancel stays for the read
    // guard, which is per-instance.
    //
    // ACROSS EVERY INSTANCE IN THIS PROCESS, which is decision 64's family
    // again and the same shape as #dropOthers: sibling B's write to `k` makes
    // instance A's guard for `k` just as obsolete as A's own write would.
    // `instances` is normally a set of one, so this costs a branch per write
    // in the common case.
    static #cancelCaps(fullKey) {
        for (const c of instances) c.#cancelCap(fullKey);
    }
    // --- THE ONLY PLACES THIS PROCESS PUBLISHES A VALUE FOR A KEY --------
    //
    // Every route a write can take out of this cache ends in one of these: the
    // primary's direct arena write, a worker's submission-ring record, or the
    // IPC fallback batch. The cap cancel lives HERE, not at the call sites.
    //
    // "Cancel a cap if and only if a write actually lands" was a hand-enforced
    // invariant spread over nine call sites, and four consecutive rounds of
    // review each found one more site on the wrong side of a branch -- a
    // cancel before a size check, a cancel before a shed test, a path that had
    // lost its cancel entirely. That is what a hand-enforced invariant
    // scattered over nine sites does, not carelessness. Here a call site
    // cannot forget what it does not have to remember, a rejected or shed
    // write cannot cancel because the cancel is inside the success branch, and
    // a write site added later has to come through here -- which
    // test/write_sites_test.js checks, by attributing every raw call to the
    // arena, the ring and the outbox to its enclosing method.
    //
    // The cap's OWN write is deliberately not one of these: it re-publishes a
    // value this process already published, with a shorter deadline, so it
    // supersedes nothing and must not cancel itself. It has its own pair,
    // named for that difference -- see #retimeArena and #retimeRing.
    static #publishArenaSet(key, enc, writerId, ttlMs) {
        if (native.set(key, enc, writerId, ttlMs) !== true) return false;
        TurboKV.#cancelCaps(key);
        return true;
    }
    // A delete is published whether or not the key was there (see
    // storeDelete), so it always supersedes; `had` is the answer to a
    // different question and is passed straight through.
    static #publishArenaDel(key, writerId) {
        const had = native.del(key, writerId);
        TurboKV.#cancelCaps(key);
        return had;
    }
    static #publishArenaClear(writerId) {
        native.clearAll(writerId);
        TurboKV.#cancelAllCaps();
    }
    // --- L3-DERIVED DATA REACHES THE ARENA THROUGH EXACTLY ONE PROCESS ----
    //
    // ONLY THE PRIMARY MAY WRITE L2 WITH A VALUE THAT CAME FROM L3. A worker
    // that reads through to L3 fills its OWN L1 and stops there.
    //
    // This is an organising rule, not a special case, and it exists because a
    // whole family of defects lives at the seam between an L3 read and the
    // submission ring's asynchrony. A worker's write is NOT in the arena when
    // the call returns, and nothing orders it against the other work that
    // writes the arena -- so a promotion pushed into the ring is ordered
    // against the primary's synchronous writes, against every other worker's
    // ring, and against this worker's own earlier records by nothing at all.
    // Two rounds of review each closed one instance of that (a promotion over
    // an acked write, a cap re-writing an old value a hop later) and each time
    // the next round found another door into the same room. The primary writes
    // the arena synchronously, so a promotion it makes is ordered against
    // everything else it does; making it the only writer of L3-derived data
    // closes the room rather than one more door.
    //
    // THE COSTS ARE KNOWN AND ACCEPTED. Each worker pays its own L3 read for
    // the same key -- bounded by the worker count, which is the same bound the
    // thundering-herd design (decision 69's #inflight sharing) already
    // accepts. And while the primary is down nothing is promoted at all, which
    // is a MISS: the failure mode this system is built around, unlike the
    // stale and resurrected values the rule removes.
    //
    // Returns whether the arena took it. `false` on a worker is not a failure
    // to report: nothing was lost, the value is in that worker's L1 and in L3.
    //
    // test/write_sites_test.js is what keeps this true: it fails if any write
    // primitive or publish helper other than this one appears in #fillFromL3,
    // and if this method ever stops refusing a non-primary caller.
    static #publishL3Derived(key, enc, ttlMs, by) {
        // AND `isPrimaryProcess`, not `#id` alone. `attached: false` yields
        // `#id === 0` in ANY process -- it is the escape hatch that lets a
        // handle skip the "workerId 0 is the primary" guard in the
        // constructor -- so an undeclared option would otherwise take a
        // process that is not the primary straight past this check and into
        // native.set against a read-only mapping. A hole in a rule that is
        // meant to be total is worth one extra conjunct.
        if (by.#id !== 0 || !isPrimaryProcess) return false;
        if (!TurboKV.#publishArenaSet(key, enc, 0, ttlMs)) return false;
        // The primary skips the ring records it writes itself, so nothing else
        // drops the copies other instances in THIS process are holding --
        // decision 64's family. A sibling instance went on serving its own
        // stale L1 copy indefinitely after a promotion.
        TurboKV.#dropOthers(key, by);
        return true;
    }
    #publishRingSet(key, enc, ttlMs) {
        if (!native.submitSet(key, enc, ttlMs)) return false;
        TurboKV.#cancelCaps(key);
        this.#ringDoorbell();
        return true;
    }
    #publishRingDel(key) {
        if (!native.submitDel(key)) return false;
        TurboKV.#cancelCaps(key);
        this.#ringDoorbell();
        return true;
    }
    // The IPC fallback. This is the one publish that is not strictly
    // CONFIRMED: flush() can still shed the batch under congestion. It cancels
    // anyway, and the honest accounting of that choice is the opposite way
    // round from what it looks like. A cap that arrives LATE re-publishes the
    // value with `ttlMs = cap`, so it clears itself within l3FailTtlMs --
    // bounded. A cap that is LOST leaves the value L3 refused resident with no
    // expiry at all -- unbounded. So this trades an unbounded divergence for
    // the certainty of never putting an old value back over a newer one, and
    // it does so because that is the ranking this whole system is built on: a
    // miss is the failure mode it accepts, a stale value is not. The cost is
    // real and it is chosen, not avoided. The window is real across INSTANCES: a
    // handle on the ring holds the cap while a sibling opened with
    // `transport: 'ipc'` writes the same key, and two channels have no
    // ordering between them.
    #publishOutbox(op, key, enc, ttlMs, bytes) {
        this.#outbox.push(op, key, enc, ttlMs);
        TurboKV.#cancelCaps(key);
        this.#schedule(bytes);
    }
    // The keyless ops -- a clear and the two halves of a clear generation.
    // A `c` supersedes every cap; `+` and `-` are bookkeeping and supersede
    // nothing, so they say which they are rather than guessing.
    #publishOutboxOp(op, supersedes) {
        this.#outbox.push(op, '', null, 0);
        if (supersedes) TurboKV.#cancelAllCaps();
        this.#schedule(48);
    }
    // The cap's own write: the SAME value, with a shorter deadline. Named
    // apart from the publish family because it is the one write that must not
    // cancel -- doing so would drop the entry the read guard still needs, and
    // #runCaps is what retires that instead. Reached only through #capArena,
    // which is what makes the write conditional.
    static #retimeArena(key, enc, cap) { return native.set(key, enc, 0, cap) === true; }
    // THE WORKER'S ONLY ROUTE, on both transports. There used to be a ring
    // member here as well, which made the cap a blind re-write ordered against
    // the primary's own work by nothing (see #capL2AfterL3Failure); now the
    // worker sends the primary a REQUEST and the primary decides. `r` carries
    // the value the cap was taken against, which is exactly what the primary's
    // compare needs, and it travels in the ordinary IPC batch -- which
    // applyBatch drains every submission ring to empty before applying, so
    // this worker's own write for the key has provably landed by the time the
    // compare runs.
    //
    // Like #publishOutbox this is queued rather than confirmed: flush() can
    // shed the batch under congestion, and a shed one is counted as
    // l3FailTtlUnapplied there rather than silently lost.
    // No return value, deliberately: the push cannot fail, and saying so with
    // an always-true boolean grew a branch that could never run.
    #retimeOutbox(key, enc, cap) {
        this.#outbox.push('r', key, enc, cap);
        (this.#outboxCapKeys || (this.#outboxCapKeys = [])).push(key);
        this.#schedule(encodedBytes(enc) + key.length + 48);
    }

    // The no-key form, for a clear: it removes EVERY key, so it supersedes
    // every outstanding cap. See clearAll().
    static #cancelAllCaps() {
        for (const c of instances) c.#cancelOwnCaps();
    }
    // NEITHER OF THESE COUNTS. A cap superseded by a newer published value,
    // or wiped by a clear, is moot: the bytes it was bounding are not what
    // anyone will read, so there is no divergence to report. Counting here
    // would put ordinary write traffic into a number that is supposed to mean
    // "this box is holding something L3 rejected, unbounded".
    //
    // "SUPERSEDED" IS NOT ALWAYS TRUE, AND THE GAP IS DELIBERATE. #publishOutbox
    // cancels on a write that is only QUEUED -- flush() can still shed that
    // batch -- so a cap dropped here can turn out to have been superseded by
    // nothing, leaving the value L3 refused resident and in neither bucket.
    // That trade is decision 68's, made on the ranking this system is built
    // on: a cap that arrives LATE re-publishes with a short deadline and
    // bounds itself, while a cap that lands BEHIND a newer write puts an old
    // value back, and a stale value is the failure this design does not
    // accept. What is new here is only that the "moot" claim above must not
    // be read as covering it: the counters cannot see this case, and the
    // shed write that caused it is counted as `writesShed` instead.
    #cancelOwnCaps() { if (this.#l3Caps.size) { this.#l3Caps.clear(); this.#stopCapTimer(); } }
    #cancelCap(fullKey) { if (this.#l3Caps.size) this.#l3Caps.delete(fullKey); }

    static #dropOthers(fullKey, self) {
        if (instances.size < 2) return;
        for (const c of instances) if (c !== self) c.#l1Drop(fullKey);
    }

    // Same reasoning as #dropOthers, for the no-key case: clearAll() has
    // nothing to drop by key, so every other instance's L1 is reset wholesale.
    static #clearOthers(self) {
        if (instances.size < 2) return;
        for (const c of instances) if (c !== self) c.clearLocal();
    }
    static isCacheMessage(m) { return m && m.t === MSG; }
    // `native()` used to hand the raw addon to any caller of the public class,
    // which put poke(), suppressRefBit(), backwardShift(), clearHints() and
    // secondChanceBudget() on the same surface as get() and set(). Those mutate
    // global algorithm state or write straight into the data region; poke()
    // exists purely to prove the read-only mapping faults. Tests reach the addon
    // through `require('../src/native')` instead, which is internal to the
    // package and not reachable through `exports`.
}

module.exports = { TurboKV, Cache: TurboKV, MSG };
