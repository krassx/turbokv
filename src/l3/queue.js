'use strict';
// The per-process L3 queue.
//
// ORDER PER KEY IS A CORRECTNESS REQUIREMENT. `set(k,A); set(k,B)` sent over a
// connection pool can arrive in either order, leaving L3 holding A while this
// box holds B -- and then an invalidation for that key makes every other box
// agree with L3 rather than with us.
//
// Order ACROSS keys is deliberately not preserved: serialising every key behind
// one chain would make one slow key throttle the whole process.
//
// `clear` IS THE ONE EXCEPTION, because it is the one operation whose meaning
// is inherently cross-key: "every key" cannot be ordered by a per-key chain,
// and a set that lands after a clear resurrects exactly what the clear
// removed. So a clear is a barrier -- see #pushClear.

// `clear` has no key of its own, so it needs a chain id that can never collide
// with a real string key. A Symbol guarantees that (no string a caller could
// ever pass as `key` is `===` to it), and gives every `clear` its own single
// chain rather than mixing it into any particular key's chain.
const CLEAR_ID = Symbol('l3-clear');

// Races `promise` against a bound, REJECTING when the bound wins.
//
// An adapter is user code talking to a network. "Threw" and "never settled" are
// the same outage seen from two angles, but only the first one reaches a
// `catch` -- so without this an adapter call that neither resolves nor rejects
// wedges its caller forever: the queue's per-key chain stops draining, and on
// the read side `getAsync`'s `.finally` never runs, leaving a permanent
// `#inflight` entry that hands every future read of that key the same dead
// promise even after L3 recovers. Turning the hang into a rejection puts it
// back on the existing retry-and-abandon path, which already knows what to do
// with a failed operation.
//
// The timer is unref'd: a bound that exists to stop a hang from wedging the
// process must not itself become the reason the process cannot exit.
function withDeadline(promise, ms, what) {
    if (!(ms > 0)) return Promise.resolve(promise);
    return new Promise((resolve, reject) => {
        let done = false;
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            reject(new Error(`turbokv: the l3 adapter's ${what}() did not settle within ${ms}ms`));
        }, ms);
        if (timer.unref) timer.unref();
        Promise.resolve(promise).then(
            (v) => { if (!done) { done = true; clearTimeout(timer); resolve(v); } },
            (e) => { if (!done) { done = true; clearTimeout(timer); reject(e); } });
    });
}

class L3Queue {
    #adapter; #maxBytes; #retryMs; #onError; #now;
    // id -> { pending: op|null, inflight: op|null, settle: fn|null, busy: bool }.
    // `pending` is queued but not yet sent; `inflight` is the one on the wire.
    // Both are needed, and neither substitutes for the other, because the read
    // path asks this queue what it still owes L3 for a key (see
    // outstandingKind): a delete that has already been dispatched is exactly
    // the case that resurrects a value, and `pending` alone is null for it.
    #chains = new Map();
    #bytes = 0;
    #count = 0;
    #idle = [];
    #closed = false;
    #backoffs = new Set();        // retry timers currently sleeping; see close()
    // The clear barrier. `#clears` counts clears pushed and not yet settled;
    // `#clearGate` is non-null exactly while that count is above zero, and every
    // non-clear dispatch waits on it. See #pushClear.
    #clears = 0;
    #clearGate = null;
    #gateRelease = null;
    // Operations currently ON THE WIRE, as opposed to merely outstanding. A
    // clear waits for this to reach zero before it is sent; #count cannot serve
    // for that, because work queued BEHIND the clear keeps #count above zero
    // and the clear would wait for operations that are waiting for it.
    #onWire = 0;
    #wireIdle = [];
    stats = { shed: 0, failed: 0, retried: 0, coalesced: 0 };

    constructor(adapter, { maxBytes = 8 << 20, retryMs = 2000, onError = null, now = Date.now } = {}) {
        this.#adapter = adapter; this.#maxBytes = maxBytes; this.#retryMs = retryMs;
        this.#onError = onError; this.#now = now;
    }

    // The cache this queue belongs to is going away.
    //
    // A `clear` retries INDEFINITELY by design (decision 70), so without a way
    // to say "stop", a failing clear keeps a retry loop running for the life of
    // the process -- against a cache that no longer exists, and after close()
    // has already returned. Two things are needed and neither substitutes for
    // the other: UNREF'ING the sleeping backoff, so the wait already in
    // progress stops holding the event loop the instant close begins, and the
    // `#closed` FLAG, so the loop ends at its next attempt instead of retrying
    // forever against a closed cache. Unref alone leaves the loop running;
    // the flag alone leaves the current backoff detaining the process.
    //
    // Backoffs are unref'd HERE rather than at creation because a queue still
    // in service must keep the loop alive for its own retries: with nothing
    // else ref'd, an unconditionally unref'd backoff lets the process exit
    // mid-retry, so `await setAsync(k, v)` against a failing L3 never resolves
    // and the work is dropped with no caller left to hear about it. Measured,
    // not theoretical -- it exits 0 in the middle of a test's assertions.
    close() {
        this.#closed = true;
        for (const t of this.#backoffs) { if (t.unref) t.unref(); }
        this.#backoffs.clear();
    }

    get pending() { return this.#count; }
    // Bytes currently OUTSTANDING -- queued PLUS in flight, not merely
    // queued. The synchronous pump (see push()) starts sending an op before
    // push() returns, so "queued" alone would undercount from the instant a
    // key goes idle-to-busy; #bytes is what maxBytes is actually checked
    // against, and it has to count a slow send in flight or a burst across
    // many distinct keys could grow this queue's outstanding work without
    // bound even though every individual key looks idle right after push().
    get pendingBytes() { return this.#bytes; }
    // Test-only: lets tests prove the per-key chain map does not accumulate
    // one entry per distinct key for the life of the process.
    get chainCount() { return this.#chains.size; }

    // What this queue still owes L3 for `key`: 'set', 'delete', or undefined.
    //
    // The read path consults this before promoting an L3 value, so it must be
    // ONE Map lookup and no allocation. It reports the NEWEST operation for the
    // key -- a queued one supersedes the one on the wire -- because that is what
    // L3 will end up holding, and it covers the in-flight case deliberately:
    // an operation already dispatched is precisely the one whose round trip a
    // concurrent read can overlap.
    outstandingKind(key) {
        const chain = this.#chains.get(key);
        if (chain === undefined) return undefined;
        if (chain.pending !== null) return chain.pending.kind;
        return chain.inflight === null ? undefined : chain.inflight.kind;
    }

    push(op) {
        // A clear is a BARRIER, not just another operation -- see #pushClear.
        if (op.kind === 'clear') return this.#pushClear(op);
        if (this.#bytes + op.bytes > this.#maxBytes) {
            this.stats.shed++;
            return Promise.resolve(false);
        }
        const id = op.key;
        let chain = this.#chains.get(id);
        if (chain === undefined) {
            chain = { pending: null, inflight: null, settle: null, busy: false };
            this.#chains.set(id, chain);
        }
        // COALESCING. If an operation for this key is queued but not yet sent
        // (a "waiting slot" filled while an earlier op for the same key is
        // still in flight), replace it: only the newest value matters, and
        // the caller of the superseded write gets the newer write's outcome.
        // Its promise then means "L3 holds your value, or a later one from
        // this process". An op already in flight (chain.pending === null
        // while chain.busy is true) is never touched here -- it is already
        // on the wire and cannot be un-sent.
        if (chain.pending !== null) {
            this.#bytes -= chain.pending.bytes; this.#count--;
            this.stats.coalesced++;
            const prevSettle = chain.settle;
            chain.pending = op;
            this.#bytes += op.bytes; this.#count++;
            return new Promise((resolve) => {
                chain.settle = (v) => { prevSettle(v); resolve(v); };
            });
        }
        chain.pending = op;
        this.#bytes += op.bytes; this.#count++;
        const result = new Promise((resolve) => { chain.settle = resolve; });
        // Kick the pump only if this key is idle. #run is async, so calling
        // it here runs synchronously up to its first real await -- which
        // clears chain.pending and starts the adapter call BEFORE this push()
        // returns. That is what makes ordering hold for two pushes to the
        // same key issued back to back with no await between them: the
        // second push sees chain.pending already claimed (null) and
        // chain.busy already true, so it correctly waits rather than racing
        // or wrongly coalescing an op that has already been sent.
        if (!chain.busy) { chain.busy = true; this.#run(id, chain); }
        return result;
    }

    // A CLEAR IS A BARRIER.
    //
    // Decision 66 leaves order ACROSS keys unenforced so that one slow key
    // cannot throttle the whole process. That rationale is about ordinary
    // per-key work; `clear` means *every* key, so a set that lands after it
    // resurrects exactly what it removed -- and spec 10.2 already says later
    // operations queue behind a clear. So, for `clear` only:
    //
    //   - work already ON THE WIRE is awaited before the clear is sent, since
    //     a set that lands afterwards puts back what the clear took out;
    //   - work PENDING BUT UNSENT is dropped and settles with the CLEAR's
    //     outcome -- the same rule coalescing already applies to a superseded
    //     write, whose promise means "L3 holds your value, or a later
    //     operation from this process", and a clear is such an operation;
    //   - work pushed AFTER the clear waits for it, which is what makes the
    //     spec's "later writes survive a clear" guarantee true.
    //
    // The dropping happens HERE, at push time, and not when the clear is
    // finally dispatched: an operation pushed after the clear must queue
    // behind it, not be swept up by it, and only push order can tell the two
    // apart. A clear is never shed, and it is counted before anything is
    // dropped so #count cannot dip to zero in between and let a drain()
    // resolve with a clear still outstanding.
    //
    // Nothing here can hang the process against a dead L3: work waiting behind
    // the clear keeps counting against maxBytes, so later pushes are shed and
    // settle false under the same contract as a full submission ring.
    #pushClear(op) {
        let chain = this.#chains.get(CLEAR_ID);
        if (chain === undefined) {
            chain = { pending: null, inflight: null, settle: null, busy: false };
            this.#chains.set(CLEAR_ID, chain);
        }
        this.#armGate();
        let result;
        if (chain.pending !== null) {
            // A clear not yet sent is replaced by this one, exactly as a write
            // would be: two flushes in a row are one flush. The work the
            // earlier clear dropped carries over to this one, so those callers
            // still settle -- and the earlier clear's hold on the gate is
            // released with it, since it will never reach #run to release it
            // itself.
            const prev = chain.pending, prevSettle = chain.settle;
            this.stats.coalesced++;
            op.superseded = prev.superseded;
            this.#bytes -= prev.bytes; this.#bytes += op.bytes;
            this.#disarmGate();
            chain.pending = op;
            result = new Promise((resolve) => {
                chain.settle = (v) => { prevSettle(v); resolve(v); };
            });
        } else {
            op.superseded = [];
            chain.pending = op;
            this.#bytes += op.bytes; this.#count++;
            result = new Promise((resolve) => { chain.settle = resolve; });
        }
        for (const settle of this.#dropPending()) op.superseded.push(settle);
        if (!chain.busy) { chain.busy = true; this.#run(CLEAR_ID, chain); }
        return result;
    }

    // Every operation queued but not yet sent, removed from its chain and
    // handed back so the clear can settle it with its own outcome. A chain
    // whose pending slot is emptied here is never orphaned: `pending !== null`
    // implies its #run loop is still running, and that loop exits and deletes
    // the chain the next time it finds the slot empty.
    #dropPending() {
        const settles = [];
        for (const [id, chain] of this.#chains) {
            if (id === CLEAR_ID || chain.pending === null) continue;
            this.#bytes -= chain.pending.bytes; this.#count--;
            // Counted as a coalesce for the same reason it behaves like one:
            // the operation never reaches L3 because a newer operation from
            // this process superseded it.
            this.stats.coalesced++;
            settles.push(chain.settle);
            chain.pending = null; chain.settle = null;
        }
        return settles;
    }

    // The gate is one promise shared by every waiter, created when the first
    // clear arrives and resolved when the last one settles. A count rather than
    // a boolean: with two clears outstanding, the first one finishing must not
    // let work through while the second is still on its way.
    #armGate() {
        this.#clears++;
        if (this.#clearGate === null) this.#clearGate = new Promise((r) => { this.#gateRelease = r; });
    }
    #disarmGate() {
        if (--this.#clears > 0) return;
        const release = this.#gateRelease;
        this.#clearGate = null; this.#gateRelease = null;
        release();
    }
    // Resolves once nothing is on the wire. See #onWire for why drain() cannot
    // stand in for this.
    #wireDrain() {
        return this.#onWire === 0 ? Promise.resolve() : new Promise((r) => this.#wireIdle.push(r));
    }

    async #run(id, chain) {
        while (chain.pending !== null) {
            // Everything but a clear waits while a clear is outstanding (see
            // #pushClear). `continue` rather than falling through: while we
            // waited, the clear may have dropped this very operation, and a
            // later push may have put a new one in its place.
            if (id !== CLEAR_ID && this.#clearGate !== null) { await this.#clearGate; continue; }
            const op = chain.pending, settle = chain.settle;
            chain.pending = null; chain.settle = null;
            // Claimed, so outstandingKind() keeps answering for this key while
            // the round trip below is in progress -- that window is exactly the
            // one the read path's guard exists to cover.
            chain.inflight = op;
            // A clear goes out only once the wire is empty: a set still in
            // flight would otherwise land after it and put back what it
            // removed. Deliberately outside the bounded attempt below -- this
            // is waiting for OUR OWN earlier work, each piece of which is
            // already bounded, not for the adapter.
            if (id === CLEAR_ID) await this.#wireDrain();
            // #bytes/#count keep counting `op` through the whole round trip,
            // not just while it sits queued: it is decremented below, after
            // the op settles, not here. If it were freed the moment #run
            // claims it, a burst of writes to many DISTINCT keys -- each
            // dispatched synchronously and immediately in flight -- would
            // never accumulate against the bound, and a slow L3 could grow
            // the queue's outstanding work without limit even though every
            // individual key looked "idle" the instant after its push().
            let outcome = false;
            this.#onWire++;
            // `now` is a user-supplied constructor option -- untrusted input
            // like any other. #run is invoked fire-and-forget (push() does
            // not await or .catch it), so a throw escaping this whole block
            // would both become an unhandled rejection (process termination
            // under Node 18's default) AND hang this op's caller forever,
            // since settle() would never run. The deadline computation used
            // to sit outside any try for exactly that reason -- `this.#now`
            // is called here too, not only inside the retry loop's catch --
            // so the try now wraps the whole per-op attempt, not just the
            // adapter call.
            try {
                const deadline = this.#now() + this.#retryMs;
                for (let attempt = 0; ; attempt++) {
                    // Each ATTEMPT is bounded, not just the retry budget: an
                    // adapter that neither resolves nor rejects would otherwise
                    // never reach the catch below, and this chain -- and every
                    // caller waiting on it -- would hang forever. The budget is
                    // `retryMs`, reused rather than given an option of its own:
                    // it is already documented as the per-operation time budget
                    // before an op is abandoned, and a call that has not
                    // settled within it has already exhausted that budget, so a
                    // second knob would only let the two disagree.
                    try { await withDeadline(this.#apply(op), this.#retryMs, op.kind); outcome = true; break; }
                    catch (e) {
                        // A clear retries past the budget on purpose: flushing
                        // twice is harmless, and until it lands this process
                        // must serve misses. Once close() has begun it stops
                        // anyway -- see close().
                        const mayRetry = !this.#closed && (op.kind === 'clear' || this.#now() < deadline);
                        if (!mayRetry) { this.stats.failed++; this.#report(e, op); break; }
                        // No report here: a failure still inside the retry budget
                        // is not yet an event a caller should act on. Reporting it
                        // anyway made onError fire twice for one abandoned op (once
                        // here, once more below) with no way for a listener to tell
                        // the two apart, AND made it fire once for an op that goes
                        // on to succeed -- nothing was lost, and stats.retried
                        // already makes that visible. onError now fires exactly
                        // once per operation, and only when the operation is
                        // actually abandoned.
                        this.stats.retried++;
                        // Registered with close(), which unrefs it. Ref'd while
                        // the queue is in service, so a retry cannot be dropped
                        // by a process that exits out from under it; unref'd the
                        // moment close begins, so it cannot be the reason a
                        // closed process stays alive. See close().
                        await new Promise((r) => {
                            const t = setTimeout(() => { this.#backoffs.delete(t); r(); },
                                                 Math.min(50 * (attempt + 1), 200));
                            if (this.#closed) { if (t.unref) t.unref(); }
                            else this.#backoffs.add(t);
                        });
                    }
                }
            } catch (e) {
                this.stats.failed++; this.#report(e, op); outcome = false;
            }
            if (--this.#onWire === 0) { const w = this.#wireIdle; this.#wireIdle = []; for (const r of w) r(); }
            this.#bytes -= op.bytes; this.#count--;
            chain.inflight = null;
            // Released BEFORE settle(), so a caller resuming on the clear's
            // promise finds the queue accepting work again rather than one
            // still holding everything back.
            if (id === CLEAR_ID) { for (const s of op.superseded) s(outcome); this.#disarmGate(); }
            settle(outcome);
            if (this.#count === 0) { const w = this.#idle; this.#idle = []; for (const r of w) r(); }
        }
        // Nothing left queued for this key -- provably true here: this is
        // the `while` loop's own exit condition, so `chain.pending === null`
        // cannot be false at this point and guarding the delete on it again
        // would be dead code. Worse than merely redundant: if some future
        // edit made the guard's false branch reachable, the code below it
        // would set `chain.busy = false` and return with an operation still
        // queued and no pump left running to send it -- exactly the one
        // failure mode a queue must not have (a promise that never settles).
        // So the delete is unconditional rather than re-guarded; the
        // invariant is enforced by the loop shape itself, not restated here.
        chain.busy = false;
        this.#chains.delete(id);
    }

    #apply(op) {
        const a = this.#adapter;
        if (op.kind === 'set') return a.set(op.key, op.value, { ttlMs: op.ttlMs || 0, originId: op.originId, willCache: op.willCache });
        if (op.kind === 'delete') return a.delete(op.key, { originId: op.originId });
        return a.clear();
    }

    #report(e, op) { if (this.#onError) { try { this.#onError(e, op); } catch { /* a listener must not break the queue */ } } }

    drain() { return this.#count === 0 ? Promise.resolve() : new Promise(r => this.#idle.push(r)); }
}
module.exports = { L3Queue, withDeadline };
