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

// `clear` has no key of its own, so it needs a chain id that can never collide
// with a real string key. A Symbol guarantees that (no string a caller could
// ever pass as `key` is `===` to it), and gives every `clear` its own single
// chain rather than mixing it into any particular key's chain.
const CLEAR_ID = Symbol('l3-clear');

class L3Queue {
    #adapter; #maxBytes; #retryMs; #onError; #now;
    #chains = new Map();          // id -> { pending: op|null, settle: fn|null, busy: bool }
    #bytes = 0;
    #count = 0;
    #idle = [];
    stats = { shed: 0, failed: 0, retried: 0, coalesced: 0 };

    constructor(adapter, { maxBytes = 8 << 20, retryMs = 2000, onError = null, now = Date.now } = {}) {
        this.#adapter = adapter; this.#maxBytes = maxBytes; this.#retryMs = retryMs;
        this.#onError = onError; this.#now = now;
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

    push(op) {
        // A clear is never shed: until it lands, this process serves misses
        // rather than values the clear was meant to remove, so dropping it
        // would leave the process permanently blind.
        if (op.kind !== 'clear' && this.#bytes + op.bytes > this.#maxBytes) {
            this.stats.shed++;
            return Promise.resolve(false);
        }
        const id = op.kind === 'clear' ? CLEAR_ID : op.key;
        let chain = this.#chains.get(id);
        if (chain === undefined) {
            chain = { pending: null, settle: null, busy: false };
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

    async #run(id, chain) {
        while (chain.pending !== null) {
            const op = chain.pending, settle = chain.settle;
            chain.pending = null; chain.settle = null;
            // #bytes/#count keep counting `op` through the whole round trip,
            // not just while it sits queued: it is decremented below, after
            // the op settles, not here. If it were freed the moment #run
            // claims it, a burst of writes to many DISTINCT keys -- each
            // dispatched synchronously and immediately in flight -- would
            // never accumulate against the bound, and a slow L3 could grow
            // the queue's outstanding work without limit even though every
            // individual key looked "idle" the instant after its push().
            let outcome = false;
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
                    try { await this.#apply(op); outcome = true; break; }
                    catch (e) {
                        // A clear retries past the budget on purpose: flushing
                        // twice is harmless, and until it lands this process
                        // must serve misses.
                        const mayRetry = op.kind === 'clear' || this.#now() < deadline;
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
                        await new Promise(r => setTimeout(r, Math.min(50 * (attempt + 1), 200)));
                    }
                }
            } catch (e) {
                this.stats.failed++; this.#report(e, op); outcome = false;
            }
            this.#bytes -= op.bytes; this.#count--;
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
module.exports = { L3Queue };
