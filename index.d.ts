/**
 * turbokv — a layered in-memory KV cache for Node.js clusters.
 *
 * L1 is a per-process JS Map with a byte budget; L2 is a shared-memory arena
 * that the primary owns and workers map read-only. Worker writes travel to the
 * primary through per-worker shared-memory submission rings.
 *
 * These declarations are hand-written against turbokv.js. They describe the
 * PUBLIC surface only: anything prefixed with `_` is internal cross-instance
 * plumbing and is deliberately absent.
 */

/** How values are encoded on their way into the arena. */
export type StorageMode =
    /** No codec. Strings, numbers, booleans, null, BigInt and binary only;
     *  byte accounting is exact and there is no aliasing hazard. The default. */
    | 'bytes'
    /** `v8.serialize`. Full fidelity: Date, Map, Set, BigInt and typed arrays
     *  survive the round trip. Costs ~2-3x JSON and allocates per L2 hit.
     *  NOTE: the format is runtime-specific — Bun's differs from Node's and
     *  Deno's, so an arena written by one cannot be read by the other. */
    | 'direct'
    /** JSON. Silently degrades eight common types (Date to string, Map/Set to
     *  {}, NaN/Infinity to null, undefined dropped) and rejects BigInt and
     *  cycles. Fast and universally understood. */
    | 'safe'
    /** @deprecated Legacy alias for `'bytes'`. */
    | 'primitives';

/** How a worker's writes reach the primary. */
export type Transport =
    /** Per-worker shared-memory submission rings. The default. */
    | 'shm'
    /** Batched over the cluster IPC channel. Slower, and it shares the channel
     *  with the application's own messages. */
    | 'ipc';

/** A caller-supplied codec. Must not pass a replacer/reviver or indentation:
 *  a replacer costs 3.5x on Node 26 and is rejected unless `allowSlowCodec`. */
export interface Codec<T = unknown> {
    encode(value: T): string;
    decode(encoded: string): T;
}

/** Options passed to an {@link L3Adapter}'s `get`. */
export interface L3GetOptions {
    /** Whether the value will be cached locally (L1/L2) once it is returned.
     *  A `minLevel: L3` read passes `false`. */
    willCache?: boolean;
}

/** Options passed to an {@link L3Adapter}'s `set`. */
export interface L3SetOptions {
    /** Time to live in milliseconds. 0 (or absent) means no expiry. */
    ttlMs?: number;
    /** @see L3DeleteOptions.originId */
    originId?: string;
    /** @see L3GetOptions.willCache */
    willCache?: boolean;
}

/** Options passed to an {@link L3Adapter}'s `delete`. */
export interface L3DeleteOptions {
    /** Which arena originated the operation: the hex arena id, stable for the
     *  life of an arena and identical in the primary and every worker mapping
     *  it. A primary restart changes it, which is correct — a restarted primary
     *  has an empty cache. Use it to suppress a subscription's echo of this
     *  box's own writes. `undefined` only on a handle with no arena mapped. */
    originId?: string;
}

/** What an L3 `get` resolves with for a key it holds. */
export interface L3Record<T = unknown> {
    value: T;
    /** Milliseconds remaining until expiry, if the key has a TTL. */
    ttlMs?: number;
}

/**
 * A user-supplied remote store behind L1/L2. `get`, `set`, `delete` and
 * `clear` are required; `has`, `subscribe` and `close` are optional. `has`
 * falls back to `get` when absent (correct, and it transfers the value
 * needlessly). Validated once, at construction, so a malformed adapter is a
 * `TypeError` from `TurboKV.open`/`createPrimary`/`attachWorker`, never a
 * rejection discovered on the first cache miss.
 */
export interface L3Adapter<T = unknown> {
    get(key: string, options?: L3GetOptions): Promise<L3Record<T> | undefined | null>;
    set(key: string, value: T, options?: L3SetOptions): Promise<void>;
    delete(key: string, options?: L3DeleteOptions): Promise<void>;
    clear(): Promise<void>;
    /** Optional: `EXISTS`-shaped check. Falls back to `get` when absent. */
    has?(key: string): Promise<boolean>;
    /** Optional: push invalidations for cross-machine staleness. Without it,
     *  staleness relies on TTL alone. Called only by the primary, and resolves
     *  with the function that unsubscribes.
     *
     *  `onRemoteChange` carries a key another box changed. `onResync` means
     *  *invalidations were lost*, not "I reconnected": a provider that can
     *  replay calls it only when entries it never read were trimmed; one that
     *  cannot calls it on every disconnection, because it genuinely cannot
     *  tell. */
    subscribe?(onRemoteChange: (key: string) => void,
               onResync: () => void): Promise<() => void>;
    /** Optional: release any resources the adapter holds. */
    close?(): Promise<void> | void;
}

/** Bounds L1 by live heap measured after a collection, since the byte budget is
 *  only an estimate. Pass `false` to disable. Driven by a FinalizationRegistry,
 *  which works on Node, Bun and Deno, with a floor-polling backstop for when
 *  finalizers go quiet. */
export interface HeapGuardOptions {
    /** Shed L1 once live heap exceeds this fraction of the limit. Default 0.80. */
    maxHeapFraction?: number;
    /** Fraction of L1 bytes to release when it fires. Default 0.25. */
    shedFraction?: number;
    /** Minimum gap between guard evaluations. Collection frequency is the
     *  runtime's business (6.3/s on Node, 9.7/s on Bun, 28.6/s on Deno under
     *  identical churn) and sampling is not free — `v8.getHeapStatistics()`
     *  costs ~200ns on Node and Deno but 426µs–2.6ms on Bun. Default 500ms.
     *  The signal is process-wide, so the most eager subscriber sets the pace. */
    minIntervalMs?: number;
}

export interface CacheOptions<T = unknown> {
    /** @see StorageMode. Default `'bytes'`.
     *  @throws TypeError if the value is not one of the modes. */
    storage?: StorageMode;
    /** Legacy alias for `storage`.
     *  @throws TypeError if the value is not one of the modes. */
    values?: StorageMode;
    /** Explicit codec. Mutually exclusive with a `storage` mode that implies one.
     *  @throws TypeError unless it is an object with `encode` and `decode`
     *  functions — in particular `codec: 'direct'` is rejected, since the modes
     *  are named with `storage`. */
    codec?: Codec<T>;
    /** Permit a codec that takes a replacer/reviver or indentation. Off by default
     *  because it silently costs 2-3.5x. */
    allowSlowCodec?: boolean;
    /** Freeze cached objects so a caller mutating one raises instead of
     *  corrupting L1. Also neutralises Date/Map/Set mutators, which
     *  `Object.freeze` cannot reach. */
    freeze?: boolean;
    /** Decode once per `set` so L1 never aliases the caller's object. */
    isolate?: boolean;
    /** L1 byte budget for this process. Default 2MB. 0 disables L1. */
    l1MaxBytes?: number;
    /** Multiplier from encoded bytes to retained heap. Measured 2.79-3.21x for
     *  JSON-shaped objects. Default 3. */
    heapFactor?: number;
    heapGuard?: HeapGuardOptions | false;
    /** @see Transport. Default `'shm'`. */
    transport?: Transport;
    /** Bytes a worker may hold in the IPC outbox before shedding. Default 1MB. */
    outboxMaxBytes?: number;
    /** Bytes handed to `process.send` and not yet drained. 0 sends nothing.
     *  Default 8MB. */
    maxInFlightBytes?: number;
    /** How stale the primary's heartbeat may get before a worker detaches and
     *  serves L1 only. Default 5000ms. */
    primaryStaleMs?: number;
    /** Run the primary's maintenance timer (heartbeat, expiry sweep). */
    maintenance?: boolean;
    maintenanceMs?: number;
    sweepSlots?: number;
    sweepFullPassMs?: number;
    /** A remote store behind L1/L2. With one attached, the three tiers are
     *  ONE cache: `clearAll`/`clearAsync` empty it too, and reads fall
     *  through to it on an L1/L2 miss. Validated once, at construction. */
    l3?: L3Adapter<T>;
    /** How long a value stays in the local tiers after its L3 write failed,
     *  before it reverts to whatever L3 holds. Default 5000ms. */
    l3FailTtlMs?: number;
    /** TTL applied to a value filled into the local tiers from an L3 read
     *  that carried none of its own. Default 60000ms. */
    l3TtlMs?: number;
    /** Byte bound on the per-process L3 write/delete queue, outstanding
     *  (queued plus in flight). Past it, non-`clear` ops are shed under the
     *  same contract as a full submission ring; `clear` is never shed.
     *  Default 8MB. */
    l3QueueMaxBytes?: number;
    /** Time budget, per queued L3 operation, before it is abandoned and
     *  `onL3Error` fires. A `clear` ignores this and retries indefinitely.
     *  Default 2000ms. */
    l3RetryMs?: number;
    /** Called once per abandoned background L3 operation (a queued op past
     *  its retry budget, or a failed read). Never called synchronously from
     *  `set`/`get`/etc. — those report failure through their own return
     *  value or promise, and `lastError` is never touched by a background
     *  failure, which is why this listener exists. */
    onL3Error?: (error: unknown, op: { kind: 'get' | 'set' | 'delete' | 'clear' | 'has' | 'close'; key?: string }) => void;
    /** How long `close()` waits for the L3 queue to drain before closing the
     *  adapter anyway. A `clear` retries indefinitely and ignores
     *  `l3RetryMs`, so an unreachable L3 would otherwise hang `close()`
     *  forever. Default 5000ms; 0 waits without a bound. */
    l3CloseTimeoutMs?: number;
}

export interface PrimaryOptions<T = unknown> extends CacheOptions<T> {
    /** Number of worker submission ring slots. Default 32. */
    submitRings?: number;
    /** Bytes per submission ring. Default 1MB. A value larger than half a ring
     *  can never be delivered and `set` reports it as a rejection. */
    submitRingBytes?: number;
    /** Requires an addon built with `--turbokv_lz4=1`. Measured a poor trade;
     *  neither the default nor a build dependency. */
    compress?: boolean;
    compressMinBytes?: number;
    compressAccel?: number;
}

export interface OpenOptions<T = unknown> extends PrimaryOptions<T> {
    /** Segment name. Defaults to one derived from the application's identity so
     *  a restart reclaims its own segment. */
    name?: string;
    /** Total segment size. The data region takes whatever the metadata leaves,
     *  about 95-97% of this — it is no longer rounded down to a power of two. */
    arenaBytes?: number;
    /** Index slots. Rounded to a power of two: open addressing probes with a mask. */
    indexSlots?: number;
}

/** Cache levels. L1 is the calling process's own map, L2 the shared arena, L3
 *  the remote tier (not built yet). */
export type Level = 1 | 2 | 3;

export interface LevelOption {
    /** The lowest level this record may occupy, as a PREFERENCE rather than a
     *  contract.
     *
     *  On `set`, the value is written from this level upwards and not stored
     *  locally. On `get`, the value is looked up normally (L1, then L2, then L3)
     *  but only filled back down to this level — so a large or one-off read can
     *  be served without evicting the caller's working set.
     *
     *  A level that does not currently exist is clamped DOWN to the highest one
     *  that does, so the data is always stored: `L3` behaves as `L2` today and
     *  starts using L3 when that tier lands, and a worker that has lost its
     *  primary clamps to `L1`. A value that is not a level at all throws.
     *
     *  This only ever changes where a record lives and how fast it is reached —
     *  never which value is observed. Nothing is recorded with the entry, so one
     *  caller's preference never constrains another's. */
    minLevel?: Level;
}

export interface SetOptions extends LevelOption {
    /** Time to live. Clamped to ~24.8 days; longer values are capped, never
     *  wrapped into "no expiry". */
    ttlMs?: number;
}

export interface CacheStats {
    sets: number; deletes: number; misses: number;
    l1Hits: number; l2Hits: number;
    invalidated: number; expired: number;
    /** Writes accepted locally that never reached L2 (ring or channel full). */
    writesShed?: number;
    sent?: number; flushes?: number; flushDropped?: number; congested?: number;
    rejectedKey?: number; rejectedType?: number; rejectedSize?: number;
    heapShed?: number;
    /** Times this worker re-attached after losing its primary. */
    recoveries?: number;
    lastRecovery?: { sameArena: boolean; at: number } | null;

    // The L3 counters are declared EXPLICITLY, not left to the index signature
    // below: an index signature of `unknown` types every one of them as
    // `unknown`, so `stats.l3Hits > 0` does not compile and the only way to
    // read a counter this library maintains is to cast it.
    /** Reads answered by L3 after an L1 and L2 miss. */
    l3Hits?: number;
    /** Reads L3 did not answer: absent, failed, or refused by a pending clear. */
    l3Misses?: number;
    /** Writes handed to the L3 queue. */
    l3Sets?: number;
    /** L3 writes abandoned past `l3RetryMs`, or shed past `l3QueueMaxBytes`. */
    l3SetFailed?: number;
    /** L3 deletes abandoned past `l3RetryMs`. */
    l3DeleteFailed?: number;
    /** Failed L3 writes whose local copy was re-timed to `l3FailTtlMs`. */
    l3FailTtlApplied?: number;
    /** L3 hits returned to the caller but not promoted, because the key was
     *  invalidated while the read was in flight or the ring could not rule it
     *  out. Ordinary contention, not an error. */
    l3PromotionsBlocked?: number;
    /** L3 hits not promoted because the key cannot live in L1 or L2 at all
     *  (an unpaired surrogate the arena cannot hash). */
    l3UnhashableKeys?: number;
    /** L3 hits discarded because this process deleted the key while the read
     *  was in flight. */
    l3DeletedWhileReading?: number;
    /** L3 hits discarded because a `clearAll()` -- issued by ANY process
     *  sharing this arena, not necessarily this one -- was handed to L3 while
     *  this read was in flight and had not landed there yet. Serving that
     *  value, or promoting it, would put back exactly what the clear is
     *  removing. A count of blocked reads, not a measure of how many clears
     *  are outstanding. */
    l3ClearedWhileReading?: number;
    [k: string]: unknown;
}

export interface ArenaStats {
    mode: number; live: number; inserts: number; evictions: number;
    reappends: number; reappendSkippedNoRoom: number; dropped: number;
    tailAdvances: number; tailLive: number;
    liveBytes: number; dataBytes: number;
    logHead: number; logTail: number; indexSlots: number; ringHead: number;
    [k: string]: number;
}

export interface AutoSize {
    arenaBytes: number; indexSlots: number; l1MaxBytes: number;
}

export interface SubmitStats {
    pushed: number; applied: number; shed: number; corrupt: number;
    rings: number; enabled: number; ringIndex: number;
}

export interface KeysOptions {
    /** Maximum keys to yield. Default 1000. */
    limit?: number;
    /** Index slots scanned per native call. Default 512. */
    batch?: number;
}

/**
 * A cache handle. Every method is synchronous.
 *
 * `set` never throws: an unusable key, value or type is reported as `false`
 * with the reason in `lastError`.
 */
export declare class TurboKV<T = unknown> {
    constructor(options?: CacheOptions<T>);

    /** Create the arena and become its sole writer. Call before forking. */
    static createPrimary<V = unknown>(
        name: string, arenaBytes: number, indexSlots: number, options?: PrimaryOptions<V>
    ): TurboKV<V>;

    /** Attach read-only from a worker. `workerId` must be an integer >= 1;
     *  0 is the primary and is rejected. */
    static attachWorker<V = unknown>(
        name: string, workerId: number, options?: CacheOptions<V>
    ): TurboKV<V>;

    /** Create or attach automatically, choosing the role from `cluster`. */
    static open<V = unknown>(options?: OpenOptions<V>): TurboKV<V>;

    /** Wire the primary to apply worker batches. Idempotent. */
    /** @see LevelOption */
    static readonly L1: 1;
    /** @see LevelOption */
    static readonly L2: 2;
    /** @see LevelOption */
    static readonly L3: 3;

    static install(cluster: unknown): void;

    /** Whether `m` is one of turbokv's own cluster messages.
     *
     *  `install()` is the easy path and takes over the primary's `message`
     *  handling. If your application already routes cluster messages itself,
     *  use this to pick turbokv's out of your own handler and pass them to
     *  {@link applyBatch}. The two are a pair: identifying a message is only
     *  useful if you can also apply it. */
    static isCacheMessage(m: unknown): boolean;

    /** Apply a worker's batch to L2, from your own `message` handler.
     *  Only call this for messages {@link isCacheMessage} accepted, and only on
     *  the primary. `install()` does exactly this for you. */
    static applyBatch(m: unknown): void;

    /** Tell the primary that a worker is gone, so any L3 `clear` it had in
     *  flight is settled rather than left blocking L3 reads in every process
     *  for the life of the arena. Returns how many were settled, and is
     *  idempotent.
     *
     *  Pass a cache message from that worker -- any one of them, so keep the
     *  last one you routed. A worker is identified by its ATTACHMENT, not by
     *  its writer id: ids are yours to choose and are normally reused across
     *  restarts, so releasing by one would settle whatever worker holds that
     *  slot now, which may be a live successor with a clear of its own in
     *  flight. A bare id is accepted but names only a sender that never
     *  identified itself, such as a hand-built batch.
     *
     *  `install()` calls this on a worker's `'exit'` and `'disconnect'`. Call
     *  it yourself only if you route cluster messages yourself, the way
     *  {@link applyBatch} is called -- the two are a pair. */
    static releaseWorker(who: unknown): number;

    /** Undefined when no arena is attached (before open, or after close). */
    static arenaStats(): ArenaStats | undefined;
    static submitStats(): SubmitStats | null;
    /** Milliseconds since the primary last stamped its heartbeat; -1 if never. */
    static primaryAgeMs(): number;
    static autoSize(): AutoSize;
    static defaultName(): string;
    /** Whether the loaded addon was built with LZ4. */
    static hasCompression(): boolean;
    /** Freeze deeply, neutralising Date/Map/Set mutators that Object.freeze
     *  cannot reach. Exposed because the codec modes use it. */
    static deepFreeze<V>(value: V): V;
    /** Throws if a codec takes a replacer/reviver or indentation. */
    static assertFastCodec(codec: Codec<unknown>): void;
    /** Built-in codecs, for callers who want one explicitly. */
    static readonly JSON_CODEC: Codec<unknown>;
    static readonly V8_CODEC: Codec<unknown>;
    /** Apply pending worker submissions on the primary. Returns records applied. */
    static drainSubmissions(budget?: number): number;
    /** Heap-guard cadence: evaluations performed, finalizer signals dropped by
     *  the debounce, and the interval currently in force. */
    static heapGuardPace(): { evaluations: number; debounced: number; minIntervalMs: number };

    get(key: string, options?: LevelOption): T | undefined;
    /** `get`, then L3 on a local miss (if an adapter is attached), filling
     *  L1/L2 back down to `options.minLevel`. Identical effects to `get`; the
     *  only difference is that this one can wait for L3. Concurrent misses on
     *  one key inside this process share a single L3 request. */
    getAsync(key: string, options?: LevelOption): Promise<T | undefined>;
    set(key: string, value: T, options?: SetOptions): boolean;
    /** `set`, then L3 (if an adapter is attached), resolving on the L3
     *  outcome. Identical effects to `set`; the only difference is what the
     *  caller can wait for — the value is in the local tiers before this
     *  promise even settles. Resolves `false` when the L3 write failed (the
     *  local write still stands), never throws. */
    setAsync(key: string, value: T, options?: SetOptions): Promise<boolean>;
    has(key: string): boolean;
    /** `has`, then L3 on a local miss (if an adapter is attached). Uses the
     *  adapter's `has` when present, otherwise falls back to `get` and
     *  discards the value. */
    hasAsync(key: string): Promise<boolean>;
    /** Returns whether the key was present at call time. */
    delete(key: string): boolean;
    /** `delete`, then L3 (if an adapter is attached), resolving once the L3
     *  delete has settled. Identical effects to `delete`; the only
     *  difference is what the caller can wait for. If L3 is unreachable the
     *  local delete still stands for the whole outage — there is no local
     *  tombstone to revert from the way a failed `setAsync` has one. */
    deleteAsync(key: string): Promise<boolean>;
    /** Drop this process's L1. The arena is untouched. */
    clearLocal(): void;
    /** Clear the whole arena and every process's L1. With an adapter
     *  attached this also clears L3 — the tiers are one cache, so clearing
     *  only the local ones would be undone by the next read. Until the L3
     *  clear lands, L3 reads in this process serve misses rather than the
     *  values the clear was meant to remove. */
    clearAll(): void;
    /** `clearAll`, then L3 (if an adapter is attached), resolving once the L3
     *  clear has landed. A clear is never shed by the L3 queue and retries
     *  indefinitely, so this promise always eventually resolves `true`. */
    clearAsync(): Promise<boolean>;

    /** Wait for every operation currently queued for L3 (of any kind, for
     *  any key) to settle. No-op, resolving immediately, when no adapter is
     *  attached. */
    drainL3(): Promise<void>;

    /** Lazily enumerate keys. Not a snapshot: the arena may change mid-scan. */
    keys(options?: KeysOptions): Generator<string, void, unknown>;
    /** Live entries in the arena, from the arena-wide counter. */
    get size(): number;

    /** Which write path this handle negotiated. */
    get transport(): Transport;

    /** Push any buffered worker writes now. */
    flush(): void;
    /** Drains the L3 queue (bounded by `l3CloseTimeoutMs`), closes the L3
     *  adapter if one is attached, then releases the ring slot, stops the
     *  heap guard, deregisters, and on the primary destroys the arena. An
     *  open L3 connection keeps the event loop alive, which is why this
     *  returns a promise; a caller that ignores it is unaffected. */
    close(): Promise<void>;

    /** Entries currently held in this process's L1. `size` counts what the
     *  cache can serve; this counts only what is resident locally. */
    readonly l1Size: number;

    readonly stats: CacheStats;
    /** Why the last operation failed, or null. Mutable: the library overwrites
     *  it, and a caller may clear it. */
    lastError: string | null;
    /** Live heap fraction sampled after the last collection. 0 until the guard
     *  has taken its first reading. */
    readonly liveHeapFraction: number;
    /** True when this handle has lost its primary and is serving L1 only. */
    readonly primaryDead: boolean;
    /** The storage mode in force. */
    readonly storage: StorageMode;

    /** Stop this cache's heap-guard subscription without closing it. */
    stopGuard(): void;
}

export { TurboKV as Cache };
/** Message tag used on the cluster channel. */
export declare const MSG: string;
export default TurboKV;
