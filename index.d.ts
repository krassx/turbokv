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

export interface NamespaceOptions {
    name: string;
    /** Soft byte quota. A namespace under its quota is protected from eviction
     *  by a hotter one; over it, it competes normally. 0 means no quota. */
    quotaBytes?: number;
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
    /** Prefix and arena-level identity, optionally carrying a byte quota. */
    namespace?: string | NamespaceOptions;
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

export interface NamespaceStat {
    name: string; id: number; bytes: number; quota: number;
    protected: number; dropped: number;
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

    /** Undefined when no arena is attached (before open, or after close). */
    static arenaStats(): ArenaStats | undefined;
    static namespaceStats(): NamespaceStat[] | undefined;
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
    set(key: string, value: T, options?: SetOptions): boolean;
    has(key: string): boolean;
    /** Returns whether the key was present at call time. */
    delete(key: string): boolean;
    /** Drop this process's L1. The arena is untouched. */
    clearLocal(): void;
    /** Clear the whole arena and every process's L1. */
    clearAll(): void;
    clearNamespace(): number;

    /** Lazily enumerate keys. Not a snapshot: the arena may change mid-scan. */
    keys(options?: KeysOptions): Generator<string, void, unknown>;
    /** Counts by enumerating, so it is O(index slots), not a cached counter. */
    get size(): number;

    /** Which write path this handle negotiated. */
    get transport(): Transport;

    /** Push any buffered worker writes now. */
    flush(): void;
    /** Release the ring slot, stop the heap guard, deregister, and on the
     *  primary destroy the arena. */
    close(): void;

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
