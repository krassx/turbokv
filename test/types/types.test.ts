// Type-level tests for index.d.ts.
//
// The `@ts-expect-error` lines are the point: a declaration file that accepts
// everything type-checks fine and is worthless. tsc reports TS2578 for an
// UNUSED expect-error, so this file only passes when each of those really is
// an error and every line above them really is not.
import { TurboKV, Cache, MSG } from 'turbokv';
import type { CacheOptions, StorageMode, Codec, CacheStats, L3Adapter, L3SetOptions } from 'turbokv';

// --- inference -------------------------------------------------------------
const c = TurboKV.createPrimary<{ a: number }>('/t', 1, 1, { storage: 'bytes' });
const v = c.get('k');                       // {a:number} | undefined
const n: number | undefined = v?.a;
const okSet: boolean = c.set('k', { a: 1 });
const present: boolean = c.has('k');
const removed: boolean = c.delete('k');
const size: number = c.size;
const t: 'shm' | 'ipc' = c.transport;
const s: CacheStats = c.stats;
const hits: number = s.l1Hits;
const err: string | null = c.lastError;
for (const key of c.keys({ limit: 10 })) { const k: string = key; void k; }
const strC = TurboKV.createPrimary<string>('/t2', 1, 1, {});
const sv: string | undefined = strC.get('k');
const codec: Codec<{ a: number }> = { encode: (x) => JSON.stringify(x), decode: (x) => JSON.parse(x) };
const opts: CacheOptions<{ a: number }> = { codec, freeze: true, l1MaxBytes: 1024 };
const mode: StorageMode = 'direct';
void [n, okSet, present, removed, size, t, hits, err, sv, opts, mode, Cache, MSG];

const dead: boolean = c.primaryDead;
const mode2: StorageMode = c.storage;
// arenaStats is undefined before an arena exists
const st2 = TurboKV.arenaStats();
const liveCount: number | undefined = st2?.live;
void [dead, mode2, liveCount];

// --- these MUST be errors --------------------------------------------------
// @ts-expect-error storage mode is a closed set
const bad1 = TurboKV.createPrimary('/t', 1, 1, { storage: 'nope' });
// @ts-expect-error value must match the cache's type parameter
const bad2 = c.set('k', { a: 'string-not-number' });
// @ts-expect-error get returns T | undefined, not T
const bad3: { a: number } = c.get('k');
// @ts-expect-error no such method
const bad4 = c.nonexistentMethod();
// @ts-expect-error internal plumbing must not be part of the public surface
const bad5 = c._dropByHash('x');
// @ts-expect-error transport is a closed set
const bad6: CacheOptions = { transport: 'carrier-pigeon' };
void [bad1, bad2, bad3, bad4, bad5, bad6];
// @ts-expect-error arenaStats can be undefined
const bad8: number = TurboKV.arenaStats().live;
void [bad8];

// --- the async surface, and close() returning a promise --------------------
const ga: Promise<{ a: number } | undefined> = c.getAsync('k');
const sa: Promise<boolean> = c.setAsync('k', { a: 1 });
const da: Promise<boolean> = c.deleteAsync('k');
const ha: Promise<boolean> = c.hasAsync('k');
const ca: Promise<boolean> = c.clearAsync();
const cl: Promise<void> = c.close();
// @ts-expect-error getAsync returns a promise, not a bare value
const bad10: { a: number } | undefined = c.getAsync('k');
void [ga, sa, da, ha, ca, cl, bad10];

// --- onL3Error's op.kind is a closed set that must match every kind the
//     runtime actually reports: 'get' (a failed read), 'set'/'delete'/
//     'clear' (abandoned queued writes), 'has' (fallback-read failure), and
//     'close' (a failing adapter.close()). 'close' used to be missing from
//     the declared union while close() already emitted it at runtime -- a
//     TypeScript consumer narrowing on op.kind would hit a value the type
//     ruled out.
//
// Pinned by assignability, not by a runtime-style `===` chain: a chain of
// `op.kind === '...' || ...` narrows op.kind via control flow as each arm is
// excluded, so by the LAST comparison in the chain TS has already narrowed
// it to `never` and silently accepts a comparison against a kind that was
// never declared -- an equality check against a value already narrowed to
// `never` raises no error. Extracting the declared kind type and assigning
// each literal to it individually has no such narrowing and fails to
// compile the moment a real kind (in either direction) is missing from it.
type L3ErrorKind = NonNullable<Parameters<NonNullable<CacheOptions['onL3Error']>>[1]>['kind'];
const kGet: L3ErrorKind = 'get';
const kSet: L3ErrorKind = 'set';
const kDelete: L3ErrorKind = 'delete';
const kClear: L3ErrorKind = 'clear';
const kHas: L3ErrorKind = 'has';
const kClose: L3ErrorKind = 'close';
void [kGet, kSet, kDelete, kClear, kHas, kClose];
// @ts-expect-error op.kind is closed -- 'subscribe' is never a queued or reported L3 op
const kSubscribe: L3ErrorKind = 'subscribe';
void kSubscribe;

// --- the L3 counters are usable as numbers ---------------------------------
//
// CacheStats carries an `[k: string]: unknown` index signature, so a counter
// that is only reachable through it types as `unknown` and cannot be compared,
// added or formatted without a cast. Declaring each one explicitly is what
// makes `stats.l3Hits > 0` compile, and these lines fail the moment a counter
// falls back to the index signature again.
const l3Hits: number | undefined = c.stats.l3Hits;
const l3Misses: number | undefined = c.stats.l3Misses;
const l3Sets: number | undefined = c.stats.l3Sets;
const l3SetFailed: number | undefined = c.stats.l3SetFailed;
const l3DeleteFailed: number | undefined = c.stats.l3DeleteFailed;
const l3FailTtl: number | undefined = c.stats.l3FailTtlApplied;
const l3Blocked: number | undefined = c.stats.l3PromotionsBlocked;
const l3Unhashable: number | undefined = c.stats.l3UnhashableKeys;
const l3DelReading: number | undefined = c.stats.l3DeletedWhileReading;
const l3Clearing: number | undefined = c.stats.l3ClearedWhileReading;
const released: number = TurboKV.releaseWorker(1);
const anyHit: boolean = (c.stats.l3Hits ?? 0) > 0;
void [l3Hits, l3Misses, l3Sets, l3SetFailed, l3DeleteFailed, l3FailTtl,
     l3Blocked, l3Unhashable, l3DelReading, l3Clearing, released, anyHit];

// --- the adapter contract matches spec section 4 ---------------------------
//
// `subscribe` takes (onRemoteChange, onResync) and resolves with the
// unsubscribe function; `originId` is the hex arena id, a string. Nothing
// calls subscribe yet, so the declared shape is exactly what adapter authors
// will implement against -- which is the only reason getting it wrong is
// expensive later and free to fix now.
const adapter: L3Adapter<string> = {
    async get(key, o) { void [key, o?.willCache]; return { value: 'v', ttlMs: 10 }; },
    async set(key, value, o) {
        const origin: string | undefined = o?.originId;
        void [key, value, origin, o?.ttlMs, o?.willCache];
    },
    async delete(key, o) { const origin: string | undefined = o?.originId; void [key, origin]; },
    async clear() {},
    async has(key) { return key.length > 0; },
    async subscribe(onRemoteChange, onResync) {
        onRemoteChange('k'); onResync();
        return () => {};
    },
    async close() {},
};
const withL3: CacheOptions<string> = { l3: adapter, l3FailTtlMs: 100, l3RetryMs: 50 };
void [adapter, withL3];
// @ts-expect-error originId is the arena id, a string, not a worker number
const badOrigin: L3SetOptions = { originId: 7 };
void badOrigin;
// @ts-expect-error subscribe resolves with an unsubscribe function, not void
const badSub: L3Adapter<string> = { ...adapter, subscribe: async () => {} };
void badSub;
