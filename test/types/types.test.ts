// Type-level tests for index.d.ts.
//
// The `@ts-expect-error` lines are the point: a declaration file that accepts
// everything type-checks fine and is worthless. tsc reports TS2578 for an
// UNUSED expect-error, so this file only passes when each of those really is
// an error and every line above them really is not.
import { TurboKV, Cache, MSG } from 'turbokv';
import type { CacheOptions, StorageMode, Codec, CacheStats } from 'turbokv';

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
