# turbokv — Design

> Status: **draft, pre-implementation**. Nothing in this repo is built yet.
> All performance numbers were measured on the target machine (Apple Silicon, Node 24.15.0, V8 13.6.233) — see [Measurements](#measurements).

---

## Reading this document

Sections 1–8 describe **what the system is now** and are kept current. Section 9
is the **measurement record** and section 12 the **decision log** — both are
append-only history, including ideas that were built and then rejected, and they
are deliberately not rewritten when the design moves on. Section 13 tracks what
is still open.

If you only read one thing: §3 for the shape, §12 for why it is that shape.

---

## 1. Understanding

**What.** A layered in-memory key/value cache for Node.js services running under `node:cluster`. Two tiers: **L1**, private to each worker; **L2**, owned by the primary process and exposed to workers as a read-only shared memory mapping. A future **L3** (Valkey/Redis) sits behind both.

**Why.** Existing options force a bad choice: per-worker caches (`lru-cache`) duplicate data N times with no coherence, and out-of-process stores (Redis over a socket) cost ~50–200µs per hit. The gap is a cache that is *shared* across workers but served at *memory* latency.

**Who.** Node services on multi-core hosts, caching values hot enough that decode cost matters.

**Non-goals for v1.** Persistence. Distribution. Async APIs.

*Two of the original non-goals were reached anyway and are now supported:*
arbitrary JS object values (the `direct` and `safe` codec modes, §9) and Windows
(every OS call is behind `src/platform.h`, and `windows-latest` is in CI). A
third, atomic read-modify-write, was reached (`incr`/`cas`) and then reversed —
see decision 62. The cache also runs unmodified on **Bun** and **Deno** — the
addon is Node-API, so one binary serves all three — with the caveats in the
runtime notes of the README.

---

## 2. Public API (v1)

The authoritative declaration is `index.d.ts`, which is type-checked in CI
against deliberate misuses (`test/types/`). This section is the shape and the
reasoning; the file is the contract.

```js
const { Cache } = require('turbokv');   // alias of TurboKV
// or: import { TurboKV } from 'turbokv';
const cluster = require('cluster');

// Primary, BEFORE forking. Sizes itself from the machine, names its own
// segment, and passes the name to workers through the environment.
const cache = Cache.open();
Cache.install(cluster);          // the entire primary-side wiring

// Worker. Same call; it detects that it is a worker and attaches. Throws if the
// primary has not opened the arena yet: a worker must never create one, or it
// silently shadows the primary's.
const cache = Cache.open();
```

```ts
class Cache<T> {
  constructor(opts?: {
    storage?: 'bytes' | 'direct' | 'safe';   // default 'bytes'
    codec?: { encode, decode };              // explicit codec instead of a preset
    allowSlowCodec?: boolean;                // permit a replacer/reviver; default false
    freeze?: boolean;                        // codec modes; also neutralises
                                             //   Date/Map/Set mutators
    isolate?: boolean;                       // decode per set, so L1 never aliases
    l1MaxBytes?: number;                     // default heapLimit x 0.5%, 512KB..2MB
    heapFactor?: number;                     // encoded bytes -> retained heap; ~3
    heapGuard?: false | { maxHeapFraction?, shedFraction?, minIntervalMs? };
    transport?: 'shm' | 'ipc';               // default 'shm'
    outboxMaxBytes?: number;                 // IPC fallback buffer; default 1MB
    maxInFlightBytes?: number;               // IPC send window; default 8MB
    primaryStaleMs?: number;                 // degrade threshold; default 5000
    maintenance?: boolean;                   // primary heartbeat + expiry sweep
  });

  get(key: string): T | undefined;
  set(key: string, value: T, opts?: { ttlMs?: number }): boolean;   // never throws
  has(key: string): boolean;
  delete(key: string): boolean;              // whether it was present at call time
  clearLocal(): void;                        // this process's L1 only
  clearAll(): void;                          // the shared arena AND every L1
  flush(): void;                             // push buffered worker writes now
  close(): void;

  keys(opts?: { limit?, batch? }): Generator<string>;   // the whole arena
  readonly size: number;                     // the arena-wide live-entry counter
  readonly transport: 'shm' | 'ipc';         // what this handle negotiated
  readonly stats: CacheStats;                // hits, misses, writesShed,
                                             //   recoveries, lastRecovery, ...
  readonly lastError: string | null;
  readonly liveHeapFraction: number;         // 0 until the guard has sampled

  static open(opts?): Cache;                 // create (primary) or attach (worker)
  static createPrimary(name, arenaBytes, indexSlots, opts?): Cache;
  static attachWorker(name, workerId, opts?): Cache;   // workerId >= 1; 0 is the
                                                       //   primary and is refused
  static install(cluster): void;             // idempotent
  static drainSubmissions(budget?): number;  // primary: apply worker records
  static arenaStats(): ArenaStats;
  static submitStats(): SubmitStats | null;
  static heapGuardPace(): { evaluations, debounced, minIntervalMs };
  static primaryAgeMs(): number;             // heartbeat age; -1 if never stamped
  static autoSize(): { arenaBytes, indexSlots, l1MaxBytes };
  static defaultName(): string;              // derived from app identity
  static hasCompression(): boolean;          // was the addon built with LZ4
}
```

Everything is **synchronous**. Async variants are reserved for L3 (§13.9) and
deliberately not stubbed, because a `Promise`-returning shim over a synchronous
call costs 100–300ns of allocation and a microtask tick on a ~21ns operation.

---

## 3. Architecture

```
┌─ primary process ─────────────────────────────────────────────────┐
│  L2 arena   shm, mapped READ/WRITE — the primary is the sole writer│
│    ├── header      magic (published last), geometry, heartbeat,    │
│    │               arenaId, log and eviction stats                 │
│    ├── hash index  open addressing, backward-shift delete, 75% cap │
│    ├── invalidation ring   primary → workers, carries writerId     │
│    └── data        circular log, bounded second-chance re-append   │
│  hints segment     CLOCK reference bits; workers map this O_RDWR   │
│  submission rings  one SPSC ring per worker slot, drained here     │
│  maintenance timer heartbeat + a slice of TTL expiry per tick      │
└───────────────────────────────────────────────────────────────────┘
     ▲ records (shared memory)          │ arena mapped READ-ONLY
     │ + edge-triggered doorbell        │ hints mapped READ-WRITE
     │                                  ▼
┌─ worker process ──────────────────────────────────────────────────┐
│  L1   JS Map<string, entry>, FIFO + budgeted second chance         │
│       byte budget, plus a post-collection heap guard as backstop   │
│  reads   seqlock + monotonic log position over the arena           │
│  writes  encode → push into this worker's submission ring          │
│  coherence  drain the invalidation ring; drop matching L1 entries  │
└───────────────────────────────────────────────────────────────────┘
```

### Repository layout

```
index.js  index.mjs  index.d.ts   entry points; consumers never see src/
binding.gyp                       addon build, at the package root
src/   turbokv.js              the JS layer: L1, coherence, transports
       native.js                  the one place the addon is resolved
       binding.cc  *.h            arena, submission rings, platform layer
       vendor/                    rapidhash, verbatim upstream
test/  *_test.js  run.js          the suite; npm test and CI both use run.js
       *.cc                       standalone C++ tests (arena, rings)
       tsan/  types/              sanitizer gate, declaration tests
bench/                            microbenchmarks and design experiments
loadtest/                         sustained multi-worker load harness (Docker)
scripts/                          build helpers
```

### Why the primary is the only writer

This is the load-bearing decision. A single writer means:

- **No cross-process locks.** No shared allocator under contention, no write-side seqlock, no CAS loops.
- **No robust-mutex problem.** macOS has no `PTHREAD_MUTEX_ROBUST`. With multiple writers, a `SIGKILL`ed worker holding a lock deadlocks the arena permanently, and there is no portable recovery. With one writer, that failure mode does not exist.
- **A worker physically cannot corrupt L2.** The arena fd is opened `O_RDONLY`, so the restriction is enforced by the kernel rather than by discipline. Workers *do* map two things read-write — the hints segment and their own submission ring — and both are treated as untrusted input by the primary (decisions 18, 36c).
- **The primary is off the read path**, so it is not a throughput bottleneck. It only absorbs writes.

The cost is that a worker's `set()` reaches L2 asynchronously. For a cache, that is the right trade.

---

## 4. L1 — per-worker

L1 is a **JS `Map`**, not a native store. Measurements drove this:

| L1 strategy | hit, 200B | hit, 16KB | allocation per hit |
|---|---|---|---|
| **JS `Map`** | **~20 ns** | **~21 ns** | **none** |
| native, copy out | 30 ns | 439 ns | new V8 string every hit |
| native, external string per call | 58 ns | 59 ns | new external + finalizer every hit |

A native store must build a fresh V8 string on **every hit** — so it does not avoid GC pressure, it *manufactures* it. A `Map` returns the identical immutable value with zero allocation.

> **Not built:** decision 6 proposed a second tier holding large values as
> external strings over a per-worker off-heap arena, created once at insert.
> It was never implemented — there is no `externMinBytes` and no external-string
> path in the code. The idea is recorded there for its measurements; the
> eviction-becomes-GC-gated problem it introduces is why it stayed unbuilt.

L1 keys are JS strings in a `Map` — V8 already hashes and caches those. **rapidhash is used for L2 only.**

Eviction is **FIFO with a budgeted second chance**, not strict LRU: strict LRU in a JS `Map` means `delete`+`set` on every hit (~50–80ns), which would triple hit cost. A per-entry counter bumped on read is ~1ns, and an entry that has been re-read gets one reprieve — bounded, because a reprieve frees no bytes and an unbounded one makes a single insert walk the whole map (decision 34b).

Finding the oldest entry uses a **retained** `Map` iterator. A fresh `entries().next()` per eviction is O(n): V8 tombstones deletions and only compacts on rehash, so each call re-scans the hole prefix. That made cold reads get *slower* as L1 grew (decision 34).

The byte budget is exact in `bytes` mode and a `heapFactor` estimate whenever a codec is in use, so a **post-collection heap guard** backs it up — driven by a `FinalizationRegistry` rather than gc performance entries, which two of the three supported runtimes never emit (decision 43).

---

## 5. L2 — shared arena

Created by the primary before forking. `shm_open` (mode 0600) + `ftruncate` + `mmap`; workers reopen `O_RDONLY` and `mmap` with `PROT_READ`.

### Entry layout

```c
struct Entry {
  uint32_t seq;         // seqlock: even = stable, odd = write in progress
  uint64_t hash;        // rapidhash64(key)
  uint32_t version;     // bumped per write; matches invalidation ring
  uint32_t expiresAt;   // MILLISECONDS from the arena epoch; 0 = no TTL
  uint32_t rawLen;      // uncompressed length
  uint32_t storedLen;   // on-disk length (== rawLen if uncompressed)
  uint32_t blockSize;   // total bytes of this record, for the tail walk
  uint16_t keyLen;
  uint8_t  flags;       // COMPRESSED | STRING | LATIN1 | NUMBER | BOOL | NULL | BIGINT
  // ... keyLen bytes of key text, then storedLen bytes of value
};  // 40-byte header (CLOCK bits live in the hints segment)
```

**Key text is stored.** It costs bytes, and it buys three things: exact `memcmp` verification (so a 64-bit hash is *exactly* correct, no collision risk, rather than probabilistic); key enumeration remains possible; and the future L3 can use real Redis keys instead of opaque hash hex.

`IS_LATIN1` matters: `napi_create_string_latin1` is materially cheaper than UTF-8 decoding, so latin1 values take a faster read path.

### Data region: circular log with second chance

Entries are appended at a monotonic head; eviction advances the tail. There is
**no free list and no fragmentation** — a variable-size record never has to fit
a fixed class, so the calcification that strands memory in a size-class
allocator cannot occur.

Plain FIFO eviction, though, is frequency-blind, which costs it real hit rate.
So when the tail reaches a live entry whose CLOCK reference bit is set, the
entry is **re-appended at the head** and its bit cleared, rather than dropped —
CLOCK's second chance, expressed on a log. Re-appends are capped (16 per
allocation, measured to saturate at 8) so a fully-hot arena still makes progress.

A record must not straddle the wrap point, so the tail of the buffer is filled
with a pad record when the next entry will not fit. Note that masking also
quantised capacity to powers of two until decision 44 replaced it with a modulo. Note that in this mode a
re-append advances the head, so any write position must be recomputed *after*
the eviction loop, not before — getting this wrong silently corrupts entries,
and it is the one bug the prototype actually hit.

### Seqlock read (worker side)

```
do {
  s1 = load_acquire(e.seq);
  if (s1 & 1) continue;              // writer mid-update
  bounds-check keyLen/storedLen against arena size
  memcpy header + key + value into a local scratch buffer
  s2 = load_acquire(e.seq);
} while (s1 != s2);
memcmp(scratch.key, requestedKey)    // verify, then decompress
```

Copy-then-validate is required: a torn read must be discarded, not acted on. The bounds check is defensive — the primary is trusted, but a corrupt length would otherwise be an out-of-bounds read.

### Invalidation ring

Rather than `worker.send()` per write per worker (`O(writes × workers)` IPC messages), the ring lives **in the arena**:

```c
struct Ring { _Atomic uint64_t head; uint32_t capacity; Record records[]; };
struct Record { uint64_t hash; uint32_t version; uint16_t writerId; };
```

The primary appends on every write — `O(1)` regardless of worker count, zero IPC. Each worker keeps a private cursor and drains lazily at the top of each `get`/`set`: one relaxed load of `head`, a hot and usually-unchanged cache line. Records carry `writerId` so a worker skips invalidations caused by its own writes.

If `head - cursor > capacity` the ring has wrapped past that worker; it flushes its entire L1. Safe, self-correcting, and bounded.

---

## 6. Data flow

**`get(key)`**
1. Drain the invalidation ring (one atomic load, usually a no-op) and, at most every 500ms, check the primary's heartbeat.
2. `l1.get(key)` → hit: check TTL against a monotonic clock, bump the reference bit, return. **~21ns.**
3. Miss → native L2 probe: rapidhash64 → open-address probe → copy under the seqlock → verify `seq` unchanged **and** `tailPub <= pos` → `memcmp` the key → rebuild the JS value by its type tag → refill L1 carrying the remaining TTL → return. **~50–500ns**, size- and locality-dependent.
4. Still a miss → `undefined`. (Reserved: fall through to L3, async only — unbuilt.)

**`set(key, value, {ttlMs})`**
1. Reject what cannot be stored: an over-long key, an empty key, an unpaired surrogate, a value past the arena or ring limit, a type the mode does not accept. Reported as `false` with a reason in `lastError` — `set` never throws.
2. Encode once. `bytes` writes the value's own bytes with a type tag; `direct` and `safe` run the codec.
3. Insert into L1, evicting to stay under budget.
4. **Primary:** apply straight to the arena, then publish an invalidation record.
   **Worker:** `memcpy` the encoded record into this worker's submission ring and ring the doorbell if the primary might be idle. If the ring is full the write is *shed* — counted in `stats.writesShed`, never silent — and L1 still holds it, so the effect is a miss for other workers, never a wrong value.
5. Return (synchronous).

The primary drains the rings, applies each record as the sole writer, and then invalidates its own L1 for records other workers wrote — skipping its own, which it already has correct.

Staleness is bounded by the drain, not by a tick: a worker's write is visible to another worker once the primary has applied it and the reader has drained the invalidation ring.

> `clearAll` still travels over cluster IPC rather than the rings. The primary
> drains the rings to empty before applying an IPC batch, which is what keeps
> one worker's operations in order (decision 36).

---

## 7. Sizing

Computed once at startup, then fixed. A fixed mapping is the single biggest simplification available — growing L2 would mean remapping in every live worker mid-read.

| | formula | typical |
|---|---|---|
| L2 | `clamp(totalRAM × 1%, 16MB, 128MB)` | ~40MB on a 4GB host |
| L1 | `clamp(heapLimit × 0.5%, 512KB, 2MB)` | ~1–2MB |

Both overridable via constructor options.

The data region takes whatever the metadata leaves, so a requested size is
roughly what you get: a 24MB arena yields 23.2MB of data, 26MB yields 24.7MB,
28MB yields 26.7MB. It used to be rounded down to a power of two so the log
could mask offsets, which meant all four of those yielded exactly 16MB —
capacity could be doubled but never tuned, and the formulas above named numbers
nobody actually received. `autoSize()` reports what a given machine gets.

The **index** and the **invalidation ring** are still powers of two. Open
addressing probes with a mask on every lookup, which is a far hotter path than
computing a log offset.

The submission rings are sized separately: `submitRings` slots (default 32) of
`submitRingBytes` each (default 1MB), in their own segment. A value larger than
half a ring can never be delivered, so `set` rejects it at the call site rather
than reporting success on a write that will always shed.

---

## 8. Failure modes

| Failure | Behaviour |
|---|---|
| Worker `SIGKILL`ed | Arena untouched — the worker had no write permission. Records already published to its submission ring are still applied; anything half-written is never published, and the slot is reclaimed by the next worker that finds its owner pid dead. |
| Worker killed mid-read | Nothing held; no lock, no cleanup. |
| Primary crashes | **Handled.** Workers see the heartbeat go stale, detach (mandatory — on Windows a held handle blocks a new primary from creating the segment at all), keep serving their warm L1, and poll the name. They re-attach only when a heartbeat *advances* across two polls, then flush L1, reset the invalidation cursor and re-claim a ring slot. `arenaId` distinguishes a new primary from the same one resuming. See §13.11. |
| Primary stalls (long GC, `SIGSTOP`, paused container, laptop sleep) | Same path as a crash, and it recovers when the primary resumes. Ticks are monotonic and suspend-counting, so a wall-clock step cannot trigger this and a sleep does not permanently degrade. |
| Invalidation ring wrap | The lagging worker flushes its whole L1. Detected on the wrap boundary *and* on being lapped mid-read. |
| Submission ring full | The write is shed and counted in `stats.writesShed`. L1 still holds it, so other workers see a miss, never a wrong value. |
| Corrupt submission ring (hostile or buggy worker) | The primary snapshots geometry and validation bounds at create and never re-reads them from shared memory; `head - tail` is clamped to capacity, and every record is bounds-checked. A rejected record stops that one ring — the worker loses its own writes and nothing else. |
| Layout change across versions | `magic` (published last, with release ordering) + `layout` + full geometry validation; a mismatch refuses to attach rather than misreading. |
| Corrupt length field | Bounds-checked before every `memcpy`. |
| Backing filesystem smaller than the arena | Reserved at `create()` via `posix_fallocate`, so it fails there with a message naming `--shm-size` rather than `SIGBUS`ing on first touch. |
| Synchronous write burst with no event-loop yield | On the ring transport the records land immediately; only the doorbell waits for the loop. On the IPC fallback, propagation stalls entirely — the drain callback cannot fire, so writes shed. L1 serves them either way. |
| Worker attaches read-only and tries to write | Every mutating entry point refuses with an exception rather than taking a `SIGBUS` on the read-only mapping. |
| Clock stepped (NTP, manual) | No effect. Heartbeats and TTLs are on a monotonic tick clock, and L1 expiry is on `performance.now()`. |

**Security boundary:** the segment is uid-scoped, mode 0600. Any process running as the same user can read every cached value. This is not safe for untrusted co-tenants, and must be documented plainly. Note also that workers map two segments read-write — hints and their own submission ring — so the isolation claim is specifically about the *arena*, and the primary treats both writable surfaces as untrusted input.

---

## 9. Measurements

Apple Silicon, Node 24.15.0, V8 13.6.233. Harness in `src/`.

> **Payload entropy matters enormously.** An earlier draft of this document reported
> LZ4 figures measured against `'x'.repeat(n)`, which compresses to nothing and is
> ~16x faster to compress than real data. Every number below uses JSON-shaped
> payloads with randomised field values (gzip 29–67%). Do not benchmark this
> system with repetitive filler.

### Call overhead

| Operation | ns/op |
|---|---|
| JS no-op function | 0.9 |
| Node-API no-op call | 8.1 |
| Node-API, read latin1 string arg | 17.7 |
| Node-API, arg + construct 200B string | 30.8 |
| JS `Map.get` (cached string) | 19.2 |

### String return strategy

| bytes | copy out | external per call | `Map` holding external |
|---|---|---|---|
| 200 | 30.0 | 58.0 | 20.6 |
| 1024 | 59.7 | 55.2 | 22.1 |
| 16384 | 438.9 | 58.9 | 21.6 |
| 262144 | 26350.3 | 55.8 | 21.4 |

External-string cost is **flat ~56ns** irrespective of size; copy cost is linear.
Crossover ~1KB. Caching the handle in a `Map` beats both at every size.

### L2 arena, end to end, uncompressed

`probe` = hash + index probe + `memcmp`. `getLen` = full seqlock read + copy.
`get` = plus V8 string construction. All three allocators agree within noise.

| bytes | probe | getLen | get (full) | set | JS `Map.get` |
|---|---|---|---|---|---|
| 64 | 26 | 36 | 42 | 45 | 17 |
| 1024 | 32 | 54 | 80 | 86 | 17 |
| 16384 | 35 | 335 | 830 | 620 | 17 |

**Those are a best case and were wrongly used as the headline.** They cycle 2000
keys in order, so index and entries (~200KB) stay cache-resident. Measured across
access order and keyspace (`bench/l2_latency.js`):

| value | keyspace | order | L2 hit | full `get()` |
|---|---|---|---|---|
| 64B | 2,000 | sequential | 42ns | 51ns |
| 64B | 2,000 | random | 57ns | 65ns |
| 64B | 200,000 | sequential | 91ns | 101ns |
| **64B** | **200,000** | **random** | **265ns** | **298ns** |
| 1KB | 200,000 | sequential | 156ns | 187ns |
| **1KB** | **200,000** | **random** | **445ns** | **497ns** |

The realistic figure is **~300–500ns**, not 42ns — a 7–12x correction. §6's
"150–400ns" was closer to the truth than §9's own headline.

### LZ4 on realistic data — the case against compression

| bytes | ratio | compress | decompress | memcpy |
|---|---|---|---|---|
| 64 | 94% | 210ns | 8ns | 1ns |
| 256 | 75% | 305ns | 24ns | 2ns |
| 1024 | 64% | 806ns | 155ns | 9ns |
| 4096 | 57% | 2984ns | 697ns | 37ns |
| 16384 | 51% | 11919ns | 3034ns | 183ns |

Measured through the real store, LZ4 is **the entire cost** of an operation:

| bytes | get uncompressed | get compressed | set uncompressed | set compressed |
|---|---|---|---|---|
| 256 | 40ns | 75ns | 84ns | 388ns |
| 1024 | 55ns | 205ns | 104ns | 1242ns |
| 4096 | 117ns | 791ns | 249ns | 4671ns |
| 16384 | 316ns | 3403ns | 749ns | 20118ns |

Compression makes reads **2–10x slower** and writes **5–27x slower**, to buy
roughly **2x density** — and it gets worse as values grow, so no size threshold
rescues it. On a 32MB default arena, the alternative to compressing is simply
allocating 64MB, which costs nothing anyone will notice.

### Allocator comparison — hit rate at fixed capacity

32MB arena, 60k keys, Zipf s=1.0, cache-aside, compression off. `SLAB` =
size-class free lists + CLOCK. `LOG` = circular log, FIFO eviction.
`LOG2` = circular log where a live entry with its reference bit set is
re-appended at the head instead of dropped (CLOCK semantics on a log).

| scenario | SLAB | LOG | **LOG2** |
|---|---|---|---|
| stable mixed sizes (64B–8KB), steady state | 71.7% | 72.3% | **75.0%** |
| after a shift to larger values (new keys) | 50.0% | 55.2% | **58.1%** |
| workload oscillating back to the earlier small keys | **87.6%** | 80.4% | 80.4% |

`LOG2` wins the two realistic cases. `SLAB` wins only the third, and for a
narrow reason: its stranded small-size classes are never reclaimed, so old
small entries survive a large-value phase and are still there if the workload
swings back. That is slab calcification being scored as a benefit by a
contrived oscillation — the same stranding costs it 8 points in the row above.

Write cost is identical across all three when the arena is not under pressure;
`LOG2`'s re-append only does work during eviction, and is capped at 64
relocations per allocation so a hot arena still makes progress.

### Does compression make sense at all? — measured at fixed budget

Compression is never a latency win; it is only ever a density win. Density only
matters while the working set does not fit. So the question is whether trading
latency for density ever beats simply buying the density with RAM.

Same workload, varying the data region, `LOG2`, foreground compression on write:

| data region | uncompressed hit / get | compressed (≥1KB) hit / get | compression buys |
|---|---|---|---|
| 16MB | 80.7% / 117ns | 85.5% / 355ns | +4.8 pts |
| 32MB | 87.7% / 122ns | 89.9% / 371ns | +2.2 pts |
| 64MB | 89.9% / 122ns | 89.9% / 352ns | **+0.0 pts** |

Compression costs ~3x on reads and ~9x on writes (176ns → 1729ns) at every
point, and its benefit decays to exactly nothing once the working set fits.

**At every row, spending 2x the memory dominates compressing.** 32MB
uncompressed (87.7%, 122ns) beats 16MB compressed (85.5%, 355ns) on *both*
axes. 64MB uncompressed matches 32MB compressed on hit rate at a third of the
read latency. Since L2 is one shared arena per host — not per worker — the
memory in question is tens of megabytes.

Break-even, if you genuinely cannot spend the RAM: the added cost is
~0.85 × 230ns per hit plus ~0.15 × 1550ns per miss ≈ 420ns per operation, against
a 4.8-point hit-rate gain. So compression pays only where a miss costs more than
**~8.8µs** — which is true of a Valkey hop (~50–200µs) or a database query, and
false of anything computed locally.

Threshold and acceleration, if it is enabled:

| policy | hit% | get(ns) | set(ns) |
|---|---|---|---|
| off | 80.7 | 117 | 182 |
| ≥256B | 85.8 | 372 | 1909 |
| **≥1KB** | **85.5** | **346** | **1729** |
| ≥2KB | 84.8 | 321 | 1521 |
| ≥4KB | 83.4 | 257 | 1027 |
| ≥1KB, LZ4 accel=4 | 84.5 | 341 | 1488 |
| ≥1KB, LZ4 accel=16 | 80.7 | 112 | 573 |

A 1KB floor is the right default: below it, ratios are 75–94% so it is nearly
pure cost. LZ4 acceleration does not rescue the write path — at accel=16 nothing
clears the "12.5% smaller" bar at all, so it degenerates to compression being
off. There is no setting that makes compression cheap.

**Verdict: off by default, off in v1 — and now off at build time too.** Since
nothing uses it, linking LZ4 unconditionally only made the addon unbuildable
anywhere without a system LZ4 at a hardcoded path. It is now an optional build
feature:

```
node-gyp configure build                      # default: no LZ4, no dependency
node-gyp configure build --turbokv_lz4=1   # link system LZ4
```

The default build has **no external dependencies at all**. `find_lz4.js` locates
LZ4 for the opt-in build via `pkg-config`, then the usual prefixes, honouring
`LZ4_PREFIX`. `Cache.hasCompression()` reports the capability, and asking for
`compress: true` on a build without it throws rather than silently storing
uncompressed.

Mixing builds is guarded rather than left to chance: the header records a
`FEATURE_LZ4` bit the first time a compressed entry is written, and a build
without LZ4 **refuses to attach** to such an arena with a message naming the fix,
instead of attaching and reporting silent misses for every compressed key.

### Background compaction — prototyped, and rejected

The idea: since the primary owns L2 and is off the read path, compress cold
entries in the background, keeping writes fast and recovering density only
under pressure. It was built and it works correctly. It is still not worth it.

Design as built: capture candidates walking forward from the tail, compress on
the **libuv threadpool**, apply back on the writer thread. Because a circular
log cannot reclaim space in the middle, "compress" means re-append the smaller
record at the head and repoint the index slot — i.e. LOG2's second chance, but
compressed.

**Race safety (works).** Only the LZ4 call is off-thread; capture and apply both
run on the single writer thread, so the sole exposure is the window between
them. It is closed by re-checking `(slot, offset, hash, version)` before
publishing, and again after the allocation (which can evict the source).
The `version` field is the global `++inserts` counter, so it is never reused —
strictly stronger than a per-key counter, which would be ABA-vulnerable across
an eviction and reinsertion of the same key. Compaction deliberately does not
bump the version or touch the invalidation ring, since the value is unchanged.

| test | result |
|---|---|
| every key overwritten during a widened 40ms window | **18,000 stale captures discarded, 0 wrong values, 0 resurrections** |
| realistic 5% overwrite rate during the window | 4,103 applied / 212 discarded — **95% make progress**, 0 wrong values |

**Value (does not justify it).** 16MB data region, 40k keys, Zipf s=1.0:

| configuration | hit rate | entries resident | read latency | complexity |
|---|---|---|---|---|
| plain | 80.8% | 10,321 | 129ns | — |
| + compaction, all entries near tail | 82.7% | 12,118 | **390ns (3.0x)** | async subsystem + race window |
| + compaction, cold entries only | 80.8% | 10,175 | 285ns (2.2x) | same |
| **plain, data region 16MB → 32MB** | **87.7%** | 20,459 | **121ns** | none |

Restricting to cold entries cuts the latency penalty but erases the entire
hit-rate gain — which is not a policy bug but the shape of the trade: the gain
comes precisely from compressing data that gets read. Meanwhile utilisation
falls to ~78%, because re-appending leaves garbage behind that the tail has not
yet reclaimed, eating much of the density won.

And simply doubling the arena — 16MB to 32MB, an amount nobody will notice —
delivers **+6.9 points, 3.6x the gain of compaction, at no latency cost and no
complexity.** The hit-rate curve then saturates near 64MB for this working set.

Conclusion: do not build it. The entry format keeps the `COMPRESSED` flag so a
genuinely memory-constrained deployment can opt in later.

The machinery itself has since been **removed** rather than left compiled in
(decision 47). Foreground compression is a separate, live feature and is
untouched: `compress` / `compressMinBytes` / `compressAccel`, the `COMPRESSED`
flag, and the opt-in LZ4 build all remain. The measurements above were taken
with `bench/compact_bench.js` and `bench/race.js`, which went with it; both are
recoverable from git at `ec93071`.

### Two gaps found by measurement — both now fixed

**Gap 1: workers could not set CLOCK reference bits.** Reads happen in workers
holding a `PROT_READ` mapping, so they could not write a reference bit; every
earlier `LOG2` benchmark ran in-process where the reader *was* the writer.
Suppressing bit-setting collapsed `LOG2` (80.8%) to `LOG` (78.7%) — the entire
advantage. (`LOG` was removed in decision 49; `bench/gaps.js` now takes its
baseline from a second-chance budget of 0, which is exactly what the mode
difference was. Re-measured on the current build: 83.4% / 82.3% / 80.9%, so
second chance is worth 2.4 points against the 2.1 above. All three rose ~2.5
points because decision 44 enlarged the usable data region.)

*Fixed* with a **separate hints segment**: one byte per index slot, in its own
`shm` object opened `O_RDWR` by workers, while the arena's own descriptor stays
`O_RDONLY`. Isolation is therefore a property of the file descriptor, not merely
of the mapping — a worker cannot map the arena writable even deliberately, but
can still record what it read. The read path loads before storing, so a hot
entry already marked skips the store and workers do not ping-pong the cache line.

Verified end to end: a worker attached read-only, read exactly 100 of 5000 keys,
and the primary observed **exactly 100 reference bits set**. Arena writes from a
worker still fault with SIGBUS.

**Gap 2: tombstones accumulated without bound.** `HASH_TOMB` was never reclaimed
and `findSlot` stops only at `HASH_EMPTY`, so probes degraded to full-table
scans — 45ns → 342ns at **3% load factor**, and up to ~33µs with a larger live set.

*Fixed* with **backward-shift deletion** (Knuth 6.4 Algorithm R), which closes
the gap by relocating entries instead of leaving a marker, plus a **75% index
load ceiling**. The claim that this meant the index "can never saturate" was
**false**; see the review findings below.

| cumulative inserts | live | before | after |
|---|---|---|---|
| 65,536 | 2,001 | 45ns | 31ns |
| 400,000 | 2,001 | 342ns | 29ns |
| 1,000,000 | 49,152 | ~33,000ns | 41ns |

Probe cost is now flat. Live entries cap cleanly at 49,152 (75% of 65,536)
instead of drifting to 59,074 while inserts silently failed.

### A silent data-corruption bug, and a correction

Stress-testing the fixes surfaced a **pre-existing** bug that invalidates an
earlier claim in this document. The reported "22,257,202 reads, 0 torn values"
was **under-stressed**: the arena was large enough that the log rarely wrapped
over an offset a reader was holding. Tuned to force wrap-around (2MB arena, 800
keys, 6 readers) the original code produces **3–7 corrupt reads per ~24M**, and
it is present with or without the index changes.

Two distinct defects:

**a) Second-chance re-append wrote without checking for room.** `logDropTail`
relocated a surviving entry to the head, but that branch only runs while the log
is under allocation pressure — precisely when free space is scarce. It could
overwrite live records near the tail whose index slots still pointed at them.
Fixed by requiring `freeBytes >= bsz` before writing.

**b) The seqlock cannot detect log reuse.** A seqlock protects an *in-place
rewrite* of an entry. When an entry is evicted and the head wraps over its bytes,
that address is no longer an `Entry` header at all — `e->seq` becomes somebody
else's payload and can read as stable and even twice in a row. Because the same
keys are rewritten repeatedly, those bytes frequently hold an **older copy of the
same key**, so `memcmp` passes too, and a stale or torn value is returned.

*Fixed* by making index slots store the **monotonic log position** rather than a
physical offset (physical address is `pos % dataBytes`). The position never
wraps, so it can express liveness that an offset cannot: the bytes at `pos`
belong to record `pos` exactly while `logTail <= pos`. The reader loads the
published tail *after* copying; since the tail only advances, observing
`tailPub <= pos` proves the record was live for the entire copy.

This is the load-bearing correctness argument of the whole read path and should
be the first thing any reviewer checks.

After both fixes, the same wrap-heavy stress across five configurations —
~90M reads against ~28M concurrent writes — reports **0 corrupt reads**, and
read latency is unchanged (43/52/80/205/741ns at 64B–16KB).

### Arena sizing validation

`create()` did not check that header + index + ring actually fit. A 1MB segment
with 65,536 index slots (1MB of index alone) underflowed `totalBytes - dataOff`
into a huge unsigned value and hung. Now rejected, along with a non-power-of-two
`indexSlots`.

### Comparison against the Bugsee appserver cache

Measured against `Bugsee/appserver/code/components/shared/cache`, a production
implementation of the same shape: L1 `JsonLru` per worker, L2 in the primary's
heap reached over `cluster` IPC, plus TTL, alias refs and an L3 adapter.
Both sized identically (L1 2MB/worker, L2 256MB), same Zipf s=1.0 workload,
90% read / 10% write, cache-aside, shared keyspace. Harness in `bench/`.

**Single process** — neither cache has an IPC peer, so this is each engine's
in-process fast path. Values are objects, so *both* pay a JSON encode/decode at
the application boundary (bugsee internally, turbokv in the caller).

| scenario | turbokv | bugsee | hit rate (tc / bs) |
|---|---|---|---|
| 60k keys, exceeds L1 | 304k ops/s | **321k ops/s** | 87.4% / 58.4% |
| 1k hot keys, fits L1 | **440k ops/s** | 392k ops/s | 99.7% / 99.7% |
| 60k keys, **opaque string values** | **1,722k ops/s** | 530k ops/s | 87.4% / 56.8% |

With object values the JSON codec dominates and the two are within ~10% — bugsee
is actually *faster* on the large working set, because in a single process its
misses cost nothing (no L2 to consult) while turbokv pays a real L2 lookup.
The 3.2x gap appears only with opaque payloads, where turbokv stores bytes
verbatim and bugsee's JSON-only API must still encode.

**Cluster, 1 primary + 4 workers** — this is where the architectures separate.

| workers | turbokv (`bytes`) | hit rate | bugsee | hit rate |
|---|---|---|---|---|
| 1 | 213k ops/s | 80.4% | 57k ops/s | 80.4% |
| 2 | 374k ops/s | 85.7% | 89k ops/s | 68.4% |
| 4 | 666k ops/s | 90.5% | 80k ops/s | 30.2% |
| 8 | **1,040k ops/s** | **94.1%** | **42k ops/s** | **12.8%** |

turbokv scales close to linearly because reads never touch the primary.
bugsee plateaus near 80k regardless of worker count: every L1 miss is an IPC
round-trip through one event loop, and past saturation its client either times
out (100ms) or hits the 256-request pending cap, resolving `undefined` — which
the application sees as a miss. Its L2 is not the problem; both L2s held exactly
49,146 entries (62MB of 256MB), so the data was there and simply could not be
reached in time. This is the bottleneck predicted in decision 1, measured.

At 4 workers with object values: p50 4.0µs vs 56µs, p99 19µs vs 115µs,
p99.9 173µs vs 4.7ms. With opaque string values, 1,540k vs 72k ops/s.

**Caveats that matter for reading these numbers:**

1. **The write paths are not equivalent.** turbokv's `set` is fire-and-forget
   — buffered and batched to the primary, unacknowledged, visible to other
   workers about a tick later. bugsee's `set` awaits an ack. turbokv is
   trading write visibility for speed, and part of its margin is that trade
   rather than pure efficiency.
2. **bugsee is complete; turbokv is a prototype.** TTL, alias refs, an L3
   adapter, key validation, backpressure and worker-death handling all cost work
   per operation that turbokv simply does not do yet.
3. **Single-process misses are unrealistically cheap here.** A miss just refills
   from a pre-built value. In a real service a miss costs a database query, so
   the hit-rate column would dominate throughput far more than it does above.

### Resolving the decision 4 / decision 5 tension

Decision 4 limits values to bytes and strings; decision 5 says L1 caches the
*decoded* value so hits skip decoding. With object workloads these conflicted:
the cache only ever saw an encoded string, so L1 cached a string and the
application re-parsed it on **every hit** — exactly the cost decision 5 exists
to remove.

**Resolution: the two decisions apply at different boundaries, bridged by an
optional caller-supplied codec.**

```js
new Cache({ codec: { encode, decode } })     // e.g. JSON, msgpack, protobuf
```

- Decision 4 holds at the **L2 and wire boundary**: L2 stores only bytes, IPC
  carries only bytes, and the arena format is unchanged.
- Decision 5 holds at **L1**: with a codec, `set` encodes once (L2 needs bytes
  regardless, so this is not extra work) while L1 keeps the **decoded** value,
  and an L1 hit returns it with no decoding at all.
- With **no** codec the value is already opaque bytes, L1 stores it as-is, and
  decision 5 is satisfied trivially. This stays the fastest configuration.

| scenario | app-side codec | **cache-owned codec** | bugsee |
|---|---|---|---|
| 1k hot keys, fits L1 | 426k ops/s | **1,529k ops/s** | 382k ops/s |
| 60k keys, exceeds L1 | 291k ops/s | **366k ops/s** | 285k ops/s |
| cluster, 4 workers | 661k ops/s | **778k ops/s** | 79k ops/s |

On an L1 hit, p50 goes from 1334ns to **42ns**. The gain tracks L1 hit rate, so
it is largest exactly where a cache is meant to spend its time.

**Three consequences, all real:**

1. **Aliasing is now a live hazard.** L1 returns the same object reference on
   every hit. Decision 7 got away with sharing because strings are immutable;
   objects are not, so one caller mutating a returned object corrupts the cache
   for every other reader in that worker. Default is a documented do-not-mutate
   contract, matching decision 7's treatment of strings. `freeze: true`
   deep-freezes at insert instead, turning mutation into a thrown error —
   measured cost 25–30% (1,529k → 1,125k ops/s).
2. **The L1 byte budget becomes an estimate, not a cap.** JS cannot measure an
   object's heap footprint, so the budget counts encoded bytes scaled by
   `heapFactor`. See "Can object size be measured?" below for why no better
   estimate exists, and what to do instead. Measured against real heap usage for
   JSON-shaped objects:

   | encoded size | heap per object | ratio |
   |---|---|---|
   | 200B | 641B | 3.21x |
   | 741B | 2142B | 2.89x |
   | 2846B | 7942B | 2.79x |

   A default of 3 is therefore calibrated rather than guessed, but it is
   shape-dependent — objects dominated by long strings or typed arrays will sit
   well below it. The "hard byte cap" claim in section 4 is weakened to an
   estimate whenever a codec is in use.
3. **Effective L1 capacity drops by `heapFactor`**, so on a working set larger
   than L1 more reads fall through to L2 (161k L1 hits becomes 132k in the 60k-key
   scenario). Net throughput still improves, but by 26% rather than 3.6x.

### Can object size be measured? No - and a better estimate is not the answer

**Where L1 crosses into native.** An L1 *hit* in the primary crosses nothing —
it is a plain `Map` lookup, which is why it measures 42ns. A worker's L1 hit
crosses once, for the `ringHead()` drain check. Every L1 *insert* already calls
`hashKey()`, so a size measurement there would cost no extra boundary crossing.

**V8 exposes no per-object size to embedders.** `GetShallowSize()` exists only
on `HeapGraphNode` — inside a heap snapshot, which is stop-the-world. Node-API
has nothing. Confirmed against the Node 24.15 headers.

**A native structural estimate was built and is worse on both axes.** It walks
the value through Node-API and models V8 layout (Smi 0, HeapNumber 16,
SeqOneByteString 16+len, JSObject 16+8n, JSArray 16+16+8n), counting internalised
property names and shared hidden classes as zero. Against ground truth (measured
`heapUsed` delta per object, after forced GC):

| estimator | 645B object | 2144B object | 7946B object | cost per call |
|---|---|---|---|---|
| **`encodedBytes x 3`** | **-7%** | **+4%** | **+7%** | free — length already known |
| native structural walk | -26% | -17% | -14% | **5825ns** |
| `v8.serialize().length` | -73% | -71% | -70% | 3506ns |

The walk consistently undercounts because backing stores, allocation alignment
and per-object bookkeeping cost more than the model, and shared property names
cannot be attributed to any one instance. It is also ~5x more expensive than the
`JSON.stringify` it would accompany (1191ns). Tuning its constants would only
curve-fit toward the accuracy `encodedBytes x 3` already delivers for free.

**The productive answer is to stop needing per-object accuracy.** The point of
the byte budget is to bound memory; bound the memory directly instead. A guard
reads `v8.getHeapStatistics()` after a GC and sheds L1 when the live set exceeds
a configured fraction of the heap limit:

| configuration | retained live heap | L1 entries kept |
|---|---|---|
| guard off, 1GB byte budget | 445MB (50% of limit) | 200,000 |
| guard at 40% | 297MB (33%) | 128,998 |
| guard at 20% | **121MB (14%)** | 46,079 |

The deliberately-wrong 1GB budget bound nothing; the guard bounds what actually
matters. Two things it is not:

- **It needs the event loop to turn.** GC notifications arrive on a later tick,
  so a fully synchronous loop never receives them. Fine for a server — and a
  cache that never yields cannot flush its IPC write batch either.
- **The signal must be read after a GC.** Sampling `used_heap_size` at an
  arbitrary moment includes uncollected garbage, so shedding *raises* the
  reading and the guard thrashes. A first attempt did exactly that, ending with
  higher peak heap than no guard at all.

So: `heapFactor` stays the sizing mechanism, and the guard is the backstop that
makes its inaccuracy non-fatal rather than something to engineer away.

### Primitives-only mode

Restricting accepted values to `string | number | boolean | null` fixes, in one
move, the three things the codec mode could not. All three verified:

1. **Byte accounting becomes exact.** A flat V8 string costs `16 + len`
   (one-byte) or `16 + 2*len` (two-byte), 8-aligned. Measured against real heap
   usage:

   | kind | len | measured | predicted | error |
   |---|---|---|---|---|
   | one-byte | 32 | 51B | 48B | -6% |
   | one-byte | 1024 | 1041B | 1040B | -0% |
   | one-byte | 8192 | 8209B | 8208B | -0% |
   | two-byte | 1024 | 2064B | 2064B | -0% |

   Smis and `true`/`false`/`null` genuinely cost nothing; a non-Smi number is a
   16-byte HeapNumber. So `heapFactor` disappears and the budget is a real cap
   rather than an estimate.
2. **The aliasing hazard disappears.** Primitives are immutable, so there is
   nothing to mutate, no do-not-mutate contract, and no need for `freeze`.
3. **No codec to configure.**

**One hole, found and closed.** A V8 `SlicedString` keeps its parent alive, so a
cached substring can retain an arbitrarily larger document. Measured with an 8MB
parent, keeping one derived 1MB string and dropping the parent:

| kept value | retained |
|---|---|
| the whole 8MB parent (control) | 8.00MB |
| **1MB substring, as-is** | **8.00MB — the entire parent** |
| 1MB substring via `native.flatten()` | **1.00MB — exactly its own bytes** |

Primitives mode therefore flattens strings on insert, in native. This costs
nothing extra in boundary crossings because the insert path already calls
`hashKey()`, and the flatten itself measures ~42ns for a 200-char value —
cheaper than the hash call beside it.

**The cost of the mode** is ~20% throughput for flatten plus exact sizing
(1,542k to 1,227k ops/s on opaque payloads, still 2.4x the bugsee cache), and
that object workloads must encode in the caller and therefore decode on every
L1 hit — 291k versus 1,583k measured.

### Three coherent value modes

| mode | L1 holds | accounting | aliasing | L1 hit cost |
|---|---|---|---|---|
| **bytes** | the value's bytes | **exact** | **none** | free |
| JSON-always (the bugsee design) | JSON string | exact | none | parse per hit |
| codec, decoded L1 | decoded object | `heapFactor` estimate | **yes** | free |

Worth stating plainly: the bugsee cache's fixed-JSON design gets exact
accounting and freedom from aliasing for the same structural reason primitives
mode does — its L1 holds a string. That simplicity is real, and its price is one
parse per hit, which is what the cluster comparison measured.

**Recommendation:** primitives is the default. The codec mode stays available
for object-heavy workloads that are dominated by L1 hits, carrying its two
documented caveats. This keeps the safe, exactly-accountable configuration as
the one users get without reading anything.

### JSON fast paths on Node 26

Measured on Node 24.15.0 and Node 26.8.1, same machine, 1.4KB payload.

**The premise holds, and then some.** `JSON.stringify` of a pure-ASCII object
went from 2324ns to **1524ns, 34% faster**. `JSON.parse` improved about 11%.

**But the slow paths did not improve, so falling off one now costs more:**

| variant | Node 24 | Node 26 | penalty on 24 | penalty on 26 |
|---|---|---|---|---|
| plain object (fast path) | 2324ns | **1524ns** | — | — |
| 2-space indent | 3118ns | 3260ns | 1.29x | **2.11x** |
| replacer function | 5605ns | 5409ns | 2.32x | **3.51x** |
| all values non-ASCII | 2560ns | 3096ns | 1.10x | **2.03x** |

Two things follow. A `replacer` now costs 3.5x rather than 2.3x. And a heavily
non-ASCII payload is not merely slower than ASCII on Node 26 — it is **slower
than the same payload was on Node 24** (3096ns vs 2560ns). The new fast path is
ASCII-oriented, and it is evaluated per string: injecting a *single* non-ASCII
character into a 41-string document cost nothing measurable (0.99x); only when
most strings are non-ASCII does the 2x appear.

**`JSON.parse` is insensitive to string representation.** Flat one-byte, sliced,
cons, and strings returned from our arena all parse within 2% of each other on
both versions. So flattening is worth doing for memory (a slice retains its
parent) but buys nothing for parse speed.

**A replacer is never acceptable, and this is now enforced two ways.** The rule
is absolute: `JSON.stringify(v)` and `JSON.parse(s)` take exactly one argument.

1. **Repo-wide lint.** `json_fastpath_test.js` scans every `.js` file in the
   project for `JSON.stringify(`/`JSON.parse(` calls with more than one
   top-level argument, using balanced-paren scanning rather than a regex so
   nested calls are not miscounted, and stripping comments first. A file that
   measures the slow paths on purpose opts out with a
   `json-fastpath-lint: allow` marker, so the exemption is visible in the file
   rather than hidden in the linter.
2. **Construction-time codec check.** The codec is supplied by the caller, where
   no source lint can reach, so `assertFastCodec` inspects
   `Function.prototype.toString` of both `encode` and `decode` and rejects any
   `JSON.stringify`/`JSON.parse` call with a second argument. Native or bound
   functions report `[native code]` and are accepted; codecs that are not JSON
   at all are not affected. `allowSlowCodec: true` overrides it.

Output probing alone is not sufficient, which is why the source check exists: an
**identity replacer** — `JSON.stringify(v, (k, x) => x)` — produces byte-identical
output while still costing 3.51x, so nothing about the result reveals it.

| codec | verdict |
|---|---|
| `JSON.stringify` / `JSON.parse` directly | accepted |
| `v => JSON.stringify(v)` | accepted |
| `v => JSON.stringify(Object.assign({}, v))` (nested call) | accepted |
| `v => JSON.stringify(v, null, 2)` | **rejected** |
| `v => JSON.stringify(v, (k, x) => x)` | **rejected** |
| `v => JSON.stringify(v, ['a'])` | **rejected** |
| `s => JSON.parse(s, (k, x) => x)` | **rejected** |
| a non-JSON codec (msgpack, protobuf) | accepted, unaffected |

**A real bug this surfaced.** The prototype encoded every value through
`napi_get_value_string_latin1`, silently mangling any non-ASCII string — a
limitation noted earlier and now fixed. Values are classified at insert:
ASCII is stored one byte per character and rebuilt with
`napi_create_string_latin1`; anything else is stored as UTF-8 and rebuilt with
`napi_create_string_utf8`. This both fixes correctness and keeps ASCII values on
the one-byte representation that the Node 26 stringify fast path favours if the
application re-encodes them. The `FLAG_LATIN1` bit the entry format already
reserved is what carries the classification.

**End to end**, with JSON on the hot path (codec mode, 60k-key working set):
444k ops/s on Node 24, **513k ops/s on Node 26** — a 16% gain for free.

### Node-API ABI stability, verified

The addon compiled against Node 24.15 headers loads and runs unmodified on Node
26.8.1. This is decision 12 paying off directly: had the design used V8 fast
calls it would have needed a rebuild and a new prebuild for that major, for a
saving measured at ~6ns per call.

### Mutation safety: what the bugsee cache was buying with JSON

The bugsee cache stringifies on write and parses on read at every layer. That is
not incidental overhead — it buys two properties deliberately: **every read
returns a fresh object, so no layer can be corrupted by a caller**, and **any
JSON-serialisable value is accepted**. Any design that caches decoded objects to
avoid the parse has to pay for those properties some other way.

**The hazard, as originally built.** `set(key, obj)` put the *caller's own*
object into L1. So a caller could corrupt the cache without ever calling `get`:

```
set('acct', user)          // user = { role: 'viewer' }
user.role = 'admin'        // caller mutates a variable it still holds
get('acct').role           -> 'admin'    L1 followed the mutation
L2 (other workers)         -> 'viewer'   diverged; version never changed
...L1 evicts...
get('acct').role           -> 'viewer'   silently REVERTED
```

The revert is the worst part: the bug appears and then disappears on its own,
at a time determined by eviction pressure.

**Two fixes, both now on by default in codec mode.**

- `isolate` — `set` decodes its own encoding to produce the L1 object, so the
  cache holds something the caller has never seen. The encoding was needed for
  L2 anyway; the extra cost is one `decode` per `set`.
- `freeze` — the cached object is deep-frozen, so mutating a `get` result throws
  instead of silently corrupting L1.

| configuration | caller mutates its own object | caller mutates `get()` result |
|---|---|---|
| codec, `isolate:false` | **corrupts, then reverts** | **corrupts** |
| codec, `isolate:true` | safe | **corrupts** |
| **codec, isolate + freeze (default)** | safe | throws `TypeError` |
| **primitives (default mode)** | safe | safe |

**What each guarantee costs** (300k ops, 90/10 read/write):

| configuration | L1-resident | exceeds L1 | guarantee |
|---|---|---|---|
| codec, `isolate:false` | 1,545k | 411k | none |
| codec, `isolate:true` | 1,068k | 320k | set-side only |
| **codec, isolate + freeze** | **813k** | 235k | full |
| primitives / parse-per-get | 413k | 291k | full |

The useful result: **freezing buys the same immutability guarantee as parsing on
every read, at roughly twice the throughput** — 813k versus 413k on L1-resident
data. The parse is paid once per insert instead of once per read.

They differ in ergonomics, not safety. Parse-per-get hands back a **fresh mutable
object every time**, which is the friendlier contract — the caller may do
whatever it likes with it. Freezing hands back a **shared immutable object**, so
a caller that needs to modify must clone it first. That is the real trade, and
it is why primitives — where the application owns the codec and therefore gets a
fresh object per parse, exactly as the bugsee cache does — remains the default
mode. Codec mode is the opt-in for workloads dominated by L1 hits.

| Claim | Result |
|---|---|
| Workers read a live arena through `PROT_READ` while the primary writes | **22,257,202 cross-process reads against 11,660,000 concurrent writes, 0 torn or wrong values** (4 forked readers, 5s, blocks constantly reused at changing sizes) |
| A worker cannot corrupt the arena | Write through a worker mapping faults with **SIGBUS**; primary reads correctly afterwards. Enforced by the MMU, not by convention. |

Seqlock correctness needs TSAN before this is trusted in production — 22M clean
reads is strong evidence, not proof.

### Type conversion between L1 and L2

Two conversions on the path, answering different questions.

```
  app value ──codec.encode──► string ──native set──► arena bytes + type flags
   (L1 holds THIS form)                               (L2 holds this)
```

The **codec layer (JS)** converts application value to string, and exists only in
codec modes. L1 holds the value on the *application* side of that arrow, which is
what makes an L1 hit free of decoding. The **native layer (C++)** converts a JS
value to arena bytes, and this is where type must be preserved — L2 is read by
other processes that share no JS state.

| JS value | stored in the arena | flags | rebuilt as |
|---|---|---|---|
| ASCII string | one byte per char | `STRING\|LATIN1` | `create_string_latin1` |
| non-ASCII string | UTF-8 | `STRING` | `create_string_utf8` |
| number | the 8 raw bytes of the double | `NUMBER` | `create_double` |
| boolean | one byte | `BOOL` | `get_boolean` |
| null | zero bytes | `NULL` | `get_null` |
| BigInt | sign byte + 64-bit words | `BIGINT` | `create_bigint_words` |

Doubles are stored raw rather than as text: exact, no parsing, verified to
round-trip `-0`, `NaN`, `±Infinity`, subnormals and `MAX_VALUE`. A stored `null`
stays distinguishable from a miss.

Two bugs this analysis found, before the type tags existed: **numbers and
booleans never reached L2 at all** (they lived in L1 only, vanished on eviction,
were invisible to other workers — while `get` looked correct until then), and
**`null` threw** on both paths from taking `.length` of it.

### Type support across the three modes

Measured end to end — set, forced L1 eviction, read back through the arena.

| input | `bytes` | codec: JSON | codec: `v8.serialize` |
|---|---|---|---|
| string / number / boolean / null | exact | exact | exact |
| `BigInt` | exact | rejected | exact |
| `Date` | rejected | **`string`** | `Date` |
| `Array` / `Object` | rejected | exact | exact |
| `Map` / `Set` | rejected | **`{}`** | preserved |
| `Uint8Array` | rejected | **plain object** | `Uint8Array` |
| `RegExp` | rejected | **`{}`** | `RegExp` |

Primitives rejects loudly; JSON converts silently. A cache that quietly changes
your types is worse than one that refuses them.

### structuredClone for L1: measured, rejected for isolation

Cloning the cached object per read is the obvious way to hand back something
mutable. It is the slowest option, and Node 26 widens the gap because it sped up
JSON and not structured cloning:

| payload | freeze (shared) | `structuredClone` | `JSON.parse(str)` |
|---|---|---|---|
| 201B | 8ns | 1675ns | 604ns |
| 1411B | 4ns | 9533ns | 4133ns |
| 7311B | 4ns | 44026ns | 18046ns |

2.0–2.4x slower than parsing the equivalent string, and parsing needs that string
kept in L1 beside the object. Freezing is three orders of magnitude cheaper than
either. V8 structured serialization still earns a place — not as a per-read
clone, but as a **codec**, where it encodes to bytes for L2 exactly as JSON does.

### Why the codec-free mode is called `bytes`, not `primitives`

It was called `primitives` and then accepted `Buffer` and `TypedArray`, which are
not primitives. That was incoherent, and the cause was ordering: decision 4 fixed
the accepted value types before the three storage modes existed, so when binary
support finally landed it was attached to the only codec-free mode without
revisiting the name.

Removing binary from it would have been worse. Bytes are the most directly
storable thing there is, and the alternative is routing them through a
serializer:

| 4KB `Buffer` | set | get |
|---|---|---|
| codec-free path | **861ns** | **906ns** |
| via `direct` (v8 codec) | 4598ns | 3531ns |

5.3x on writes and 3.9x on reads to serialise something that is already bytes.

So the behaviour was right and the name was wrong. The mode means **no codec:
the native layer encodes the value itself** — scalars become their byte
representation, binary is stored verbatim, and anything that would need a codec
is rejected loudly. `'primitives'` is still accepted as a legacy alias.

### Storage modes

`storage: 'bytes' | 'direct' | 'safe'`.

| | `bytes` (default) | `direct` | `safe` |
|---|---|---|---|
| accepts | scalars only, rejects rest | any structured-cloneable value | any JSON value |
| encoding | none — the app owns it | `v8.serialize` | `JSON.stringify` |
| L1 holds | the primitive | the **decoded, frozen** value | the **encoded string** |
| per read | nothing | nothing | one `JSON.parse` |
| result | immutable by nature | shared and frozen | **fresh and mutable** |
| byte accounting | **exact** | estimated (`heapFactor`) | encoded length |

`safe` means *mutation*-safe: every read is a fresh object, so a caller can do
anything to it. It is not type-safe — it is precisely the mode that turns a
`Date` into a string. `direct` is the type-faithful one. Different safeties, both
real, and the docs must say which.

**A hole in `direct` that JS cannot close.** `Object.freeze` throws on an
ArrayBuffer view with elements, and `ArrayBuffer.prototype.transferToImmutable`
exists in neither Node 24 nor 26 (checked). So the object graph is frozen but
typed-array **contents** stay writable: a caller writing into one corrupts L1 for
its own process until eviction returns the arena's copy. Values holding typed
arrays want `safe` mode, or a defensive copy.

### Mode scorecard

From `bench/modes_report.js`, which is repeatable.

**Validity** — 18 types incl. `-0`, `NaN`, cycles:

| | exact | silently converted | rejected | lost |
|---|---|---|---|---|
| `bytes` | 11/18 | **0** | 7 | 0 |
| `direct` | **18/18** | **0** | 0 | 0 |
| `safe` | 9/18 | **8** | 1 | 0 |

**Safety** — can a caller corrupt the cache?

| vector | `bytes` | `direct` | `safe` |
|---|---|---|---|
| mutate the object passed to `set` | n/a | safe | safe |
| mutate the `get()` result | n/a | throws | safe |
| mutate a nested object in the result | n/a | throws | safe |
| write into a typed array in the result | n/a | **CORRUPTED** | n/a |
| survives L1 eviction unchanged | safe | safe | safe |

**Consistency** — 6,000 randomised ops over 300 keys, identical stream per mode:
zero mismatches between what L1 serves and what the arena serves after eviction,
in every mode; a read-only worker reads 200/200 correctly in every mode.

**Performance** — 200k ops, throughput / p50:

| workload | `bytes` | `direct` | `safe` |
|---|---|---|---|
| reads dominate, fits L1 | 437k / 99% | **1,173k / 99%** | 438k / 99% |
| mixed 90/10, exceeds L1 | 314k / 84% | 177k / 84% | 329k / 84% |
| write-heavy 50/50 | 303k / 85% | 126k / 85% | 328k / 85% |

Cells are throughput / hit rate. The swing across the diagonal is nearly an
order of magnitude: `direct` is 2.7x the field when reads dominate and L1 holds
the working set, and last everywhere else — `v8.serialize` on every write is the
whole cost. Choose by read/write ratio and type needs, not by a global default.
Note this ranking **inverts in a cluster**, where `safe` leads; see the full
matrix above.

### ThreadSanitizer, run

TSAN **cannot observe races between processes** sharing an mmap — it tracks
happens-before within one process, and the real deployment is a writing primary
and reading workers. So the harness models them as threads over the same arena
code: identical atomics, fences and seqlock, with only the isolation boundary
changed. `src/tsan/run_tsan.sh` builds and runs it.

**It found a real bug.** The CLOCK reference bits were a plain `uint8_t` array,
written by the primary (clearing and relocating them) while every worker
read-modify-writes them. That is an unsynchronised concurrent access — a genuine
data race, not a benign one. They are now `std::atomic<uint8_t>` with relaxed
ordering: free at runtime on ARM, and well-defined. TSAN then reported zero
races in steady state.

**And it confirmed the one deliberate race.** Driven into constant log
wrap-around, TSAN reports 20–36 races per run, all in the writer's payload
`memcpy` (`storeSet` and `logDropTail`'s second-chance re-append) against a
reader's payload copy. That is the defining trick of a seqlock: the copy races,
and the sequence re-check detects the torn read afterwards. Across every
configuration the harness observed **CORRUPT=0** — millions of reads, no torn or
wrong value ever escaped — so the detection works. But it is undefined behaviour
by the letter of the C++ memory model, and making it defined would require
atomic per-word payload access instead of a vectorised `memcpy`, which is
exactly the cost the design exists to avoid.

Rather than suppress it — which would hide future bugs in those same functions —
the gate asserts that the **set of racing sites never grows**:

| scenario | corrupt | races | sites |
|---|---|---|---|
| steady state | 0 | 0–1 | writer payload copy |
| constant wrap-around | 0 | 36 | `logDropTail`, writer payload copy |
| wrap + index pressure | 0 | 36 | `logDropTail`, writer payload copy |
| high index load factor | 0 | 21 | writer payload copy |

Any site outside that allowlist, or any non-zero corrupt count, fails the run.
Residual risk, stated plainly: a sufficiently aggressive compiler could in
principle exploit the UB in the payload copy. The fences around it and the
`-O1` build make that unlikely, and the pattern is used this way in production
systems everywhere, but it is not a proof.

### Tuning the two arbitrary constants — and a bug they exposed

**The second-chance budget was capping a mechanism that had already stopped
working.** Instrumenting how often re-append actually fires, with live hot
entries at the tail:

| budget | tail meets live entry | re-appended | **skipped, no room** |
|---|---|---|---|
| 64 | 36,713 | 512 (1.4%) | **35,681 (97%)** |

The `freeBytes >= bsz` guard added earlier to fix a corruption bug was blocking
97% of second chances — because `logDropTail` is *called from* the eviction loop,
so free space is short by definition. Second chance was effectively dead in a
full arena, which is exactly when eviction matters.

*Fixed with a zero-copy path.* When the log is full the head lands on the tail's
own bytes (`hp == phys`). The record does not need to move at all: positions are
monotonic, so re-publishing it at the new position and advancing both pointers
grants another lap for free — no `memcpy`, no free space required. Re-appends
went from 512 to 30,283 at budget 8, and **`LOG2`'s advantage over `LOG` grew
from ~2.5 to 4.4 points** (76.3% vs 71.9%), so decision 16 now rests on a
mechanism that actually runs.

*Budget value.* Hit rate saturates at 8 and is flat to 8192:

| budget | 0 | 1 | 8 | 32 | 64 | 1024 | 8192 |
|---|---|---|---|---|---|---|---|
| hit rate | 85.6% | 85.9% | **86.3%** | 86.3% | 86.3% | 86.3% | 86.3% |

Default is now **16** — saturated with margin, at no measured cost.

**Ring capacity is a time budget, not a count.** A worker that fails to drain
before the head laps it must flush its entire L1. The ring is appended *only by
the primary*, so its rate is the primary's apply throughput — measured at
**647k records/s**:

| ring records | bytes | headroom |
|---|---|---|
| 8192 (old default) | 128KB | **12.7ms** |
| 65536 | 1MB | 101ms |
| 262144 | 4MB | 405ms |

12.7ms is roughly one *minor* GC. A major GC (10–100ms) would flush every
worker's L1. Capacity is now derived from the arena — 64KB records or 4% of the
arena, whichever is smaller — giving ~100ms on the default 128MB arena for 0.8%
of it, and degrading gracefully on small arenas (4MB arena keeps 3.1% and 13ms).

### Namespace quotas, and why they are gone (historical)

Namespaces once carried a soft byte quota enforced through the eviction path,
because with `namespace` as nothing but a key prefix a hot namespace evicts a
cold one entirely:

| | cold survivors | cold bytes | hot bytes |
|---|---|---|---|
| no quotas | **0 / 1000** | 0KB | 4096KB |
| cold 1MB / hot 2MB | **942 / 1000** | 508KB | 3588KB |

*(8MB arena, ~4MB data region. `cold` writes 1000x500B once; `hot` then writes
30000x500B — about 20x the arena.)*

The quota needed no new structure, because the zero-copy second chance made
protection free: at the tail, an entry of an under-quota namespace was given
another lap and an over-quota one was dropped, with the CLOCK reference bit
deciding for namespaces without a quota.

That measurement is why the quota was the one namespace capability a caller
could not reproduce in one line — and it is also why it could not survive the
L3 seam, since Valkey evicts across the whole database and the guarantee would
hold in L2 and break in L3. **Namespaces and their quotas are removed; see
decision 63.** The eviction path is now plain CLOCK: at the tail, a live entry
whose reference bit is set is given another lap and the bit cleared, bounded by
the re-append budget. `TC_LAYOUT` 6 (the header table went with them) and
`TCS_LAYOUT` 2 (the namespace id left the submission-ring record), so a process
built before the removal cannot attach to an arena built after it.

### Three operational bugs found by auditing what was still open

**The worker outbox was unbounded.** Writes batch until `setImmediate` fires, but
a worker doing a long *synchronous* burst never turns the event loop, so nothing
flushed. 60,000 sets produced **zero flushes and 37.5MB of retained worker heap**.
`process.send` can be called at any time — the tick is only there to batch — so
the outbox now flushes eagerly past a byte cap (1MB default):

| outbox cap | flushes | retained worker heap |
|---|---|---|
| unbounded | 0 | **+37.5MB** |
| 1MB | 31 | **+7.2MB** |

**A flush racing primary shutdown killed the worker.** The scheduled flush fired
after the primary had gone, and `process.send` failed with `EPIPE`. The failure
is *asynchronous*, so a `try/catch` around the call cannot see it — Node emits an
unhandled `'error'` event that terminates the process. Passing a callback to
`process.send` routes the failure to the callback instead; the dropped batch is
counted in `stats.flushDropped`. Losing a batch during shutdown is acceptable;
crashing the worker over it is not.

**A crashed primary leaked its shared-memory segment permanently.** `create()`
unlinks any prior segment of the same name, but the name was pid-derived, so a
crashed run's segment had a name nothing would ever reuse — verified: after a
`SIGKILL` the segment and its contents were still there, and there is no portable
way to enumerate POSIX shm on darwin, so the leak is invisible until reboot. At
the 128MB default that is roughly 500 crashes to exhaust 64GB. The name is now
derived from the application's identity (`argv[1]`/cwd, plus an optional
`TURBOKV_ID`), so a restart reclaims its own segment while different
applications on one host still get different ones. Verified: after a crash, a
restart finds the previous run's data gone.

### A clear crosses processes, and so must its guard

`clearAll()` empties L1, L2 and L3, and the L3 half takes a round trip. Until it
lands, L3 still answers with everything the clear is removing, so any read that
goes through to L3 in that window promotes those values straight back into the
arena with a fresh TTL (decision 70). The guard against that was a module-scope
counter, `l3ClearsInFlight` — itself a widening of an earlier per-instance flag,
correct as far as it went, because several cache instances in one process share
one arena (decision 64).

**The arena is shared across PROCESSES, and a module-scope counter is not.**
Reproduced with a file-backed L3, a real primary and a real worker: the worker
calls `clearAll()`, its L3 clear takes 400ms, the primary applies the clear
through IPC so L2 is empty — and then the primary's own `getAsync('k')` is
completely unguarded, because *its* counter is zero. It reads the not-yet-cleared
value out of L3 and writes it into the shared arena for a full 60s TTL. When the
worker's clear finally lands and L3 is empty, `worker.get('k')` returns the value
it cleared, out of L2. So decision 70's "until it lands this process serves
misses" did not hold even for the process that issued the clear, and the mirror
direction — primary clears, worker reads — is the same hole.

**The signal has to live where the sharing lives: in the arena header.** Two
monotonic counters, `l3ClearGen` (clears handed to L3) and `l3ClearSettled`
(clears that landed, or that `close()` gave up on); `gen != settled` means some
process on this box has a clear on its way to L3, and no process may then serve
or promote an L3 read. Monotonic counters rather than a flag for the reason the
process-local count was a count: two overlapping clears must not have the first
one's completion unblock reads while the second is still outstanding. And a
reader samples `gen` before its `await` and compares it after, so a clear that
both began and settled inside one read — where the in-flight count is back to
zero — is still seen.

**Only the primary may write it.** A worker maps the arena `PROT_READ`, so a
store into the header is a SIGBUS the process cannot catch, not an exception it
can handle. A worker's clear therefore opens and closes its generation the way
its `clearAll` already travels: as ops in the IPC batch the primary applies,
pushed alongside the `c` rather than folded into it, so that a `clearAll` with no
adapter still reaches the arena exactly as it did before. That leaves the header
lagging a worker by one flush — which is precisely why the process-local counter
**stays** alongside the generation rather than being replaced: it is the only
signal that exists during that window, and it is also all a degraded worker with
no arena to read has left. Nothing can be harmed inside the lag either, because a
clear the primary has not applied yet has not emptied L2 yet: anything promoted
before it lands is wiped by it, and anything promoted after it sees the
generation already open.

**`TC_LAYOUT` 6 → 7.** The two counters are appended after `readsSkippedNoLz4`,
so every existing field keeps its offset and the 64-byte rounding in `create()`
absorbs them whole (`sizeof(Header)` 264 → 280, `indexOff` 320 either way) —
nothing in the data region moves, and `sizeof(Entry)` and `sizeof(SubmitRec)` are
untouched. The bump is not about offsets: a process built before this change
attaches to an arena built after it, never looks at these counters, and promotes
over another process's clear, which is the whole defect. The layout gate is what
stops those two builds from sharing an arena, and the gate case in
`native_regression_test.cc` covers it symbolically, so it needed no new value.

**A clear that never lands must not wedge anything.** The queue retries a clear
indefinitely by design, and while it retries the guard stays armed — correctly,
because L3 really does still hold what the clear was meant to remove. `close()`
is what settles it: it stops the retry loop, the operation settles, and the same
promise chain that decrements the local counter also closes the generation.

**But an open generation is a cluster-wide outage of the L3 tier, so every route
that could leave one open has to be closed.** The guard has no key list — a
clear has no keys — so while it is armed, every L3 read in every process on the
box misses and every promotion is refused. Two routes could leave it armed
forever. The obvious one is a worker killed mid-clear, which can never send its
`-`. The likelier one needs no crash: `flush()` defers a whole batch while the
IPC send window is full and sheds it once the outbox is full too, and nothing
re-arms a deferred flush except the next write — so the `-` of a worker that has
just flushed its cache, and therefore has nothing more to say, could sit in the
outbox indefinitely or be thrown away with a batch of writes.

Three things close them, and none of them is a timeout:

- **A `-` is never shed with ordinary writes.** Shedding a write costs one key
  its place in L2 and the value is still in L1; shedding a `-` darkens L3 for
  the whole box. It carries no key and no value, so keeping it cannot be what
  grows the outbox.
- **While it is still queued, an unref'd timer keeps retrying the flush**, so it
  leaves on its own rather than waiting for traffic that may never come — and
  unref'd, so a settle is never the reason a process cannot exit.
- **The primary keeps a per-attachment count of generations opened and not
  settled**, and reconciles what a worker still owed when its channel emits
  `'exit'` or `'disconnect'`. `install()` wires that;
  `TurboKV.releaseWorker(message)` is public for a primary that routes cluster
  messages itself, for the same reason `applyBatch` is. It takes a *message*
  from the worker, never a writer id — see below — and it **throws** on an id,
  because that call used to work, would now settle nothing, and a silently
  leaked generation is a cluster-wide L3 outage. What `install()` keeps is the
  name (`{ id, n }`) and not the message: `m.b` holds the batch, and keeping it
  would pin the largest batch a worker ever sent for the life of the channel.

That count does a second job. `-` is a lever that disarms a cluster-wide guard,
and it arrives over a channel every worker holds: before this, a worker could
only *wipe* the cluster's cache, and `-` would have let it resurrect what
someone else's clear was removing. Counting per sender turns the lever into an
accounting rule — a sender may settle exactly as many generations as it opened,
and a `-` matching no `+` of its own changes nothing.

**Reconciliation is itself the same lever, held by the primary — so it must not
be aimable at the wrong worker.** Counting per *writer id* looked per-worker and
was not: `attachWorker` takes the id from the caller, and naming workers by a
stable slot index out of the environment is how it is normally done, so a
replaced worker inherits its predecessor's number. Releasing by that number
settled the SUCCESSOR's outstanding clear, dropping the guard while L3 was still
mid-clear — the resurrection this whole wave exists to prevent, arrived at
through ordinary supervision rather than through a race. So each process mints a
nonce when it loads and carries it on its batches; generations are counted per
nonce, and `install()` reconciles a channel by naming it with one of that
channel's own messages rather than with a number. A successor is a different
attachment however early it attaches, which is what makes the ordering safe: the
predecessor's `exit` may arrive long after the successor is already clearing,
and it still cannot name it.

**And the `c` itself is kept out of the shed path, not just its bookkeeping.**
The first version of the keep-list saved `+` and `-` and left `c` to be shed
with the writes, which is the worst of the three: the clearAll is dropped on the
floor while the guard around it opens and cleanly settles, so nothing reports a
failure and L2 serves values L3 no longer has. The argument for keeping the
generation ops — no key, no value, one per clearAll — is the argument for
keeping the clear.

**A batch that names no attachment gets no generation at all.** Falling back to
the writer id for such a message reads as a kindness to an older worker and is
the original defect wearing a different hat: a mixed-version rolling restart is
*precisely* the stable-slot deployment that makes ids collide, so the fallback
re-opened the hole the nonce closes, silently, and only during an upgrade. The
`+` and the `-` are both refused, and the refusal is reported through
`lastError` rather than swallowed. The `c` in that same batch still applies:
a wipe can only REMOVE data, which every worker on that channel can already ask
for, and refusing it would leave L2 serving exactly the values that worker's L3
clear is removing — a permanent divergence, where the refused generation costs
one unguarded round trip. What must not survive is a `+` that opens a
generation nothing can reliably settle.

**Two residues, recorded rather than fixed**, because each needs a protocol
change larger than its risk. The accounting is keyed on an identity the *sender*
declares, so it closes the unpaired disarm but does not bind a generation to the
channel it arrived on: a worker could still name another worker's attachment.
That is the same trust domain that already lets any worker send `c` and wipe the
cluster's cache. And settling on `disconnect` disarms the guard while that
worker's clear may still be landing in L3, so a graceful rolling restart — the
ordinary case for a disconnect — opens a real resurrection window; waiting for
`exit` instead would hold the guard armed for a worker that can no longer close
it, so neither choice is free.

### Adversarial review — findings and fixes

An independent adversarial review attacked the six load-bearing invariants, ran
ASan and UBSan for the first time, and audited the benchmarks. It found **six
severity-1 defects**. All are fixed; `src/review_regression_test.js` pins
every one.

| # | Defect | Fix |
|---|---|---|
| 1 | **Keys truncated at 511 bytes and folded to latin1**, so distinct keys returned *each other's values*. `get(K×511+'A')` → `"value-B"`; a never-set key returned data; `'中'` aliased `'-'`. L1 masked it within a process, so it surfaced only on L1 misses and cross-worker. | Keys are read as UTF-8 and capped at 1024 bytes; longer keys are rejected rather than truncated. |
| 2 | **Index saturation permanently bricked the arena.** The load-ceiling loop gave up after a fixed 4096 iterations, so under overwrite-heavy traffic `live` crept to 100%; `logAlloc` ran *before* `findFreeSlot`, so a failed insert left an uninitialised header whose garbage `blockSize` desynced the tail walk and orphaned the whole index. Repro ended with 60,000/60,000 sets failing and nothing readable, permanently. | Bound the loop by real progress (`logTail < logHead`) rather than a count; allocate, then secure the slot, then unlink the old entry; write a PAD record if the slot cannot be secured. |
| 3 | **TTL was lost whenever L1 refilled from L2** — the refill path never carried the entry's expiry, so any expiring value read once through L2 became immortal in that worker. | The arena reports the entry's expiry (`lastExpiresAt`), and the refill carries it into L1. |
| 4 | **The own-write ring skip served deleted values.** "Our own write, L1 is already correct" was false when L1 had been refilled from L2 between queuing and apply: a worker that deleted a key then read it in the same tick served the deleted value forever. | Own records are no longer skipped; the cost is one L2 refetch after each own write. |
| 5 | **The primary's L1 was never invalidated by worker writes.** `applyBatch` wrote the arena directly and the primary's drain is a no-op, so a primary that also reads served its own stale value indefinitely. | `applyBatch` drops the affected key from every cache instance in the process. |
| 6 | **A worker forked before the primary opened the arena silently created a second, writable one** and `shm_unlink`ed the primary's — total silent cache failure. | A worker never creates; it throws a message naming the ordering requirement. |

Severity 2, also fixed: `#byHash` grew without bound (32MB per 300k distinct
keys); a failed `set` destroyed the previous value; the worker-side size check
compared UTF-16 units against a UTF-8 limit and ignored the 4MB scratch cap;
`install()` attached twice if called between `fork()` and `'online'`;
`namespaceStats()` before an arena and any use after `close()` were hard
SIGSEGVs; `ringAppend` published the head before writing the record, leaving a
window in which a reader could permanently miss one invalidation.

Severity 3, also fixed: `ttlMs` near `INT32_MAX` overflowed and expired
immediately; TTL is now **millisecond**-precise rather than rounded up to whole
seconds (a 1ms TTL could be served for up to 2s); namespace names longer than 23
bytes silently shared one id and quota; the slab free-list link was a misaligned
`uint64_t` store (UBSan); compaction adjusted `liveBytes` but not `nsBytes`.

**What survived.** The reviewer could not break the `tailPub <= pos` liveness
proof, backward-shift deletion under concurrent probes, the zero-copy second
chance, or namespace quota accounting (`Σ nsBytes == liveBytes` exactly after
300k churn ops, no underflow, over-commit still made progress). ARM ordering
checks out. ASan+UBSan across ~100M reads produced no reports, and are now part
of `run_sanitizers.sh`. The TSAN gate was also flaky — its allowlist named only
writer-side frames, but TSAN attributes the deliberate race to whichever thread
detects it, so reader frames appear intermittently; both sides are now allowed.

### Full performance matrix, re-measured

Everything below was re-measured after the review fixes (UTF-8 keys, own-write
invalidation no longer skipped, TTL sweeping, the zero-copy second chance,
atomic reference bits). `bench/single_matrix.js` and `bench/cluster_matrix.js`,
sharing `bench/adapters.js` so both drive each implementation identically.

The workload is objects. Who encodes them differs by mode, and that cost is
charged where it falls: `bytes` has no codec, so the application stringifies and
parses, and those calls are inside the measurement.

**Cluster, 1 primary + 4 workers, 60k shared keys.**

| | 90/10 read/write | | | 50/50 write-heavy | |
|---|---|---|---|---|---|
| | ops/s | hit | p50 | ops/s | hit |
| `turbo/bytes` | 650k | 90.5% | 4.1µs | 551k | 89.4% |
| `turbo/direct` | 447k | 90.5% | 5.1µs | 315k | 89.6% |
| **`turbo/safe`** | **694k** | 90.5% | **3.9µs** | **638k** | 89.4% |
| bugsee | 82k | 30.4% | 54.8µs | 85k | 23.3% |

`safe` wins in the cluster, which inverts the single-process ranking. Its writes
are cheap (`JSON.stringify` on a hot path Node 26 made 34% faster) and it skips
the string flatten that `bytes` pays on every write. `direct` is last because
`v8.serialize` on every write is 2–3x JSON, and a cluster workload writes on both
explicit sets and miss-fills.

**Scaling is the headline.**

| workers | `turbo/bytes` | hit | bugsee | hit |
|---|---|---|---|---|
| 1 | 213k | 80.4% | 57k | 80.4% |
| 2 | 374k | 85.7% | 89k | 68.4% |
| 4 | 666k | 90.5% | 80k | 30.2% |
| 8 | **1,040k** | **94.1%** | **42k** | **12.8%** |

turbokv scales 4.9x across an 8x worker increase, and its hit rate *improves*
(80.4% → 94.1%) because more workers fill the shared arena faster. bugsee peaks
at two workers and then **goes backwards** — 89k to 42k — while its hit rate
collapses to 12.8%. At 8 workers the gap is **24.8x**.

That collapse is not a defect in bugsee so much as the architecture reaching its
limit: every L1 miss is an IPC round trip through one event loop, and past
saturation its client times out at 100ms or hits its 256-request pending cap and
resolves `undefined`, which the application sees as a miss. Its L2 holds the
data; it simply cannot be reached in time. Degrading rather than queueing without
bound is a deliberate and defensible choice.

**Tail latency** at 4 workers, 90/10: p99 18.5µs vs 121µs, p99.9 297µs vs 3.7ms.
At 8 workers bugsee's p50 alone reaches 181µs.

**What the modes are actually for**, given both tables:

- **`bytes`** — the default. Best when values are already scalars or bytes, and
  competitive everywhere. The app owns any codec, so it can skip encoding
  entirely for opaque payloads.
- **`direct`** — only when reads dominate *and* values carry real JS types. It is
  2.7x the field on an L1-resident read workload and last everywhere else.
- **`safe`** — best in the cluster, and the friendliest contract (a fresh mutable
  object per read). Pay for it in silent type conversion.

### Windows: the actual gap

Audited rather than estimated. The platform-specific surface is **31 lines out of
~2,600**, and all of the hard part sits in three functions in `store.h`:

| file | lines | platform-specific |
|---|---|---|
| `store.h` | 301 | 24 (8.0%) |
| `store_ops.h` | 650 | 2 (0.3%) |
| `binding.cc` | 887 | 4 (0.5%) |
| `turbokv.js` | 800 | 1 (0.1%) |

**The mechanical part** — `create()`, `attachReadOnly()`, `openHints()`:

| POSIX | Windows |
|---|---|
| `shm_open` + `ftruncate` + `mmap(RW)` | `CreateFileMapping(INVALID_HANDLE_VALUE, …)` + `MapViewOfFile(FILE_MAP_ALL_ACCESS)` |
| `shm_open(O_RDONLY)` + `mmap(PROT_READ)` | `OpenFileMapping(FILE_MAP_READ)` + `MapViewOfFile(FILE_MAP_READ)` |
| `munmap` + `shm_unlink` | `UnmapViewOfFile` + `CloseHandle` |
| `fstat` for size | already in the header (`totalBytes`) |
| `sysconf(_SC_PAGESIZE)` | `GetSystemInfo` — but note view offsets must be multiples of `dwAllocationGranularity` (64KB), not page size. Hints are a separate object mapped at offset 0, so this does not bite today; it would if hints ever moved back inside the main segment. |
| `clock_gettime(CLOCK_REALTIME)` x3 | `timespec_get` (C11, MSVC has it) |
| `usleep` (a test hook) | `Sleep` |

**The part that is not mechanical — lifetime semantics.** POSIX shared memory
persists until `shm_unlink`; a Windows file mapping is **reference-counted** and
dies when the last handle closes. That inverts one of our fixes: the "a crashed
primary leaks its segment until reboot" bug cannot occur on Windows, and the
stable-naming fix becomes merely harmless there. The flip side is that an arena
cannot outlive every process holding it, so a POSIX-only behaviour — a worker
attaching to an arena whose creator already died — has no Windows equivalent.
Any Windows port has to decide whether that divergence is acceptable or whether
the POSIX side should be constrained to match.

**Naming** also differs: POSIX wants `/name` (≤31 bytes on darwin), Windows wants
`Local\name` for a session-scoped object (`Global\` needs
`SeCreateGlobalPrivilege`). `defaultName()` needs a platform branch; `Local\` is
the right scope for a `cluster`.

**The safety guarantee needs re-establishing, not assuming.** The current claim is
descriptor-level: the arena fd is `O_RDONLY`, so a worker cannot map it writable
even deliberately. The Windows analogue is that `OpenFileMapping(FILE_MAP_READ)`
returns a handle whose access rights forbid a writable view. That *should* be
equivalent, but `protect.js` proves the POSIX case by observing a SIGBUS, and
Windows raises `EXCEPTION_ACCESS_VIOLATION` with no signal — so the test needs a
platform branch and the guarantee needs re-verifying on real hardware rather than
inherited by analogy.

**Two blockers already removed** while auditing:

- `binding.cc` contained one GNU statement-expression (`({ … })`), which MSVC
  rejects. Replaced with a plain function; there are now none.
- `binding.gyp` piped `find_lz4.js` through `cut`, and gyp's `<!()` runs in the
  platform shell — `cut` does not exist on Windows. `find_lz4.js` now prints a
  single field on request.

**And one that never existed**, thanks to vendoring: upstream `rapidhash.h`
already carries `_umul128`/`__umulh` paths for MSVC. The hand transcription it
replaced used `__uint128_t` unconditionally, which MSVC has no equivalent for —
so the transcription would have been a Windows blocker in its own right.

**Testing would be partial.** ASan exists for MSVC; **TSAN does not**, so the
sanitizer gate that guards the lock-free read path could not run on Windows at
all. That is an argument for treating Linux/macOS as the platforms of record for
concurrency verification regardless of whether the port happens.

## 10. Why not V8 fast calls

The original premise. It does not survive contact:

1. **The header is not shipped.** `v8-fast-api-calls.h` is absent from Node's public headers on 22.20.0, 24.14.0 and 24.15.0. In `v8-template.h`, `CFunction` is only forward-declared (line 21) — you can pass a `CFunction*` but cannot construct one. Node core uses fast calls because it builds against the full V8 tree. Using them from an addon means vendoring an unsupported internal header pinned to V8 13.6.233, where a layout mismatch is a runtime crash, not a build error.
2. **The prize is ~6ns.** A Node-API no-op is 8.1ns; a fast call would be ~2ns. Against ~21ns for an L1 hit and ~150ns+ for an L2 hit, that is noise.
3. **Node-API is worth more than 6ns.** A stable ABI means one prebuild per platform works on every current and future Node major — no rebuild treadmill, which was the largest recurring maintenance cost in the original plan.

Also, fast calls only accept `const FastOneByteString&`, so any two-byte key would have deoptimized to the slow path regardless.

---

## 11. Testing

`npm test` and CI both run `test/run.js`, which is the single list of JS suites — the list used to live in the workflow only, which is how a test stops being run without anyone noticing.

**JS suites** (`test/`)
- `test.js`, `api_test.js` — core behaviour and the public API surface.
- `codec_test.js`, `prim_test.js`, `v8codec_test.js`, `storage_modes_test.js`, `typeflow_test.js`, `typematrix_test.js` — the three storage modes. `typematrix_test.js` asserts a full input-type × mode matrix, including the cells JSON is *supposed* to degrade.
- `json_fastpath_test.js` — that no codec falls off Node's JSON fast path.
- `cluster_api_test.js`, `transport_regression_test.js` — cross-process behaviour, the latter over **both** transports, driving the public API in a real worker rather than the internals.
- `instances_test.js` — two `TurboKV` instances in ONE process stay coherent: a write on one drops the key from the other's L1, `clearAll()` included.
- `recovery_test.js` — primary death, restart, stall and resume, using `child_process.fork` because under `cluster` a worker dies with its primary and none of those cases arise.
- `guard_test.js` — that the heap guard actually fires, on the registry path and on the backstop alone.
- `review_regression_test.js`, `gaps_test.js`, `perf_regression_test.js` — one case per defect found by adversarial review; the last asserts *ratios* between configurations, never absolute times, so a slow CI machine moves both sides together.

**Native** (`test/*.cc`, no Node required)
- `submit_test.cc` — the SPSC ring: wrap, varied record sizes, a full ring, malformed records, a hostile header, a SKIP flood, and SPSC concurrency under TSAN.
- `native_regression_test.cc` — TTL across the uint32 epoch wrap, the log's wrap gap against a canary, and header geometry validation.

**Sanitizers** (`test/tsan/run_sanitizers.sh`) — TSAN across four arena configurations then ASan+UBSan across three, judging races by *address* within the arena data region rather than by symbol name, after a name-based gate was shown to pass a deliberately injected race.

**Types** (`test/types/`) — `tsc` against `index.d.ts`, with `@ts-expect-error` on six deliberate misuses. A declaration file that accepts everything type-checks fine and is worthless; tsc fails on an *unused* expect-error, so the file only passes when each of those really is an error.

**Load** (`loadtest/`) — a Docker harness running four flows across multiple workers for 10+ minutes, self-verifying every value, with leak detection on the post-GC floor and CPU accounting per operation.

**What is not tested:** no fuzzing of the entry decoder against adversarial arena bytes, and TSAN cannot observe cross-process races at all, so the multi-process evidence remains empirical.

---

## 12. Decision log

| # | Decision | Alternatives | Rationale |
|---|---|---|---|
| 1 | L2 in a shared arena, primary is sole writer; workers map it read-only | multi-writer shared memory; IPC request/response to primary | Sync reads with no cross-process locks and no macOS robust-mutex problem; a worker cannot corrupt the arena, and the primary is off the read path |
| 2 | Fully synchronous v1 API; async arrives as separate methods with L3 | `Promise` API from day one | Promise alloc + microtask tick is 100–300ns on a ~21ns operation; would pay the L3 tax for years before L3 exists |
| 3 | rapidhash **64**-bit + `memcmp` verification | 64-bit unverified; 128-bit unverified | Key text is stored for L3 anyway, so verification is nearly free — exactly correct instead of probabilistic, 8 bytes smaller, and enumeration stays possible |
| 4 | Values limited to `string` / `Buffer` / `Uint8Array` / `ArrayBuffer` | `v8::ValueSerializer` for arbitrary `any` | Structured clone costs 0.5–3µs and would dominate every other cost in the system |
| 5 | **L1 is a JS `Map`, not a native store** | native off-heap L1 | Measured: `Map` ~20ns/hit with zero allocation vs native 30ns (200B) to 439ns (16KB), allocating a fresh V8 string every hit |
| 6 | Large L1 values as external strings created once at insert | copy on every hit; externalize on every call | Flat ~21ns hits at any size with bytes off-heap; per-call externalization (~56ns) loses to copying below 1KB |
| 7 | Strings by reference, buffers copied | all by reference; frozen buffers | Immutability makes string sharing free and safe; mutable buffers would let one caller corrupt every reader |
| 8 | Invalidation via shared-memory ring buffer | IPC broadcast; per-entry version check on hit | `O(1)` per write regardless of worker count, zero IPC, and L1 hits touch one hot cache line instead of a cold per-entry one |
| 9 | CLOCK / sampled eviction | strict LRU | Strict LRU costs `delete`+`set` (~50–80ns) on every `Map` hit — more than tripling hit cost |
| 10 | **LZ4 off by default and in v1**; per-cache opt-in with a 1KB floor | compress above a threshold; compress in both layers | **Overturned by measurement.** Compression is a density win only, costing ~3x reads and ~9x writes. At a fixed 16MB budget it buys +4.8 points, but 32MB uncompressed beats 16MB compressed on *both* hit rate and latency, and the benefit reaches exactly zero once the working set fits. L2 is one arena per host, so the RAM is trivial. Justified only under a hard memory cap, where a miss must also cost >8.8µs. |
| 11 | When compression *is* enabled, the worker performs it | in the primary, synchronously | Distributes CPU across workers, shrinks IPC payloads, preserves `memcpy`-only propagation in the primary. Open: the primary's sweeper could instead compress cold entries in the background, keeping writes fast and recovering density only under pressure. |
| 12 | **Node-API, no V8 fast calls** | direct V8 with a vendored `v8-fast-api-calls.h` | The header is not shipped and `CFunction` is incomplete; the prize is ~6ns; Node-API's stable ABI removes the per-Node-major rebuild treadmill |
| 13 | Sizes computed at startup, then fixed | runtime-adaptive L2 | Growing L2 requires remapping in every live worker mid-read; a fixed mapping is the largest available simplification |
| 14 | Batched, fire-and-forget writes to the primary | synchronous write-through | Keeps `set()` off the IPC critical path; costs ~1 tick of cross-worker staleness |
| 15 | Current LTS, darwin + linux, x64 + arm64 | Windows in v1 | Windows needs `CreateFileMapping` — a second shared-memory implementation |
| 17 | **No background compaction** | async compress-on-the-threadpool with version-validated apply | Built and proven race-safe (18k stale captures correctly discarded, 0 wrong values), but worth only +1.9 points of hit rate at 3x read latency, while doubling the arena buys +6.9 points at no cost. Restricting to cold entries removes the latency penalty *and* the entire benefit. The machinery was removed in decision 47. |
| 37 | **Three-way adversarial review; every severity-1 finding fixed and pinned** | ship the transport as written | Three independent hostile reviews (shared-memory ring / native arena / JS layer). **Ring:** the primary re-read `ringCount`/`ringBytes`/`maxKey`/`maxVal` out of a segment workers map read-write, so one worker could fault it (SIGSEGV) or disable validation -- geometry is now snapshotted once into process-private fields and every offset derived, never read back. SKIP records advanced `tail` without incrementing `applied`, so a budget counted in applied records never bit: a ring of SKIPs with a forged head walked 717M iterations against a budget of 4096 -- `head - tail` is now clamped to capacity and every *step* counts. **Arena:** namespace ids were never bounds-checked, and `ns >= 16` wrote through `nsBytes[]` into `nsQuota`/`nsProtected` and past ~77 into the INDEX, permanently corrupting a live slot that eviction could never reclaim. Unpaired surrogates all encode to U+FFFD, so `'\uD800'`, `'\uDC00'` and `'\uFFFD'` were ONE key returning each other's values -- the same aliasing class as the latin1 folding already fixed once, and namespace names still had the literal latin1 bug. `incr`'s 8-spin retry cap reported 62,855 spurious misses in 151M reads of a key that was never absent. **JS:** the primary's L1 never followed worker writes on the new default transport -- a silent stale-read regression introduced with the ring. |
| 43 | **The heap guard is driven by a FinalizationRegistry, debounced, with a floor-poll backstop** | gc `PerformanceObserver` (previous behaviour); raw polling; forced `global.gc()` | The observer is Node-only -- Bun and Deno accept the subscription and emit nothing, so the guard was silently inert on two of three runtimes -- and it is less accurate than it looks, firing on scavenges that do not collect old space, so most samples are taken mid-garbage. Measured against a known 42.7MB live set under churn: observer mean **147.3MB**, FinalizationRegistry mean **52.2MB**, raw polling 147.1MB, min-over-window polling 85.3MB. A finalizer runs only after its sentinel was actually collected, so sampling there is a genuine post-collection reading, and it exists on all three runtimes. The registry is re-armed on every callback including debounced ones: dropping a sample must never drop the signal. The spec promises nothing about finalizers running, so a 1s floor poll (min of a 16-sample window) takes over after 5s of silence -- worse than the registry, far better than raw polling, and unconditional. |
| 44 | **The data region is any size; only the index and invalidation ring stay powers of two** | round the data region down to a power of two so the log can mask (previous behaviour) | Rounding down silently discarded up to half the arena -- 24MB, 26MB, 28MB and 32MB requests all yielded exactly 16MB of data, so capacity could be doubled but never tuned and the sizing formulas named numbers nobody received. A modulo costs +2.96ns per computation on a dependent-chain microbenchmark, which is real, but an end-to-end A/B at an equal working set shows no regression at all (519.5ns modulo vs 536.0ns mask per L2 read). An earlier A/B suggesting +6.2% was confounded: the two builds had different data-region sizes and so different eviction rates -- a difference in working set, not in addressing. The index keeps its mask because open addressing probes with it on every lookup, a far hotter path. `TC_LAYOUT` 4. |
| 60 | **The primary drains its submission rings on the maintenance tick, not only on a doorbell** | rely on the doorbell alone (previous behaviour) | A worker pushes to its submission ring and then rings `RING_MSG`; that doorbell was the ONLY thing that made the primary drain, because the primary's own `get()` returns immediately from `#drain` for id 0 and the maintenance tick only stamped a heartbeat and swept expiries. The doorbell is swallowed in two places -- `if (process.connected)` and a `catch` around `process.send` -- and losing one does not merely delay invalidation: **the record stays in the ring, so the write never reaches L2 at all** while `set()` reported success. Demonstrated by a worker that drops its own doorbells: without the backstop the value is still `undefined` after four maintenance intervals; with it, the write lands. Workers were never exposed to this because they drain on every operation and so self-heal; the primary now has the same property, bounded to one maintenance interval. Measured cost on an idle ring: **29.1ns per call, ~5ms of CPU per day** at the 500ms cadence, which is why the only real cost was not having it. |
| 61 | **`minLevel` is a per-operation placement preference, never stored with the record** | store the level in a spare `ns` bit so it applies to every future read; a size threshold (`l1MaxValueBytes`) alone | Measured first. One pass over a cold keyspace evicts **93.7%** of a hot working set from L1, costs **1.54x** on the next pass (against 6-7ns of run-to-run spread), and the scan itself is **3.83x** more expensive than the same scan that does not promote -- the batch job pays for its own pollution, so both sides win. A size threshold was the obvious automatic alternative and is **not sufficient**: with cold values the same size as the hot ones it has nothing to discriminate on, and that scan evicted **100%** of the working set. Pollution is a property of the access pattern, and only the caller knows it is scanning. Storing the level with the entry was rejected on the stronger argument that it locks a value to one writer's opinion: another process may legitimately want it resident, and a record is data, not policy. So the option is per-operation, and `Entry` stays 40 bytes with no `TC_LAYOUT` bump. An unavailable level is clamped DOWN to the highest that exists rather than rejected, so `L3` is usable today (behaving as L2) and starts using L3 the day the seam lands, and a degraded worker clamps to L1 -- where its writes already go. A value that is not a level at all still throws (decision 48). **Invariant: `minLevel` changes only where a record lives and how fast it is reached, never which value is observed.** That holds for free on the primary, whose `set` is synchronous, but a worker bypassing L1 must also EVICT the local copy (or it serves the old value) and mark the key pending like a queued delete (or its own next read finds L2's PREVIOUS value -- wrong, not stale). Both guards are fault-injected: removing the eviction fails the stale-copy assertion, removing the pending mark returns `WORKER-OLD` to the process that just wrote `WORKER-NEW`. **`minLevel: L3` writes nothing locally, and that had to be enforced on the WRITE path, not just claimed.** A whole-branch review found `set` splitting `minLevel === 1` from everything else and then running the L2 write unguarded, so `setAsync(k, v, { minLevel: L3 })` put the value in the shared arena AND told the adapter `willCache: false`. The read path had it right all along (`#fillFromL3` fills only at `level <= 2`), so write and fill disagreed. The `willCache: false` half is the expensive one for the provider that comes next: the remote store registers no interest for a key this box is in fact holding, sends no invalidation for it, and the stale read is permanent and cross-box. The eviction rule above applies at L3 too and for the same reason -- a resident L2 copy is deleted rather than left behind, or the option would produce stale reads instead of misses. |
| 62 | **`incr` and `cas` are removed rather than extended to L3** | keep them local-only; give them async-only variants | Neither can be atomic across L1, L2, L3 and many boxes without a global ordering this design does not have, and a synchronous return value cannot carry a remote atomic result. The codebase already drew this line: `cas` refused in a worker because "a queued CAS whose outcome the caller never learns is not a CAS", and L3 makes that true in every process. A caller who needs an atomic counter already has a Valkey client and `INCR`. Reverses decision 1, whose remaining wart -- `incr`'s return type differing by process role -- disappears with the method. The removal reaches the native layer too: with the JS methods gone, `Incr`/`Cas` in `src/binding.cc` and `storeIncr`/`storeCas` in `src/store_ops.h` were reachable from anyone who could `require` the addon directly and covered by no test -- the same shape of problem decision 47 deleted the compaction machinery for, not merely unused code left in place. Deleted along with their `FN(...)` registration entries; `findSlot`, `storeSet`, `unlinkSlot` and `ringAppend` all keep other callers, so nothing else moved. Pinned by a native-surface check in `api_test.js` alongside the public-surface pin from decision 59. |
| 63 | **Namespaces are removed** | keep them; keep only the quota as an eviction class | A namespace was a key prefix plus a one-byte tag charging a byte quota. The prefix is something callers do themselves in one line. The quota is the only capability they cannot reproduce -- measured in §9 at 0/1000 cold survivors without it and 942/1000 with it -- but it is L2-only and cannot exist in L3, where Valkey evicts across the whole database, so namespaces would promise in one tier what the next tier breaks. With L3 attached, losing an entry from L2 costs a round trip rather than the data. Removal also deletes a verified aliasing bug (namespace `users` key `42` and default-namespace key `users:42` were one entry, because identity is the full string and names were never checked for `:`), the 15-namespace limit, open item 16, and a namespace clear that wiped every other namespace's L1. If a workload ever needs eviction isolation, it returns as a per-write class without namespaces coming back as key identity. **The removal reaches the arena itself.** The JS layer came out first, passing a literal `0` to native signatures that still took a namespace id; leaving it there would have been the worst of both -- a parameter no caller can vary, a header table nothing reads, and a quota branch in the eviction path that is dead by construction and therefore covered by nothing. So the native half went too: `nsResolve`, `clearNamespace` and `nsStats` are deleted from `binding.cc` along with their `FN(...)` entries (pinned by the native-surface check from decision 62), the `ns` argument is gone from `set`, `del`, `scanKeys`, `submitSet` and `submitDel`, and `Header` loses `nsCount`, `nsName`, `nsBytes`, `nsQuota`, `nsProtected` and `nsDropped` -- 900 bytes and the fixed 16-entry table. `Entry.ns` and `SubmitRec.ns` go with them; both structs stay 40 and 24 bytes, because the byte each field freed is padding either way, so nothing else that indexes by `sizeof` had to move. The eviction quota branch collapses to the plain CLOCK decision it already fell back to (`protect = liveHere && hints[slot]`), which deletes the `byQuota` accounting but leaves the zero-copy second chance and -- critically -- its verify-room-before-writing check untouched: that check is the fix for a real data-corruption bug (writing `bsz` bytes at the head unchecked, in the one branch that only runs when free space is scarce), and it is orthogonal to quotas. Reversing decision 38, whose whole subject was honouring a quota under index pressure; the bounded budget it added to the index-eviction loop stays, because the reference bit it also rescued is still there. Both layout versions move -- `TC_LAYOUT` 5 -> 6 and `TCS_LAYOUT` 1 -> 2 -- which is what makes a pre-removal build refuse a post-removal arena and ring instead of misreading a `Header` that is 900 bytes shorter and a record whose fields shifted. That refusal is exercised by the layout-gate case in `native_regression_test.cc`, which bends a real arena's published `layout` word one version behind and one version ahead of `TC_LAYOUT` and asserts `attachReadOnly` refuses both, then confirms a matching layout still attaches. |
| 64 | **A write on the primary drops the key from every other instance in the process** | leave it; make instances share one L1 | Two instances in one process served each other stale values, including deleted ones: `one.set('k','A'); two.set('k','B'); one.get('k')` returned `A`, and it still returned `A` after `two.delete('k')`. The primary skips the ring records it writes itself (`writerId === 0`), which silently assumes a process holds one cache -- but opening the cache from two modules is ordinary, and `open()` was given a second-handle path on purpose. The worker drain had already abandoned the same shortcut for a neighbouring reason ("false whenever L1 had been refilled from L2 between queuing and apply"); the primary kept it. Sharing one L1 between instances was rejected: the per-instance byte budget and the heap guard are per instance, and merging them would make one instance's pressure evict another's entries. Verified by injection: reverting the two `set`/`delete` call sites fails two assertions in `instances_test.js`. `clearAll()` turned out to be the same bug in the same family, one screen away: its primary branch called `this.clearLocal()` for the caller and wiped the shared arena, but never touched any other in-process instance's L1, so `one.set('k','A'); two.clearAll(); one.get('k')` still returned `A` after the clear. The flush marker `clearAll` already writes to the invalidation ring does eventually reach a sibling, but only whenever `#primaryInvalidate` next runs after a drain, leaving a window where a sibling serves values that no longer exist anywhere else. Fixed the same way, with a `#clearOthers` helper next to `#dropOthers` since there is no single key to target -- every other instance's L1 is reset wholesale via `clearLocal()`. Verified by injection: reverting only the `clearAll` call site fails one assertion (a key the first instance had cached in L1 stays stale after the second instance clears). A parallel assertion for a key the first instance had *never* cached -- reachable only through the arena -- was written and checked by the same injection: it passed with the fix reverted too, because `native.clearAll(0)` empties the shared arena unconditionally on both sides of this fix, so an L1 miss falling through to it was never broken. That assertion was removed rather than kept as decoration. |
| 65 | **The L3 adapter is four required methods and three optional ones, validated at construction** | a class users extend; a registry of named drivers; duck-typing checked lazily at first use | Users implement this themselves, so the contract is the API and it should be small enough to hold in one's head: `get`, `set`, `delete`, `clear` required; `has`, `subscribe`, `close` optional. Optional members are the ones a non-Valkey store may not have cheaply -- `has` falls back to `get`, and an adapter without `subscribe` relies on TTL for cross-machine staleness. Validation happens where the adapter is supplied, not on first use: a missing method discovered on the first cache miss is a TypeError inside a promise chain in a process that has been serving for an hour, which is the worst possible place to learn it. Errors throw rather than returning a result type, because that is what `async` already does and it keeps the contract free of a turbokv-specific error shape. |
| 66 | **A per-key ordered queue with coalescing, bounded and retried, between turbokv and the adapter** | call the adapter directly from `set`; one global chain for the whole process; unbounded retries | `set(k,A); set(k,B)` over a connection pool can arrive in either order, leaving L3 with A while this box holds B -- and the next invalidation makes every other box agree with L3 rather than with us. So order per key is enforced. Order ACROSS keys deliberately is not: one chain for the process would let a single slow key throttle everything. Coalescing falls out of the same structure -- a write not yet sent is simply replaced, so a hot key costs one round trip rather than N, and the superseded caller's promise resolves with the newer write's outcome, which makes its meaning "L3 holds your value, or a later one from this process". The queue is bounded by bytes, because a slow L3 would otherwise grow it without limit; past the bound work is shed under the same contract as a full submission ring. Retries are bounded by a time budget so a promise settles exactly once and never resolves false for an operation that later succeeds on its own. `clear` is the one exception: it retries indefinitely, because until it lands this process serves misses rather than values the clear was meant to remove, and flushing twice is harmless. **Every attempt is bounded, not just the retry budget.** `retryMs` was consulted only in a `catch`, so an adapter call that neither resolved nor rejected never reached it: `setAsync`, `deleteAsync` and `drainL3` never settled, and on the read side `getAsync`'s `.finally` never ran, leaving a permanent `#inflight` entry that handed every future read of that key in this process the same dead promise even after L3 recovered. "Threw" and "never settled" are one outage seen from two angles and only the first reaches a `catch`. Each attempt now races `retryMs` and a timeout is treated as a failure, which puts a hang back on the existing retry-and-abandon path. `retryMs` is reused rather than given an option of its own: it is already defined as the per-operation budget before an op is abandoned, a call that has not settled within it has already spent that budget, and a second knob would only let the two disagree. The read path has no queue to hold the budget for it, so the cache keeps `l3RetryMs` itself and bounds `get` and `has` with the same number. **`clear` is also the one exception to the cross-key rule, and the spec wins over this decision there.** §10.2 says later operations queue behind a clear; this decision says order across keys is not preserved. They disagreed, and the disagreement was a defect: a `set` still in flight when `clearAll()` was issued landed in L3 *after* the clear, so L3 kept the key the clear had just removed and the next `getAsync` pulled it back into the local tiers -- a clear that reported success and had silently undone itself. The rationale above does not reach this case: "one slow key must not throttle the process" is about ordinary per-key work, and `clear` means *every key*. So a clear is a BARRIER: operations already ON THE WIRE are awaited before it is sent, operations PENDING BUT UNSENT are dropped and settle with the *clear's* outcome -- the rule coalescing already applies to a superseded write, whose promise means "L3 holds your value, or a later operation from this process" -- and operations pushed AFTER it queue behind it, which is what makes §10.2's "later writes survive" guarantee true. The dropping happens at push time and not at dispatch time, because only push order distinguishes work the clear supersedes from work that must outlive it. This cannot hang the process against a dead L3, where a clear retries indefinitely by design: work waiting behind the clear keeps counting against `maxBytes`, so later pushes are shed and settle `false` under the same contract as a full submission ring, rather than accumulating without bound. |
| 67 | **Level availability is a table, not a ceiling** | keep the single `highest` clamp; reject a level that does not exist | `#resolveLevel` computed one ceiling because levels were contiguous: L1 always, L2 unless the primary died. With an adapter that stops being true -- a degraded worker has L1 and L3 but no L2 -- so `minLevel: L3` on such a worker must stay at 3 while `minLevel: L2` clamps to 1. The clamp RULE is unchanged (down to the highest level that exists, so data is always stored somewhere); only its computation is. Rejecting an unavailable level instead was rejected for the reason decision 61 gives: `L3` must be usable today and start using L3 the day it lands, without the caller changing anything. |
| 68 | **Writes go local-first, and the async variant differs only in what it awaits** | L3 first, so local tiers never hold what L3 rejected; a trailing write with no way to observe it | L3-first makes split state impossible, but its answer to an unreachable L3 is to write NOTHING -- not even locally -- which turns a remote outage into a local write outage. Local-first keeps the 45ns local write and keeps the cache serving through an outage, and the promise still carries the guarantee, so the caller who wants it can still wait. The window where local holds a value L3 has not accepted is bounded by the round trip, and other processes cannot see the local write until the primary drains the submission ring anyway -- documented at ~1 event-loop tick against 50-200us for a remote hop, so the window is mostly hidden. On failure the value is KEPT locally under a short TTL: serve through the outage, converge afterwards. The honest cost is that after that TTL a write reported as failed reverts to L3's older value. Background failures never touch `lastError` -- the sync call returned long ago -- so they go to stats and an optional listener. **The short TTL is now applied, rather than only documented.** A whole-branch review found `l3FailTtlMs` written in the constructor and read nowhere: the option, its `index.d.ts` entry, the failure matrix and two code comments all asserted a behaviour that did not exist, and without it a failed L3 write diverged from L3 permanently -- for the common `ttlMs: 0`, forever -- which is exactly the split brain the option was added to bound. It is applied on BOTH abandonment routes, because the failure matrix names both: an op that outlived `l3RetryMs`, and one shed on the spot past `l3QueueMaxBytes`. It SHORTENS ONLY. L1's deadline is lowered in place, and an entry that already expires sooner keeps its own. L2 has no retime operation, so the cap has to be re-applied by writing the value again -- and that is only safe while the arena still holds THIS value, so it is guarded by a compare against what is resident. If a later write superseded it, or a delete removed it, the cap is skipped: a local entry that outlives its cap costs a longer disagreement, while resurrecting an older value costs correctness, and *a miss is the failure mode this system is built around; a stale value is not*. Nothing is capped at `minLevel: L3`, where nothing was written locally to begin with. |
| 69 | **A read marks the invalidation ring before it goes to L3, and refuses to promote if its key was invalidated meanwhile -- or if this process has already deleted it** | promote unconditionally; refuse to promote whenever the ring head moved at all; version every entry; for the pending delete, wait for the invalidation to come around, or serve L3's value and let the delete catch up | A read issued before a write can return the older value after it, and promoting that value overwrites the newer one in L2 -- not stale, WRONG, and it stays wrong until the next write to that key. The ring is already monotonic and `ringHead()` is already exported, so the mark costs one native call. Checking for the key's own hash rather than "did the head move" is what makes it usable: under load the head always moves, so the conservative version would never promote and L3 hits would never reach L2. **Three things block a promotion, not one.** Two of them are the ring answering *did anyone else change this key*: the key's own hash, and the flush marker, which is not a hash at all and means EVERYTHING changed -- a guard comparing only hashes lets a value a `clear` just removed straight back in, so both are fault-injected separately. `wrapped` falls back to conservative, which is correct and rare. The third asks a different question -- *did I already remove it* -- and the ring cannot answer it: on a worker the delete is applied by the primary a tick later, so during that window there is NO ring record to consult, which is exactly why the guard must consult `#pendingDel` itself rather than wait for the invalidation to come around. Reading through to L3 there returns the value the delete is on its way to removing, and promoting it sends that value back into L2 *after* the delete -- a resurrection every process in the cluster then sees, permanent until the next write. It is checked twice, before the request and after it, because a delete issued while the read was in flight is past the first check. **What the caller gets differs by reason, and the difference is the point.** A pending delete returns `undefined`: `get` already answers `undefined` for those keys, and the sync and async forms must not disagree about a delete this caller made and was told had succeeded. An unhashable key (a lone surrogate) still returns L3's value: L3 legitimately holds a key L2 cannot represent, `get`'s `undefined` there means "L2 cannot store this" rather than "this is deleted", and refusing the promotion costs residency, not correctness. `getAsync` is a superset of `get` by construction -- every L3 hit is a value `get` could not produce -- so the rule is not "they always agree", it is "they never disagree about this process's own state". **Three counters, so a dashboard stays readable:** `l3PromotionsBlocked` is contention -- this key changed while the read was in flight, or the ring cannot rule out that it did (degraded, detached, `wrapped`) -- and NOT a catch-all; `l3UnhashableKeys` is a key that can never live in L1 or L2 at all; `l3DeletedWhileReading` is a prevented resurrection. Folding the last two into the first would make the number an operator watches for promotions drying up unreadable. **There are FOUR block reasons, not three, and the fourth is the one this decision got wrong.** The third reason above -- *did I already remove it* -- was implemented only through `#pendingDel`, which exists only on WORKERS. The primary has none, because it applies a delete to the arena synchronously and has nothing to remember. So for one L3 round trip per delete the primary's local tiers were correct, the DEL was still on the wire, L3 still answered with the value, and a concurrent `getAsync` fetched it back and wrote it into the SHARED arena for a full TTL -- visible to every process on the box, after `delete` had returned true and `get` was already answering `undefined`. `delete` then read is an ordinary cache-aside sequence. The same hole covered a `minLevel: L3` write, which evicts the local copies and tells the adapter `willCache: false`: while its SET was on the wire a read promoted L3's PRE-write value into L1 and L2, where no provider would ever invalidate it, and reachable on workers too one drain later, when the ring clears `#pendingDel` while the L3 operation is still outstanding. **So the guard asks the L3 QUEUE what this process still owes L3 for the key, covering both the operation queued and the one in flight, and reporting the newest.** The queue answers a question the ring structurally cannot: the ring records changes that HAVE been applied, and an operation that has not reached L3 has by definition produced no record anywhere -- which is also why this check runs BEFORE the degraded-worker bail-out, since it holds whether or not there is an arena to read a ring from. It is one `Map` lookup on the read path, and free with no adapter, where there is no queue to ask. An outstanding DELETE returns `undefined` and counts `l3DeletedWhileReading`, for the reason this decision already gives for `#pendingDel`: `get` answers `undefined` for it, and the two forms must not disagree about this process's own state. An outstanding SET refuses the promotion and counts `l3PromotionsBlocked` but still returns what L3 holds -- that *is* contention on this key, just observed from our own queue rather than the ring, and L3 genuinely holds that value at that instant, so the caller is not being told anything false. **And there are FIVE, the fifth belonging to decision 70 rather than to this one.** The ring cannot answer "is a clear on its way to L3 right now" for the mirror-image reason the queue check exists: another process's clear puts its flush marker on the ring when the ARENA is emptied, which is before the adapter has applied it, so a read that starts after that marker sees a quiet ring, is handed the value the clear is still erasing, and writes it into the arena every process shares. `l3ClearedWhileReading` reads the arena header's clear generation, which is the only signal here that crosses a process boundary, and it joins `l3DeletedWhileReading` as a reason that changes the answer rather than only the placement. It also consults the process-local counter, because the header lags a worker by one flush and a read that worker already had in flight lands inside that lag. Named for the blocked read rather than for the gauge it consults: `stats.l3ClearedWhileReading` counts events, `native.l3ClearsInFlight()` measures a level, and one name for both would have made a dashboard lie. |
| 70 | **`clear` empties every tier, and until it lands EVERY process sharing the arena serves misses** | clear only the local tiers; delete L3 keys by prefix in the background | With an adapter attached the tiers are ONE cache, so `clear()` that emptied only the local tiers would silently undo itself: the next read refills everything from L3. The queue cannot shed a clear and retries it indefinitely, because flushing twice is harmless while never flushing is not. While it is pending, L3 reads in this process return misses rather than the values the clear was meant to remove -- a miss is the failure mode this system is built around, a resurrected value is not. `delete` needs no such state: if L3 is unreachable, reads cannot refetch either, so the delete holds for the whole outage, and the only case where the old value returns is reads succeeding while the DEL failed, which the caller was told about. `has` gets an optional adapter member because `EXISTS` is trivial for Valkey and not universal; without it the fallback reads the value and throws it away, which is correct and wasteful in a way the contract makes visible. **"This process" was the wrong scope, and a process-local flag of any kind could not have been right.** The pending state started as a per-instance boolean, was widened to a module-scope counter because several instances in one process share one arena (decision 64) -- and that was still a process-local answer to a cross-process question. The arena is shared, so while THIS process serves misses, another process reading the same L3 promotes the not-yet-cleared values into the same arena, and the clearing process is then served its own cleared value out of L2. Reproduced with a file-backed L3, a real primary and a real worker, in both directions. No refinement of a JS variable can fix it: the processes share memory and an arena, and nothing else. So the pending state lives in the arena header as two monotonic counters (`l3ClearGen`, `l3ClearSettled`), and the promotion guard gains a FIFTH block reason, `l3ClearedWhileReading`, which -- like `l3DeletedWhileReading` and for the same reason -- changes the ANSWER and not merely the placement: the clear already emptied L1 and L2 in every process, so a `getAsync` serving what L3 still holds would contradict both `get` and the entry guard that has been answering misses since the clear was issued. Only the primary may write the header (a worker's mapping is `PROT_READ`; a store there is a SIGBUS), so a worker's generation is opened and closed by ops in the IPC batch that already carries its `clearAll`, and the module-scope counter stays alongside the generation to cover the flush the header lags by -- and to be the only guard a degraded worker still has. `TC_LAYOUT` 6 -> 7: the counters are appended, so no existing field moves and the 64-byte rounding absorbs them (`sizeof(Header)` 264 -> 280, `indexOff` 320 either way), but a build made before this change would ignore them and promote over another process's clear, which is exactly what the gate exists to prevent. **The guard blocks reads of EVERY key, not only the ones a clear removed.** A clear has no key list -- that is what makes it a clear -- so while one is outstanding, every L3 read in every process on the box misses and every promotion is refused, for the whole round trip. That is the price of the guarantee and it is charged cluster-wide, so a workload that clears often pays it often; `clear` is an administrative operation, not a cache-shaped one. **A generation left open is therefore a cluster-wide outage of the L3 tier, and TWO routes could leave one open.** The obvious one is a worker killed mid-clear, which can never send its `-`. The likelier one needs no crash at all: `flush()` defers a whole batch while the IPC send window is full and sheds it once the outbox is full too, and nothing re-arms a deferred flush except the next write -- so the `-` of a worker that has just flushed its cache, and therefore has nothing more to say, could sit in the outbox forever or be thrown away with a batch of writes. Three things close them. A `-` is never shed with ordinary writes (it carries no key and no value, so keeping it cannot be what grows the outbox); while it is still queued an unref'd timer keeps retrying the flush, so it does not wait for traffic that may never come; and the primary keeps a per-writer count of generations opened and not settled, reconciling what a worker still owes when its channel emits 'exit' or 'disconnect' (`install()` wires this; `TurboKV.releaseWorker(message)` is public for a primary that routes cluster messages itself, exactly as `applyBatch` is). That count is also what makes `-` safe to accept at all: it is a lever that disarms a CLUSTER-WIDE guard, arriving over a channel any worker holds, so a worker may settle exactly as many generations as it opened and a `-` matching no `+` of its own changes nothing. **Reconciliation is keyed on the ATTACHMENT, not on the writer id.** `attachWorker` takes the id from the caller and a stable per-slot index is the ordinary way to name workers, so ids are REUSED across restarts: keying on one let a dead worker's reconciliation settle the clear of the live successor that took its slot, dropping the guard mid-clear and resurrecting exactly what that clear was removing -- no misuse required. Each process mints a nonce at load and carries it on its batches; the primary counts generations per nonce and `install()` reconciles a channel by naming it with one of its own messages, so a successor is a different attachment however early it attaches and the predecessor's `exit` can arrive whenever it likes. **A clearAll is not shed under congestion either.** The keep-list that saves `+`/`-` from the shed path saves `c` with them, for the same reason and by the same argument (no key, no value, one per clearAll): losing the `c` loses the clear itself while its bookkeeping reports success -- the guard opens and cleanly settles while L2 goes on serving values L3 no longer has. A batch that names no attachment gets NO generation -- falling back to the writer id would re-open this very finding during a mixed-version rolling restart, which is the same stable-slot deployment -- while the `c` in it still applies, because a wipe can only remove data and refusing it would leave L2 serving what that worker's L3 clear removes. `releaseWorker` takes a message and THROWS on a writer id: that call used to work and would now settle nothing, and a silently leaked generation is a cluster-wide L3 outage. `install()` keeps `{id, n}` rather than the message, so reconciliation does not pin a worker's largest batch for the life of its channel. **Two residues, recorded rather than fixed.** The accounting is keyed on an identity the SENDER declares, so it closes the unpaired disarm -- a `-` with no `+` now does nothing -- but does not bind a generation to the channel it arrived on: a worker could still name another worker's attachment, which is the same trust domain that already lets any worker send `c`. And settling on `disconnect` disarms the guard while that worker's clear may still be landing in L3, so a graceful rolling restart opens a real resurrection window; the alternative, waiting for `exit`, holds the guard armed for a worker that can no longer close it, and neither is free. A clear that never lands keeps the guard armed for as long as it is genuinely outstanding, which is correct -- L3 really does still hold what it was told to remove; `close()` settles it, by the same promise chain that decrements the counter. |
| 71 | **`close()` returns a promise, and the wait it does is bounded** | leave it synchronous and let the adapter leak; a separate `shutdown()`; await the L3 queue's drain unconditionally | An open L3 connection keeps Node's event loop alive, so a synchronous `close()` would leave a process that cannot exit -- the kind of defect that shows up as a hung CI job rather than a failing test. Draining the queue first is part of the same obligation: work already accepted from a caller should reach L3 before the process goes away. Widening the return type from `void` to `Promise<void>` is backwards compatible for every caller that ignores it, which is the normal case, and a second method would leave `close()` as a trap. **The drain wait is bounded, and that bound is load-bearing, not cosmetic.** A `clear` is never shed by the queue and retries indefinitely by design (decision 70): until it lands, this process must serve misses rather than the values the clear was meant to remove. So if L3 is unreachable when `close()` is called, the queue's pending count never returns to zero and `drain()` never resolves -- awaiting it unconditionally would hang `close()` forever during exactly the outage where an operator most wants the process to exit. `l3CloseTimeoutMs` caps the wait (default 5000ms, the same window `l3FailTtlMs` already uses elsewhere in this design; 0 opts out and waits without a bound, for a caller that would rather hang than risk dropping work) and on expiry `close()` proceeds anyway: a caller that asked to close gets to close. **The honest cost of that bound:** whatever is still queued when it expires does not get cancelled -- it keeps retrying in the background, against an adapter this process is about to hand its own `close()` to, and it gets no second chance to be waited on. It either lands silently with nothing left to observe the outcome, or it exceeds `l3RetryMs` and is abandoned and reported through the normal `onL3Error`/stats path (moot for a `clear`, which never abandons on its own), or it is still retrying when the process itself exits and the outcome is simply never known. A caller closing during a known outage who needs a stronger guarantee than "waited up to N seconds" has to get it some other way -- there is no way to make waiting on an unreachable peer both bounded and complete. **The bound covers the adapter's own `close()` too, separately from the drain.** A first pass bounded only the queue's `drain()` and left `await this.#l3.close()` unbounded on the theory that "close" is a fast local teardown -- wrong in general: a real client's `close()` can itself wait on a graceful-shutdown handshake (in-flight commands draining, a TLS close_notify) that never completes if the peer is the thing that is down. `close()` therefore races the adapter's `close()` against the same `l3CloseTimeoutMs` ceiling, as a second, independent bound after the drain's. Worst case this makes `close()` take roughly 2x `l3CloseTimeoutMs` (once per bounded step), which is the honest price of a guarantee that covers both places an adapter can hang rather than only the one this task started with. The implementation order matters too: the L3 half runs *after* the existing synchronous teardown (ring release, primary arena destroy), not before it. An earlier draft put it first, which is a real defect this task's own tests caught -- every pre-existing caller in this codebase invokes `close()` without awaiting it, and delaying the synchronous teardown behind an `await` left the arena undestroyed when the very next test opened a new one, corrupting unrelated tests. The L3 shutdown never touches arena state (queued writes go straight to the adapter, not through L2), so nothing requires it to run first, and running it last keeps every existing fire-and-forget `close()` caller working exactly as before. `close()` also clears `#inflight`, the herd-sharing map for concurrent `getAsync` reads: a read stuck against a hung adapter connection is not cancelled by this either (nothing here can reach into the adapter to stop it), but clearing the map drops this instance's own reference so a stalled read cannot keep a cache `close()` has already torn down reachable for the life of that hang. **`close()` is idempotent.** It is exactly the method that gets called from more than one place -- a shutdown hook, a signal handler, a test's own teardown -- and two of those firing is ordinary, not exotic; an independent review confirmed a second call re-invoked `adapter.close()` and re-ran the drain, unguarded. The public `close()` now only memoizes and returns a private `#closePromise`, produced by `#doClose()` (the method that used to be `close()` itself); every call after the first returns that same promise rather than a fresh one, so two callers awaiting `close()` from different places observe the real outcome together rather than one getting a synthetic "done" ahead of the other. **Bounding the WAIT was not enough to bound the PROCESS.** A whole-branch review verified `clearAll(); await close()` against a failing `clear`: `close()` returned, honouring every bound above, and the process then hung until it was killed. The bounds made `close()` return; they did not make the queue stop. A `clear` is never shed and retries indefinitely (decision 70), and its retry backoff was a ref'd `setTimeout` that went on rescheduling itself for the life of the process -- against a cache that no longer existed. The same hung CI job this decision exists to prevent, reached by a different route. Two changes, and neither substitutes for the other: `close()` tells the queue it is CLOSED, so the retry loop ends at its next attempt instead of retrying forever; and the sleeping backoff is UNREF'D at that moment, so the wait already in progress stops holding the event loop immediately rather than one backoff later. The backoff is unref'd at close rather than at creation deliberately -- an unconditionally unref'd backoff lets a process with nothing else ref'd exit MID-RETRY, so `await setAsync(k, v)` against a failing L3 never resolves and accepted work is dropped with no caller left to hear about it (measured: the probe exits 0 before its own `await` returns). The flag is set AFTER the bounded drain, so work already accepted still gets its bounded chance to land. **The guarantee is now tested as a guarantee**: a child process sets up a failing `clear`, closes, and must exit on its own within a bound, with the parent's kill timer turning a regression into a failed assertion rather than a wedged CI job -- nothing inside a process can assert that it exited. One more thing the same review found here: **`close()` must never reject.** Its synchronous teardown can throw (`stopGuard()`, the ring-release loop, the unwrapped `native.destroy()`), and widening the return type turned every one of those throws into a rejection of a promise that every caller deliberately ignores -- an unhandled rejection, which Node 18 turns into process termination. A failure to release a ring slot would have killed the process it was cleaning up. The teardown and the L3 half are now caught separately and reported through `lastError` and `onL3Error`'s already-declared `close` kind, and `close()` resolves either way. |
| 59b | **`api_test.js` ran only its first half** | n/a -- a defect, not a choice | `process.exit(fail ? 1 : 0)` sat at line 68 of 126, so every block appended after it was dead code. Two were: the public-surface pin, and the create-failure diagnostic test added during the coverage work in decision 55 -- which was reported there as covering `#createError` and never executed once. Made live, it failed immediately, and for a real reason: it asserted Linux's behaviour unconditionally, but whether an oversized arena fails at all is platform-dependent. Linux reserves the space with `posix_fallocate` and fails; macOS allocates shared memory lazily and returns a 1TB arena without complaint. The test now asserts what each platform actually does. Test-that-cannot-fail number twelve, and the second in a row found by fault injection rather than by review. |
| 59 | **The public surface is exactly what `index.d.ts` declares, and a test pins it** | leave internals reachable but undocumented (previous state) | An audit of the class against the declarations found **15 members reachable but undeclared** -- three statics and twelve instance members. Undeclared-but-reachable is an API you support whether you meant to or not, which is the same problem `TurboCache.native()` had in decision 45. Three were genuinely public and are now declared: `isCacheMessage` and `applyBatch` as a documented PAIR -- `install()` is the easy path, but an application that already routes cluster messages needs the first to identify ours and the second to apply them, and declaring only one would let a caller recognise a message it had no supported way to handle -- plus `l1Size` alongside `size`. Ten moved off the surface: seven `_`-prefixed internals became true `#private`, `useSubmissionRing` with them, and `callArgCounts` left the class entirely for `src/fastpath.js`, which an installed consumer cannot reach because the `exports` map has no deep paths. `__internalOnGc` is the one member that CANNOT be private: `gcNotify()` is a module-scope function declared above the class, so it has no access to a `#private` -- the wrapper is load-bearing, and is now named to say so. Two JS constraints cost time and are worth recording: a class may not have a static and an instance private of the SAME name (`static #staleMsFor` plus an instance `#staleMsFor` is a SyntaxError, which V8 reports against an unrelated line), and privatising a `_name` that already has a `#name` counterpart silently creates infinite recursion. `api_test.js` now pins the surface, verified to fail three ways: an undeclared static appearing, an undeclared instance member appearing, and a declared member vanishing. It must check instance OWN properties as well as the prototype -- `stats`, `lastError`, `storage` and `liveHeapFraction` are assigned in the constructor, so a prototype-only check reported four declared members as missing. |
| 58 | **`create()` releases the segment it replaces; tests release theirs on exit** | raise the container's `--shm-size` until the suite fits (what the release workflow first did, at 8g) | The musl release job failed with turbokv's own diagnostic -- *"/dev/shm holds 64MB (64MB free) but the arena needs 128MB"* -- and raising `--shm-size` would have hidden a real resource leak behind a bigger bucket. Two distinct causes. **In-process:** `native` is process-global, so a second `create()` REPLACES the first, but it overwrote `base` and `name` without releasing either -- the old mapping and its shm name both survived for the life of the process. `attachReadOnly` and `Submit::open` each already carried exactly this guard, with a comment describing the leak; `create()` was the one that was missed, and the one that also owns a name to unlink. `Submit::create` had the same hole, and the ring's NAME could only be released in the binding because `Submit` tracks none. **Across processes:** a POSIX segment outlives its creator until unlinked -- correct for a live primary, wrong for a test -- so ~24 test processes each stranded their last arena and ring. `test/_cleanup.js` is preloaded through `NODE_OPTIONS` from `run.js`, so a test added later cannot forget and forked workers are covered too; it is a no-op unless the addon was loaded, and on a worker's read-only attach it closes without unlinking a segment it does not own. Measured in the release job's own image: **8g before, 256m after**, with the residue being one deliberate 128MB arena in `perf_regression_test`. Pinned by `shm_leak_test.js`, which scans from OUTSIDE the creating process -- the only place a stranded segment is observable -- and was itself caught being vacuous first: it derived its name prefix from `process.pid` in both parent and child, so the parent scanned names that never existed and could only ever report zero. Verified to fail with 11 leaked once the prefix was shared. |
| 57 | **Publishing authenticates with OIDC trusted publishing, not a stored token** | a long-lived `NPM_TOKEN` in repository secrets (what the workflow first used) | A publish token in CI is a credential that outlives the job, sits in a settings page, and is exactly what recent npm supply-chain compromises have turned into published malware. Trusted publishing removes it: the workflow proves its own identity to npm over OIDC, npm checks that against a publisher configured on the package, and provenance is attached without asking. Requirements are specific and worth recording -- `id-token: write` on the job, Node >= 22.14.0, npm >= 11.5.1 (newer than what the Node 24 runner image carries, so it is upgraded explicitly), and `NODE_AUTH_TOKEN` must be ABSENT: if it is set npm uses it and the OIDC path is silently skipped, which would look like success. The sequencing constraint is that a trusted publisher is configured in an EXISTING package's settings, so the first version cannot use it -- there is a one-time `bootstrap: true` input that publishes with the token, after which the token is revoked and that path can never authenticate again. Note also that trusted-publisher configurations created from 2026-09-03 default to `npm stage publish`, with direct `npm publish` as an explicit opt-in, so the allowed action has to be chosen deliberately when configuring it. |
| 56 | **The package is `turbokv`, and the class is `TurboKV`** | `turbocache` (previous name); `@krassx/turbocache`; keep the class as `TurboCache` under a `turbokv` package | The name `turbocache` is taken on npm -- a 139-byte proprietary placeholder published in 2020 by an unrelated author and untouched since 2022 -- so `npm publish` would have failed with a 403. That was missed when the publish workflow was built, which is the wrong order: check the name is claimable before building a pipeline to claim it. `turbokv` was verified available against the live registry, along with the rest of the shortlist. A scoped `@krassx/...` name was the zero-risk alternative and was rejected only because an unscoped name is claimable here and reads better in an install line; `turbo-cache` was rejected for sitting adjacent to Vercel's `turbo*` trademark in the same ecosystem. The class was renamed with the package rather than left mismatched: there are no published consumers, so this was the only free moment, and `Cache` remains as an alias for anyone who prefers a neutral name. The addon target moved too, so the shipped binary is `turbokv.node` rather than a file named after a different package. Deliberately NOT renamed: the `MSG = 'tc'` IPC tag and the `/tc-` arena-name prefix are wire and shared-memory identifiers rather than branding -- the prefix is length-constrained and changing it would need a `TC_LAYOUT` bump for no benefit. 62 files, verified by the full suite, both native tests, the declaration type-check (which self-references the package through the `exports` map, so it proves the new name resolves on both the CJS and ESM conditions) and the 95% coverage gate. |
| 55 | **JS line coverage raised to 95% by testing worker-side behaviour, not by exempting code** | `c8 ignore` the awkward blocks; lower the target; leave it at 89% | Going 89.21% -> 95.38-95.73% took four new test groups and **found a real bug**. `flush()` reserves the batch's bytes before calling `process.send`, and its catch returns them if the send throws synchronously -- the comment records that not doing so once 'wedged the worker for its lifetime while every set() still reported success'. But `let sendThrew = true` was declared INSIDE the `try`, so the `catch` read an out-of-scope name: any synchronous throw produced `ReferenceError: sendThrew is not defined` instead of the recovery, leaving the wedge in place AND throwing an unexpected error out of a timer callback. The fix had been inert since it was written because no test ever executed that catch. Coverage flagged the lines; writing a real test for them surfaced the defect. Also now covered, each verified to fail by injection: the IPC in-flight window shedding rather than growing unboundedly (3,978 writes shed), `age === -1` meaning 'never stamped' rather than dead (a `maintenance:false` primary must not degrade its workers), the submission ring full (2,950 shed) versus a value **permanently** too large for a ring record (rejected, with the limit named), `clearAll()` reaching a worker's L1 through the ring sentinel, worker-side `delete` and `clearNamespace`, and the create-failure diagnostic that names the `/dev/shm` shortfall and the Docker flag. **The measurement had to be made deterministic first.** Coverage swung 4.5 points run to run because workers flush in a SIGTERM handler and the parents were exiting on fixed timers, racing the flush; worse, the outcome was bimodal -- the recovery machinery's 53 lines either all landed or all did not. Tests now ASK workers to exit (a normal exit is what writes coverage) and wait for the exit event, with a signal only as a backstop. Spread fell to 0.35 points. Floors are 95/82/91, each set below the MINIMUM of four consecutive runs rather than the best one. Nothing is exempted: there is no `c8 ignore` in the tree. Native coverage rose as a side effect, 80.41% -> 87.88%. |
| 54 | **Coverage is measured for both layers; the JS floor is a gate, the native number is a report** | no coverage at all (previous state); one gate over both; a round-number target like 80% | Three measurement bugs had to be fixed before any number was worth reporting, and each one produced a confident, wrong figure. **(1)** Cluster workers were invisible: the suite ends them with `worker.kill()`, and V8 writes nothing on a signal -- measured, SIGTERM and SIGKILL both produce zero coverage files, while `process.exit()` writes normally. That hid the worker half of the JS layer (IPC outbox, submission-ring client, degrade/recover) and read as 77.84%. **(2)** The hook that fixed it also wrapped `process.exit` to flush -- but `v8.takeCoverage()` writes a snapshot and RESETS the counters, so Node's own exit-time write then recorded the same files as all-zero and the merge took the later record. `index.js` and `index.mjs` reported 0% while a test was demonstrably loading both. The hook now handles signals only, which is the sole case where Node writes nothing. **(3)** gcov emitted `.gcov` files with no line data at all when it could not resolve `../src/binding.cc` from the object directory, exiting 0 -- so the native report read 0%, not an error. It runs from `build/`, where that relative path resolves. Corrected: **89.21% lines / 83.12% branches**, identical across runs. Native coverage unions line hits across translation units, because `store_ops.h`, `submit.h` and `platform.h` are header-only and each object file sees only what its own TU instantiates -- `submit.h` measures 51.85% from the addon alone and **100%** once `submit_test.cc` is included. Native totals 80.41%. The JS floors (87/81/81) are the measured values less ~2 points for platform-dependent branches, and are verified to fail when raised above actual; they ratchet up and are never lowered to make a build green. The native number is reported rather than gated because gcov's line counting is not comparable to V8's statement counting, so a threshold there would be a number nobody could act on -- `TC_NATIVE_MIN` gates it once the measurement has been watched. Coverage earned its place immediately: `index.js` and `index.mjs` were genuinely at 0%, since every test required `src/turbokv` directly and nothing loaded what consumers import. `entrypoints_test.js` closes that and is verified to catch the ESM default-export defect from decision 51. What coverage does NOT measure here is the part that matters most -- the seqlock, the wrap arithmetic and the recovery state machine are correct or not by *interleaving*, which a line counter cannot see; the sanitizer gate and the fail-injection discipline remain the evidence for those. |
| 53 | **The addon declares the Node-API level it actually needs (8), not the newest (10)** | `NAPI_VERSION 10` (previous behaviour, unexplained since the first prototype commit); drop `engines` to `>=22` | `NAPI_VERSION` is not a target, it is a **requirement**: it becomes `node_api_module_get_api_version_v1()`, and a runtime whose own NAPI level is lower refuses the module outright. Node 18 and 20 cap at NAPI 9, so every Node below 22 **segfaulted inside `require()`** -- measured on a constant distro, natively, from a plain source build: 18 SIGSEGV, 20 SIGSEGV, 22 loads, 24 loads. Nothing in the addon needs it; the newest calls used are `napi_create_bigint_words` / `napi_get_value_bigint_words`, both NAPI 6. 8 is Node 18.0's level and matches the `engines` range, so the declaration and the manifest finally agree. **Half of this is Node's**: its version check calls `ThrowError` and returns `nullptr`, and the caller dereferences it without checking -- reproduced with a ten-line addon whose only content is `#define NAPI_VERSION 99`, which segfaults on Node 20, **22 and 24** alike (`x0 = 0` at the fault, inside `napi_module_register_by_symbol`). So a wrong version is our bug, and the total absence of a diagnostic is theirs; worth reporting upstream. Found by asking whether the new prebuilds would run on Amazon Linux 2023 -- they do, but AL2023's default `nodejs` package is 18.20.8, which is how a question about glibc surfaced a Node-API bug. The process failure is the one that matters: `engines` said `>=18` while the CI matrix was `node: [24]`, so the floor was never executed. The matrix now runs 18, 20, 22 and 24. |
| 52 | **Prebuilds ship inside the tarball; no download, no postinstall compile** | `prebuild-install` fetching from GitHub Releases at install time; keep requiring a compiler | The addon is Node-API, so the ABI is stable across Node majors and **one prebuild per (platform, arch, libc) serves every supported Node** -- there is no per-version matrix, which is what makes bundling them practical at 86KB each. `prebuildify` writes `prebuilds/<tuple>/`, `node-gyp-build` resolves at require time, and the tarball carries them, so a normal install is ~1s with no compiler, no Python and no network fetch. The download-at-install alternative keeps the tarball smaller but needs a postinstall script AND network, and fails closed in exactly the environments that most want a prebuild (air-gapped CI, restrictive proxies). Bundling also **fixes the documented Bun limitation for free**: `bun add` still reports "Blocked 1 postinstall", and the package now works anyway because the binary is already present -- verified, along with `npm install --ignore-scripts`, which is the same condition. `build/Release` still wins over `prebuilds/` in node-gyp-build's resolution order, so a local development build is never shadowed by a stale prebuild. The source-build fallback is unchanged and verified: with no matching prebuild the install script compiles as before. Release runners are pinned to the OLDEST supported image (`ubuntu-22.04`, `macos-14`) because a prebuild links the glibc it was built against, so building on a newer image would refuse to load on an older one. The publish job refuses to run unless all six prebuilds arrived and the tag matches `package.json` -- publishing with a platform silently missing is worse than not publishing, since that platform degrades to a source build with no warning and, on Bun, to no build at all. The one runtime dependency, `node-gyp-build`, is a single dependency-free file. |
| 51 | **The `import` condition gets its own declaration file** | one `types` entry for both conditions (previous behaviour); duplicate the declarations into `index.d.mts` | `index.d.ts` describes a CommonJS module, because the package is `"type": "commonjs"`. Under node16/nodenext TypeScript therefore models it as `module.exports`, so `import TurboKV from 'turbokv'` in an ESM consumer bound the whole namespace object: `TS2339: Property 'createPrimary' does not exist on type 'typeof import(".../index")'`. The runtime was always right -- `index.mjs` has a real default export -- so this was **types-only**, which is the worse kind: correct code failed to compile, and the package looked fine to anyone testing it by running it. `exports["."]` now nests `types` inside each condition (`import` -> `index.d.mts`, `require` -> `index.d.ts`). `index.d.mts` re-exports rather than duplicating, so the API is declared in exactly one place and the two cannot drift. Pinned by `test/types/esm.test.mts`: a `.mts` file is an ES module whatever the package's `type`, so resolving `turbokv` from it self-references through the real `exports` map and takes the same path a consumer does. Verified to fail two ways -- reverting the `exports` map reproduces the original TS2339, and removing the default export gives TS2613 -- and it carries four `@ts-expect-error` misuses so it cannot pass by accepting everything. Also checked end to end against a real `node_modules` consumer (ESM and CJS, `nodenext`, TypeScript 5.4 as CI pins), at runtime on Node, Bun and Deno, and that `npm pack` actually ships the new file. |
| 50 | **A stale index observation is settled by the tail, not waited out** | retry and sleep (previous behaviour); the same gate unconditionally at the top of every read | Found by an adversarial review of the eight commits above, and it falsifies a decision I made in that very batch. `storeGet` retries when a check fails, on the theory that a writer is mid-update and will finish. That is wrong for one reachable state: a reader matches `idx[i].hash`, the primary then evicts that slot and the head laps the record's bytes, and the reader loads `idx[i].off` and gets the stale position. The bytes there are somebody's payload, so `seq` reads even and the lengths are arbitrary -- the bound check then fails **identically on all 4096 iterations**, 4031 of them calling `platformSleepUs(1)`. Measured by constructing that state: **11.7ms for ONE synchronous `get` on macOS**, 1793ms for 100, and `platformSleepUs` is a 1-15ms `Sleep()` on Windows, so seconds of blocked event loop. The tail only advances, so `tailPub > pos` proves the record dead *now* and there is nothing to wait for; `relaxed` suffices because a stale smaller tail only declines the shortcut and the authoritative seq_cst check after the copy is unchanged, and the gate is under `if (retry)` so the common first iteration pays nothing. **I had rejected this gate during the mask fix**, on a measurement that was worthless: the test I measured with read 20k keys from an arena under heavy eviction and never entered the retry loop at all, because eviction runs `unlinkSlot` -> `indexRemove`, which clears the slot before the bytes are reused -- so every miss resolved at `HASH_EMPTY` (instrumented: 0 retries in 20000 reads). That assertion could not fail; it is replaced by one that constructs the stale observation directly and fails at 1793ms against a 50ms bound. The window is narrow -- 0 retries observed in 185M reads under real concurrency -- so this is a tail-latency cliff, not a steady cost. |
| 49 | **`SLAB` and `LOG` are removed; `LOG2` is the only arena mode** | keep all three compiled in (previous behaviour); keep them and add them to the sanitizer gate | Decision 16 chose `LOG2` on measurement and the other two stayed, reachable as arena modes 0 and 1. Three facts settled it. **They cannot be selected in the shipped product**: `createPrimary` takes no mode, the single `native.create` call hardcodes 2, and since decision 45 a consumer cannot reach the addon at all -- so they were live only for `test/test.js` and three benches. **They had no concurrency coverage whatsoever**: the sanitizer gate, both native C++ tests, the multiprocess test and the load harness all run `MODE_LOG2` exclusively, and `test/test.js` is single-threaded. Running the TSan harness against each mode for the first time found `SLAB` reporting 4 races to `LOG`/`LOG2`'s 0 -- all inside the arena data region, so passing the gate's own criterion, but one is `unlinkSlot` writing the free-list link into a live block **without bumping the seqlock**, while the reader's compensating liveness proof (`tailPub > pos`) was explicitly disabled for `SLAB`. That write lands on bytes [4,12) -- `slot` plus the low half of `hash` -- so it is caught by the `eh != hash` re-check or is benign, and never touches the value: sound, but by an accident of field layout that nothing asserted. And the same run reproduced decision 16's verdict directly: in 5s `SLAB` managed **820,657 writes to `LOG2`'s 1,346,619** (61%) with **200,785 misses against 3,848** (52x), which is slab calcification -- no victim in the right size class means the insert simply fails. Removal is worth no measurable speed: the `mode != MODE_SLAB` guard short-circuited a load `LOG2` performs anyway, one perfectly-predicted compare, and the header fields were 400 bytes (0.001% of a 32MB arena). The win is that the read path now carries ONE safety argument instead of two. `bench/gaps.js` took its no-second-chance baseline from `LOG`; it uses a second-chance budget of 0 instead, which is precisely what the mode difference was and isolates the variable within one allocator. `TC_LAYOUT` 5, so a mismatched build refuses the arena rather than misreading a Header that is 400 bytes shorter. The `mode` field survives for a future allocator. |
| 48 | **A misconfigured `storage`/`codec`/`values` throws a TypeError at construction** | accept anything and let reads return `undefined` (previous behaviour) | `storage` names a mode and `codec` takes an object, and they are one keystroke apart in meaning. Getting it wrong cost nothing at construction and everything afterwards: the cache was built, `set()` reported success, and every `get()` returned `undefined` with nothing anywhere to explain why. Measured over ten malformed configurations, **nine were accepted silently** -- `codec: 'direct'` (a string where a codec object belongs, and the most natural way to write what the author meant), a typo or wrong case in `storage`, a numeric `storage`, a codec missing either half. The tenth failed later, at the first read, with `this[#codec].decode is not a function`. Now each is a `TypeError` naming the mistake and the fix, including a did-you-mean for near misses (`'DIRECT'` -> `'direct'`) and the specific `codec: 'direct'` -> `storage: 'direct'` confusion. Same principle as `freeze` (decision 26) and the Date/Map/Set mutators (decision 38b): turn silent corruption into an exception where the mistake was made. Validating `storage` alone would have left a hole, because **`values` was documented as a legacy alias for it and was not one**: only `'bytes'`/`'primitives'` did anything, since those coincide with the internal no-codec flag the same option sets, so `values: 'direct'` and `values: 'safe'` silently selected BYTES mode and a caller following the declaration had every object rejected by `set()`. It is a real alias now, and validated identically. Every existing use in the tree passes `values: 'bytes'`, which already worked, so nothing changes for them. Pinned by `storage_modes_test.js` -- 24 failures with the check removed, and a companion assertion that all 8 valid configurations still construct, so the validation cannot pass by rejecting everything. Two defects in the first cut, found by the review in decision 50: the check ran in the CONSTRUCTOR, after `createPrimary`/`attachWorker` had already called `native.create`/`native.attach`, so a rejected option left an arena, a hints segment and a 32MB submission ring that nothing unlinked -- verified surviving the process that threw -- and set `storeReady`, so a caller who caught the TypeError and retried with corrected options got the fast path, skipping `_startMaintenance`: a primary that never heartbeats (`heartbeatAgeMs` -1 against 500 for a healthy one), which is exactly the signal workers use to declare it dead. Validation now runs at the top of all three entry points, before any native call. And the declaration's claim that `codec` is "mutually exclusive with a storage mode that implies one" was not enforced: `storage: 'bytes'` plus a codec was accepted and the codec silently discarded. `'direct'` and `'safe'` genuinely do take an override, so only the no-codec modes are refused. |
| 47 | **The rejected background-compaction machinery is deleted, not merely unused** | leave it compiled in (previous behaviour) | Decision 17 rejected background compaction on measurement, but the code stayed: `CompactItem`, `compactApply`, `compactStillValid`, the libuv `Job`, `CompactExecute`/`CompactComplete`/`CompactAsync`/`CompactStats`, and two registered addon methods -- ~200 lines reachable from the addon surface, carrying their own liveBytes/nsBytes accounting and a capture-apply race window, exercised by nothing in the test suite. Code that no test runs and no caller reaches still has to be read, reviewed and kept correct through every layout change: `compactApply` wrote through `entryAt` and `logAlloc` and so was silently in scope for decisions 44 and 46, for no benefit. Deleted, along with the two benches that were its only callers (`compact_bench.js`, `race.js`); git keeps both at `ec93071`, which is where the decision-17 numbers can be reproduced. Explicitly NOT removed: foreground compression is a live public feature (`compress`, `compressMinBytes`, `compressAccel`, `hasCompression()`, the opt-in LZ4 CI job) and shares only the `COMPRESSED` flag; `Entry::version` looked like compaction state but is what the invalidation ring publishes. Verified after removal: LZ4 build compiles and compresses (4000B payload to 72B live), default build has no `compactAsync`/`compactStats`, full suite, both native tests and the sanitizer gate green. |
| 46 | **The `direct` codec decodes into an unpooled, exactly-sized buffer** | `Buffer.from(s, 'latin1')` (previous behaviour) | `v8.deserialize` does not copy an `ArrayBufferView` out of its input. Node's `DefaultSerializer` sets `_setTreatArrayBufferViewsAsHostObjects(true)`, so a typed array is written as a host object and read back as a **view over the buffer handed to deserialize**. `Buffer.from` returns a slice of the shared 8KB pool, which had two consequences. Every cached typed array pinned a whole 8192-byte slab for its lifetime -- measured: an 8-byte payload holding an 8192-byte `ArrayBuffer`. And the value's correctness rested on the runtime deriving the view's address from a NON-ZERO `byteOffset`: Deno 2.8.3 adds that offset twice in `DefaultDeserializer._readHostObject` (`ext:deno_node/v8.ts`), so on Deno a `Uint8Array` came back **zero-filled** -- right constructor, right length, wrong bytes -- or threw `RangeError: Invalid typed array length` when the doubled offset ran past the end. Measured across offsets 0/8/32/100, Deno's returned offset is exactly `2*byteOffset + 5` where Node's is `byteOffset + 5`; offset 0 is correct on both, which is the fix. This was the long-standing 'Deno-only failure under the `direct` codec' -- not a Deno-only bug at all, but a latent aliasing dependency in our codec that Node's pool allocator happened to satisfy, because the pool bumps forward and never re-issues bytes it has handed out. An unpooled buffer has `byteOffset` 0 (nothing to double) and is owned outright by the value decoded from it. Cost: the decode call alone is +10% on a small object and +25% on a typed array, but end-to-end in `direct` mode that is invisible -- set 2652ns vs 2667ns, get 1598ns vs 1540ns, with p10/p90 ranges overlapping -- so the tag-scanning optimisation that would have kept the pooled path for values provably free of typed arrays was not worth its correctness risk. `v8codec_test.js` also stopped re-implementing the codec locally and now tests the shipped `TurboKV.V8_CODEC`, per decision 37b: the local copy would have kept passing while the real one was broken. |
| 45 | **The addon is not reachable from the public API** | `TurboKV.native()` (previous behaviour) | That static handed the raw addon to any caller of the public class, putting `poke()` -- which writes 0x42 into the data region to prove the read-only mapping faults -- and `suppressRefBit`, `backwardShift`, `clearHints` and `secondChanceBudget`, which mutate global eviction behaviour, on the same surface as `get` and `set`. Removed; tests require `src/native` directly, which an installed consumer cannot reach because the `exports` map has no deep path (verified: `ERR_PACKAGE_PATH_NOT_EXPORTED`). The five mutating hooks now carry an `__unsafe` prefix so reaching them from inside the package is deliberate. |
| 43b | **Guard cadence is ours, not the runtime's: 500ms debounce** | evaluate on every collection | Collection frequency varies more than 4x across runtimes under identical churn (6.3/s Node, 9.7/s Bun, 28.6/s Deno) and is none of the cache's business. Two costs argue against following it. The sample is not free, and on Bun 1.4.2 it is expensive out of all proportion: `v8.getHeapStatistics()` is ~110ns and constant-time on Node but **O(heap) on Bun -- 3.2ms at 17MB rising to 74ms at 411MB**, roughly twice the cost of a full GC on the same heap while collecting nothing. Reported upstream with a standalone repro (`repro/`) and being fixed (oven-sh/bun#30596, unmerged as of 2026-09-10), so that figure is dated rather than permanent. The second reason does not expire: shedding is not free either, and acting 28 times a second churns L1 far harder than a memory guard needs to on any runtime -- so the debounce stays regardless of what Bun does. Measured effect on Bun: 17.0 evaluations/s uncapped, 2.1/s at 500ms. `heapGuardPace()` exposes evaluations, dropped signals and the interval. The cheap sampler was rejected on evidence: `process.memoryUsage().heapUsed` equals `used_heap_size` exactly on Node and Deno but on Bun tracks something else (flat at 9.4MB while the heap grew), and Bun's `heap_size_limit` is not constant either (318MB -> 644MB), so neither the numerator nor the denominator can be shortcut. |
| 42 | **Dual CommonJS/ESM entry points and hand-written TypeScript declarations** | CJS only; generate types from a TS port of the implementation | Deno treats every `.js` as ESM unless a `package.json` says otherwise, so without one the module simply would not load there -- a packaging gap, not an incompatibility. The root manifest now declares `type: commonjs` and an `exports` map with `types`/`import`/`require` conditions; `index.mjs` reaches the unchanged CJS implementation through `createRequire` and re-exports each name explicitly so bundlers can see through it. Verified as CJS *and* ESM on Node, Bun and Deno, and from a real tarball install that compiles the addon. Declarations are hand-written rather than emitted from a TypeScript port: the JS layer carries seqlock, cursor and recovery-state-machine invariants that four adversarial reviews have just hardened, and a mechanical port risks changing behaviour there for a type surface of about fifteen public methods. `types/types.test.ts` guards them with `@ts-expect-error` on six deliberate misuses -- a declaration file that accepts everything type-checks fine and is worthless, and tsc fails on an UNUSED expect-error, so the file only passes when each of those really is an error. A TS port of the implementation stays open, and the suite is now strong enough to make one verifiable. |
| 40 | **Every cross-process timestamp moves to a monotonic, suspend-counting tick clock** | CLOCK_REALTIME (previous behaviour); CLOCK_MONOTONIC everywhere | The heartbeat and the TTL epoch both used wall clock, so a forward NTP step larger than `primaryStaleMs` marked a healthy primary dead in **every worker simultaneously**, and a backward step shifted every TTL. "Just use CLOCK_MONOTONIC" is not portable in the sense needed: on Linux it EXCLUDES suspend, on macOS it INCLUDES it. Suspend must be counted -- a TTL should still elapse while a laptop sleeps, which is what wall-clock TTL meant -- so each platform gets its explicitly suspend-counting clock: `mach_continuous_time`, `CLOCK_BOOTTIME`, `QueryInterruptTimePrecise` (resolved via `GetProcAddress`, since its import library is not one node-gyp links). Measured 5.1ns/call against CLOCK_REALTIME's 10.8ns, and verified comparable across a fork. L1 expiry moves to `performance.now()` for the same reason: with `Date.now()` a clock step moved L1 and L2 expiry in OPPOSITE directions. `heartbeatAgeMs` also clamped a future-dated stamp to 0, so after a backward step a dead primary looked alive; ticks never go backwards, so a future stamp now reports -1. `TC_LAYOUT` 3. |
| 41 | **A worker that loses its primary detaches, polls, and recovers** | degrade to L1-only permanently (previous behaviour, DESIGN 13.11) | Two facts force the shape. A new primary is a NEW SEGMENT (`create()` unlinks and re-creates), so the mapping a degraded worker holds is an orphan that will never update -- detection means re-opening BY NAME. And on Windows `CreateFileMappingA` fails with `ERROR_ALREADY_EXISTS` while any process holds a handle, so a worker clinging to a dead arena **prevents a new primary from ever starting**: detaching is mandatory, not hygiene, and free because a degraded worker serves L1 only. Recovery requires the heartbeat to ADVANCE across two 1s polls, not merely to look recent -- a dead primary's last stamp stays plausible until `staleMs` elapses, and a fresh arena starts with a fresh one. On recovery the worker flushes L1 (it missed every invalidation), resets its ring cursor to the current head rather than replaying, and re-claims a submission slot. `arenaId` separates "a different primary owns this name" from "the same primary resumed" (SIGSTOP, a long GC, laptop sleep); the actions are identical, so it only feeds stats. State lives on the class because `native` is process-global. |
| 41b | **Primary staleness is checked on a clock, not every 256 drains** | amortise the check over operations (previous behaviour) | Tied to the operation count, a worker doing three reads a second took **85 seconds** to notice a dead primary, and one that went quiet and came back served stale data on its first read. Staleness is a question about time. `performance.now()` is ~21ns against the native `ringHead()` call the drain already makes, so a 500ms-gated check is free at any call rate. Found by the recovery test, not by review. |
| 39 | **Arena geometry is validated on attach; `magic` is published last with a release store** | check magic and layout only (previous behaviour) | `bind()` turned `indexOff`/`indexSlots`/`dataOff`/`dataBytes`/`ringOff`/`ringCap` into pointers and masks without ever validating them, and `create()` wrote `magic` FIRST, so a reader attaching mid-create passed the check while the geometry was still zero -- `indexSlots = 0` gives a probe mask of 2^64-1. I could not reproduce a fault: the window is masked because the hints segment does not exist yet, so the attach is refused for an unrelated reason. That is an accident, not a check, and the recovery poll in the open items would hit the window deliberately once per second. Every field is now checked against the mapping actually obtained, and `magic` publishes last. This is corruption and version-skew robustness, NOT the hostile-input case the submission ring faces: the arena is written only by the primary and the fd is read-only to everyone else. |
| 39b | **BigInt words are stored 8-byte aligned; every decode constructor's status is checked** | sign byte at 0 and words at +1 (previous layout) | `(const uint64_t *)(src + 1)` is a misaligned load -- UB even though x64 and arm64 tolerate it, and the project runs a UBSan gate. `rawLen == 0` would also have asked for 536,870,911 words; unreachable from the encoder, but this is the read path and the rest of it verifies rather than trusts. Words moved to offset 8, `TC_LAYOUT` bumped to 2 so a mismatched build refuses the arena instead of misreading it. Separately, six `napi_create_*` results in the decode path were ignored, leaving `out` uninitialised on failure. |
| 39c | **Rejected on measurement: cache-line padding in `Header` and `SubmitRing`, and single-conversion `readKey`** | pad the hot lines; convert each key once | `tailPub`/`ringHead`/`heartbeatNs` share a line, but the effect is unmeasurable at realistic rates: across five reader/writer configurations the padded and unpadded builds differ by less than the +/-20% run-to-run variance of either. It is also **true** sharing, not false -- every reader loads both `ringHead` and `tailPub` on every get, so co-locating them means one line transfer carries both updates and splitting them can only add a second miss. `SubmitRing` padding shows 25% only when the consumer drains once per push (24-37M drains/s); at 13k drains/s, still ~10x the real per-event-loop rate, it is 9ns either way. Single-conversion `readKey` saves 6-15ns (~1.5% of a get) but the obvious `KEY_MAX+2` buffer is WRONG: truncation never splits a character, so a 1027-byte key reports `got = 1024` and is accepted as a legal 1024-byte key -- silently reintroducing the aliasing class fixed twice already. Not worth 1.5%; the aliasing case is pinned by a test so a future attempt cannot land it quietly. |
| 39d | **Percentiles are merged from raw samples, never averaged across workers** | average each worker's p99 (previous behaviour) | A mean of four workers' p99s is not a p99. With one worker carrying a heavier tail, avg-of-p99 read 8.8us where the true merged p99 was 17.1us -- **48% low**, and low in exactly the case the measurement exists to find. Workers now ship raw samples once, after the measured window. Event-loop delay is a per-process histogram that cannot be merged, so it is reported as the WORST worker rather than an average, and the pass criterion uses that too. |
| 38 | **A namespace quota is honoured under INDEX pressure, not only data pressure** | null second-chance budget in the index-eviction loop (previous behaviour) | A re-append frees no index *slot*, so the index-pressure loop passed a null budget and dropped unconditionally -- which deleted quotas and CLOCK bits entirely whenever the index was the binding constraint. Measured: a cold namespace lost **all 500** of its quota-protected entries while `liveBytes` sat at 0.45MB of 32MB, and `nsProtected` was incremented for entries that were then dropped (protected=500, dropped=500). `autoSize()` gives one slot per 512B, so any workload averaging under ~384B is index-bound in production and never saw the documented policy at all. The loop now gets a bounded budget: re-appending a protected entry lets the scan step past it to find a droppable one, which does free a slot, and once the budget is spent it falls back to unconditional drops so progress is guaranteed. 0/500 survivors becomes 441/500, protected/dropped becomes 66621/59. |
| 38b | **`freeze: true` neutralises Date/Map/Set mutators, since `Object.freeze` cannot** | document the hole | `Object.freeze` does not seal internal slots, so `d.setTime(0)`, `m.set(k,v)` and `s.add(x)` all succeeded on a "frozen" cached value and corrupted L1 until eviction. The entire purpose of the option is to turn silent corruption into a `TypeError` (decision 26), so the mutators are shadowed with throwing own-properties before the freeze. |
| 38c | **Two "tests" that could not fail now assert** | leave them as diagnostics | `typeflow_test.js` and `typematrix_test.js` printed a table and exited 0 regardless, so any silent change in type behaviour passed unnoticed while both were counted as passing. The matrix IS the specification, so the expected cells are now written down -- including the lossy JSON ones, which are correct behaviour rather than bugs (decision 29) -- and a changed cell fails. Verified by flipping one. |
| 37b | **A regression test must exercise the shipped path, not a helper on the way to it** | test the applying function directly (previous practice) | The primary-L1-coherence defect already had a regression test from the FIRST adversarial review. That test called `applyBatch()` directly, so when `set()` started bypassing `applyBatch` for the ring, the test kept passing while the default path served stale data. `transport_regression_test.js` now drives every case through the public API in a real worker process, on both transports, and was verified to fail when each fix is reverted. Same lesson as the fixed-size ring test that missed the wrap remainder: a test that cannot reach the code it names is not coverage. |
| 36d | **Cache write traffic on the cluster channel degrades the APPLICATION's own IPC by 19.7x at p99** | assume the channel is turbokv's to use | `process.send` is not a private pipe: it is the one cluster channel the application also uses for its own worker/primary messaging, and any other library shares it too. Measured with an app-level ping/pong and a control phase doing byte-identical `set()` work whose outbox never reaches the channel: not sending, p50 0.25ms / p99 0.69ms; sending, p50 0.75ms / p99 **13.59ms**. So the cost falls on unrelated code, which no throughput argument about the cache itself can weigh against. An earlier run of this experiment reported 'no effect' -- its control passed `maxInFlightBytes: 0`, which `|| (8 << 20)` turned into the default, so both phases were the same configuration. The bug that produced the wrong answer is fixed (`??`, not `||`) and is itself pinned by decision 37. |
| 36 | **Worker writes travel through per-worker SPSC shared-memory rings, not `process.send`** | keep cluster IPC; one shared MPSC ring; a native drain thread | `process.send` was never bandwidth-limited -- the raw channel carries 439 MB/s (JSON) and 1738 MB/s ('advanced'). Its real cost is that each send **synchronously freezes the sending worker's event loop** while V8 serializes the batch: 0.49ms p50, 1.15ms p99 for ~525KB. That stall lands on the channel the application shares for its own messages, so it is an externality on unrelated code, not just a cache cost. Measured worker->L2 with an identical workload: IPC 163k writes/s with **51.7% shed** and loop p99 9.68ms; rings 364k writes/s with **0% shed** and loop p99 5.94ms. The zero-shed result matters more than the 2.2x: every offered write reached L2 instead of half being dropped. One ring per worker rather than one shared ring -- no atomic contention between producers, no head-of-line blocking when a producer is preempted between reserving and publishing, and a dead worker's ring is individually skippable. Positions are monotonic byte counts, the same idiom the index uses (decision 19). |
| 36b | **Ring slots are CLAIMED by CAS, not passed in by the caller** | caller supplies a worker id (previous behaviour) | `attachWorker(arena, 0)` used to wedge the process permanently: `#id === 0` is how every method recognises the primary, so an id-0 worker took the primary's write path against a read-only mapping, blocking the event loop on its first `set()` with no throw, no crash and no log. Zero-based ids are the natural thing to write. It now throws, and with ring slots assigned by `compare_exchange` on the ring's `owner` field the collision becomes structurally impossible rather than merely rejected. |
| 36c | **Ring contents are treated as untrusted input** | trust the worker | Workers map the submission segment read-write (the arena itself stays read-only, which is the isolation that matters). Every field a consumer reads is bounds-checked before use, and a record that fails validation stops that one ring rather than the drain -- a bad worker loses only its own writes. A wrap that left 8 or 16 bytes (records are 8-aligned, the header is 24) was too small to hold even a SKIP header; writing one there overran the ring, produced a record the consumer rejected, and wedged that ring permanently after 26k records. The gap is now IMPLICIT -- both sides derive it from the offset with the same rule, so nothing describes it. Fixed-size test records never produced that remainder, which is why the first test suite passed while the cross-process run failed; the suite now cycles key length to walk every alignment. |
| 35 | **The L2 write ceiling is the cluster IPC transport, not the arena** | assume the single-writer primary is the limit | Measured separately: the primary applies **986k writes/s** straight into the arena, while one worker delivers **60k writes/s** through `process.send` -- 16x less. Four workers delivered 242k/s in the load test, matching 4 x 60k, so the channel is per-worker and scales with worker count. The 65.8% shed rate is therefore a transport limit; raising it means replacing cluster IPC with a shared-memory submission ring, not touching the single-writer design. Also note the path is asynchronous by construction: `process.send` queues into libuv and the drain callback needs the event loop, so a fully synchronous burst of 400k sets delivered only 4486 of them (97.9% shed) regardless of primary speed. |
| 34b | **L1 second-chance reprieves are budgeted (16), matching the arena's** | unbounded reprieves (previous behaviour) | A re-queue frees no bytes, so it does not advance the eviction loop's condition: with every resident re-read, one insert walks the entire map before it can evict anything. The native arena already bounded its equivalent loop (`g_secondChanceBudget`); the JS L1 did not. Not the cause of the O(n) above -- bounding it alone changed 8002ns to 8002ns -- but a real unbounded worst case either way. |
| 34 | **L1 eviction pops the oldest entry through a retained Map cursor, not a fresh `entries().next()`** | fresh iterator per eviction (previous behaviour); a separate linked list or ring for FIFO order | V8's `OrderedHashMap` tombstones deletions and only compacts on rehash, so a fresh iterator re-scans the whole accumulated hole prefix on every call. Using a `Map` as a FIFO queue that way is **O(n) per eviction**. Isolated in pure JS with no cache involved, popping the oldest key from a steady-size Map: 1671ns at 8k entries / 6173ns at 32k / 20018ns at 128k with a fresh iterator, against 89/94/115ns retained -- 174x at 128k. In the cache this made cold reads get *slower* as L1 grew (2832ns at 2MB, 8006ns at 8MB, 23664ns at 32MB) and snap back to 395ns at 128MB where the set fits and eviction never runs. Fixed: 1176/1149/959ns, flat in L1 size, 24x at 32MB. A retained iterator is safe because Map iterators are live -- tail appends are still visited, deletions ahead are skipped. Pinned by `perf_regression_test.js` as a *ratio* between two evicting configurations (0.93x fixed, 5.07x reverted). |
| 33 | **Worker IPC respects `process.send()` backpressure and sheds at the outbox cap** | unbounded queueing (previous behaviour); block the worker until drain | The libuv send queue lives outside the JS heap and is unbounded: under sustained write load a worker's RSS climbed 425MB to 485MB while its JS heap stayed flat. Honouring the `false` return and waiting for the drain callback plateaus RSS at ~250MB and raises throughput 4.2x, because the process stops paying to grow a queue the primary can never drain. Under saturation this sheds ~65-78% of offered L2 writes -- the primary's single-writer apply ceiling is real and now visible as `stats.writesShed` rather than as unbounded memory. |
| 32 | **Shared-memory pages are reserved with `posix_fallocate` at create time (Linux)** | `ftruncate` alone (previous behaviour) | `ftruncate` sizes a tmpfs object *sparsely*: it succeeds even when the filesystem cannot supply the pages, and the process takes a `SIGBUS` the first time it touches one. Docker's `/dev/shm` defaults to 64MB, so a 192MB arena was created successfully and killed the load test minutes later, exit 135, with no diagnosable cause. `fallocate` reserves for real, turning the shortfall into a clean `create()` failure that names `--shm-size`. Costs a one-time ~125ms/GB at create (32MB 5.0ms, 192MB 24.6ms, 512MB 63.3ms, 1GB 301ms) and no process RSS -- tmpfs blocks are reserved, not mapped. |
| 31 | **Modes are chosen by read/write ratio and type needs, not by a global default** | pick one mode for everyone | Measured across four dimensions: `direct` is lossless (18/18 types exact) and 2.8x faster than `primitives` when reads dominate, but 3.8x slower when writes do; `safe` silently converts 8 of 18 types but gives fresh mutable results and cheap writes; `primitives` never converts, refusing instead. All three are consistent L1-to-L2 and cross-process. |
| 30 | **Three named storage modes: `bytes`, `direct`, `safe`** | one mode with codec/freeze/isolate knobs | The knobs are still there, but the presets name the tradeoff being accepted. Measured, the ranking inverts by workload: `direct` 585k vs `safe` 433k when L1 hits dominate, and 175k vs 307k when writes and misses do, so neither is a default for everyone. `direct` carries one hole JS cannot close: typed-array contents cannot be frozen. |
| 29 | **BigInt accepted in primitives mode; rich types require the `v8.serialize` codec** | BigInt via text; JSON codec for everything | BigInt is a primitive and immutable, so excluding it was inconsistent; stored as sign byte plus 64-bit words for arbitrary precision. Date/Map/Set/TypedArray are rejected loudly by primitives mode and corrupted *silently* by JSON (Date to string, Map/Set to `{}`), so fidelity workloads must use the v8 codec. |
| 28 | **Value type is tagged in the entry, not inferred** | strings only; encode everything to text | L2 is read by other processes, so the type must travel with the bytes. Before this, numbers and booleans never reached the arena - they lived in L1 only, vanished on eviction and were invisible to other workers, while `get` looked correct until then. `null` threw outright. Doubles are stored as raw bytes: exact for `-0`, `NaN`, `Infinity` and subnormals, and no parsing. |
| 27 | **`structuredClone` rejected for per-read isolation; V8 serialization offered as a fidelity codec** | clone per read; JSON everywhere | Cloning per read is 2.0-2.4x slower than parsing the equivalent string and 1000x more than freezing, and Node 26 widens the gap. But JSON silently degrades eight common types (Date to string, Map/Set/RegExp to `{}`, NaN/Infinity to null, undefined dropped) and throws on cycles and BigInt, so a `v8.serialize` codec is offered for callers who need fidelity, at roughly 2-3x JSON's cost. |
| 26 | **Codec mode isolates on set and freezes by default** | adopt the caller's object; document a do-not-mutate contract; deep-copy on every get | `set` adopting the caller's object let a caller corrupt L1 without calling `get`, and the value then silently reverted when L1 evicted. Isolation costs one decode per set; freezing costs ~24% and turns a silent corruption into a `TypeError`. Freezing delivers the same guarantee as parse-per-read at roughly twice the throughput (813k vs 413k), differing in ergonomics: shared-immutable rather than fresh-mutable. |
| 25 | **A JSON replacer/space/reviver is never permitted; enforced, not documented** | rely on code review; document the rule only | A replacer costs 3.51x on Node 26 and indentation 2.11x, and an identity replacer produces byte-identical output so no output check can catch it. Enforced by a repo-wide balanced-paren lint plus a construction-time check on the caller-supplied codec, with `allowSlowCodec: true` as the deliberate escape hatch. |
| 24 | **ASCII values stored and returned as one-byte strings; non-ASCII as UTF-8** | latin1 for everything (previous behaviour) | Fixes silent mangling of non-ASCII, and keeps ASCII on the representation Node 26's 34%-faster stringify fast path favours. Node 26 penalises all-non-ASCII payloads 2.03x, worse in absolute terms than Node 24. |
| 23 | **`values: 'bytes'` is the default mode; codec is opt-in** | codec everywhere; JSON-always like bugsee; accept objects natively | Primitives make accounting exact (verified within 1% against measured heap), remove the aliasing hazard entirely, and need no codec. Costs ~20% for flattening plus exact sizing, and pushes object workloads onto a decode-per-hit path. Requires flattening on insert: a cached 1MB substring otherwise retains an 8MB parent. |
| 22 | **No per-object size measurement; a post-GC heap guard instead** | native structural size walk; `v8.serialize().length`; sampling `used_heap_size` directly | V8 exposes no per-object size outside a heap snapshot. A native walk was built and is -14% to -26% accurate against +/-7% for `encodedBytes x 3`, at 5825ns versus free. Bounding live heap after a GC bounds the thing that actually matters: retained heap 445MB to 121MB where the byte budget bound nothing. |
| 21 | **Optional caller-supplied codec; L1 caches decoded values, L2 stores bytes** | app owns the codec (L1 caches encoded strings); built-in JSON mode; accept the limitation | Resolves the decision 4/5 conflict by applying each at its own boundary. 3.6x on L1-resident object workloads, p50 1334ns to 42ns. Costs a documented aliasing contract (or 25-30% for `freeze: true`) and turns the L1 byte cap into a `heapFactor`-scaled estimate, measured at 2.79-3.21x for JSON-shaped objects. |
| 20 | **Arena sizing and `indexSlots` validated at create time** | trust the caller | A too-small segment underflowed into a hang; a non-power-of-two slot count breaks the probe mask |
| 19 | **Index slots hold the monotonic log position, not a physical offset** | physical offsets + seqlock alone | A seqlock cannot detect log reuse: once the head wraps over an evicted entry, its `seq` field is another record's payload. A monotonic position lets a reader prove liveness with `tailPub <= pos`. Closes a silent stale/torn-read bug. |
| 18 | **Reference bits live in a separate hints segment**, workers map it `O_RDWR` while the arena fd stays `O_RDONLY` | reference bit inside the entry | Workers do the reads but cannot write the arena, so in-entry bits were never set and second chance never fired. Isolation is preserved at descriptor level; a bad worker can only degrade eviction quality. |
| 17b | **Backward-shift deletion + 75% index load ceiling** | tombstones | Tombstones were never reclaimed, degrading probes to full-table scans at 3% load. Probe cost is now flat over 1M inserts. |
| 16 | **L2 data region is a circular log with second-chance re-append (`LOG2`)** | size-class slabs + CLOCK; plain FIFO log | Measured best hit rate in both realistic scenarios (+3.3 and +8.1 points over slab); zero external fragmentation; no slab calcification and no per-class rebalancing to build. Slab wins only a contrived size-oscillation case, and wins it *because* of calcification. Both losing allocators were removed in decision 49. |

---

## 13. What is still open

### Decisions that need a call

1. ~~**Atomic read-modify-write.**~~ **Removed** — `incr` and `cas` are gone from
   the public API; see decision 62. Neither can be atomic across L1, L2, L3 and
   many boxes, and a caller who needs an atomic counter already has a Valkey
   client. ~~Remaining wart: `incr`'s return type differs by process role, which
   only an async request/response path would fix.~~
2. ~~**Namespaces share one eviction budget.**~~ **Resolved by removal** —
   namespaces are gone; see decision 63. The prefix was something callers could
   do themselves, and the quota — the only capability they could not reproduce
   — is L2-only and cannot exist in L3, so the tension this item tracked no
   longer has an object to apply to.
3. ~~**Ring capacity and second-chance budget are arbitrary.**~~ **Measured and
   set** — see §9. The ring is now derived from arena size as a ~100ms time
   budget; the second-chance budget is 16, where hit rate saturates at 8.

### Known limitations, accepted and documented

4. ~~**Capacity is quantised to powers of two.**~~ **Fixed** — the log computes
   offsets with a modulo instead of a mask, so the data region takes whatever the
   metadata leaves (95–97% of the arena) rather than being rounded down to the
   previous power of two. A 26MB arena went from 16MB of data to 24.7MB. The cost
   was measured twice: a dependent-chain microbenchmark puts it at +2.96ns per
   computation, but an end-to-end A/B at an equal working set shows no regression
   (519.5ns modulo vs 536.0ns mask per L2 read). An earlier A/B suggesting +6.2%
   was discarded — the two builds had different data-region sizes and so
   different eviction rates, which is a difference in working set rather than in
   addressing.
5. **`direct` mode cannot protect typed-array contents** — JS offers no way to
   freeze them. Documented, asserted in `storage_modes_test.js`.
6. **The L1 byte budget is an estimate whenever a codec is in use**
   (`heapFactor`, measured at 2.79–3.21x but shape-dependent). The post-GC heap
   guard is the backstop, and it needs the event loop to turn.
7. **`safe` mode silently converts 8 of 18 types.** Inherent to JSON, not a bug,
   but it is the reason the mode is not the default.

### Unbuilt

8. ~~**Binary values.**~~ **Done** — `Buffer`, `TypedArray`, `ArrayBuffer` and
   `DataView` are stored as raw bytes under `FLAG_BINARY` and returned as a
   copied `Buffer`.
9. **The L3 (Valkey) seam.** **The core is built; the Valkey provider is not.**
   Built: the `L3Adapter` contract (`get`/`set`/`delete`/`clear` required,
   `has`/`subscribe`/`close` optional), validated once at construction
   (decision 65); the per-process `L3Queue` with per-key ordering, byte-bounded
   shedding that never sheds a `clear`, and bounded retries (decisions 70, 71);
   local-first `set`/`delete` with the async variants differing only in what
   they await (decision 68); the read path's promotion guard, which refuses to
   let a stale L3 read resurrect a value into L2 (decision 69); and
   `close()`/`drainL3()` shutdown (decision 71). Not built, and still exactly
   what an adapter author or an operator has to supply or accept: the Valkey
   adapter itself (a concrete implementation of `L3Adapter` against a real
   Valkey client — every test in this plan runs against `test/l3_fake.js`, not
   a network); cross-machine invalidation (the contract's `subscribe` is
   optional and unimplemented by anything in this repo, so staleness across
   machines relies on TTL alone until an adapter supplies it); and cluster
   support (every behaviour here is proven per-process — L3 has no notion of
   the shared-memory arena or the submission rings that make one machine's
   workers agree with each other, only of what one process asks it to
   store). This is the reason a worker's `incr` still cannot return a value:
   there is no request/response path over IPC, and L3 does not add one.
10. ~~**TTL sweeping.**~~ **Done** — the primary sweeps a slice of the index on a
    timer, sized so a full pass completes in a bounded time (a fixed slice
    covered only ~3% of a 1M-slot index per second). Reclaimed entries append to
    the invalidation ring so workers drop their L1 copies.
11. ~~**Primary-crash detection and recovery.**~~ **Done** — the primary stamps
    `heartbeatNs` on each maintenance tick, on the monotonic tick clock
    (decision 40). A worker whose staleness check fires detaches — mandatory,
    because on Windows a held handle prevents a new primary from creating the
    segment at all — keeps serving its warm L1, and polls the name once a
    second. It recovers only when the heartbeat *advances* across two polls,
    then flushes L1, resets its ring cursor and re-claims a submission slot
    (decision 41). Covered by `recovery_test.js`: kill/restart, SIGSTOP/SIGCONT,
    and no flapping against a healthy primary; verified to fail with the fix
    reverted. Honest scope: under `cluster` a worker dies with its primary, so
    the "a new primary appeared" half pays off for supervisor-managed
    deployments, while the "the same primary was stalled" half — a long GC, a
    paused container, a laptop sleep — applies everywhere and used to be a
    permanent outage.
12. ~~**Windows.**~~ **Done** — every OS call is behind `platform.h`, using
    `CreateFileMapping`/`MapViewOfFile`, `QueryInterruptTimePrecise` for the tick
    clock and `OpenProcess` for ring-owner liveness. `windows-latest` is in the
    CI matrix and green: the arena, both transports, the submission rings,
    geometry validation and recovery all build and pass there. Note Windows
    mappings are reference-counted, so `shmUnlink` is a no-op and a worker
    holding a handle blocks a new primary from creating the segment — which is
    why a degraded worker must detach (decision 41).

### Operational, still open

16. ~~**Quotas are not validated against arena capacity.**~~ **Resolved by
    removal** — namespaces, and the quotas they carried, are gone; see decision
    63. The fixed 16-entry namespace table went with them.
17. ~~**No dead-code removal.**~~ Done: the test-only hooks are off the public
    surface (decision 45), the background-compaction machinery is deleted
    (decision 47), the native size estimator went with it, and `SLAB`/`LOG` are
    removed so `LOG2` is the only arena mode (decision 49).
19. ~~**The addon segfaults on load under Node 18 and 20.**~~ **Fixed** —
    decision 53. `#define NAPI_VERSION 10` on line 1 of `binding.cc`, present
    since the first prototype commit with no comment and nothing depending on
    it. Node 18 and 20 cap at NAPI 9, so they refused the module; Node's refusal
    path is a null-pointer dereference rather than a thrown error, so the
    symptom was a bare SIGSEGV inside `require()` with no diagnostic. Now 8,
    matching the `engines` floor, and the CI matrix covers 18/20/22/24.

20. ~~**The test suite needs more than 2GB of `/dev/shm`.**~~ **Fixed** —
    decision 58. Two separate leaks: `create()` stranded the arena it replaced
    (the same guard `attachReadOnly` and `Submit::open` already carried), and
    every test process left its final segments linked. The suite now runs in
    256MB, and what remains is a real requirement rather than a leak —
    `perf_regression_test` builds a 128MB arena on purpose.
21. ~~**The primary served stale L1 across instances in the same process.**~~
    **Fixed** — decision 64. This was live in the published code path, not a
    theoretical gap: `set()` and `delete()` on the primary skip the ring
    record for their own write (`writerId === 0`), and `clearAll()` had the
    same gap in a neighbouring form — its primary branch cleared the caller's
    own L1 and the shared arena but never touched any other in-process
    instance's L1 — so a second `TurboKV` instance opened in the same process
    never saw the invalidation and kept serving the stale value, including a
    deleted one, until eviction. Pinned by `instances_test.js`, verified to
    fail with the fix reverted.

18. **`bytes` mode copies every string value on `set`.** `flatten` is
    unconditional because Node-API cannot tell a flat string from a slice, and a
    slice of any size can retain an arbitrarily large parent. That is the right
    default for correctness, but it is an allocation per write.

### Before anyone else could use this

13. ~~**TSAN has never been run.**~~ **Run** — see §9. It found and fixed a real
    race (non-atomic reference bits) and confirmed the deliberate seqlock
    payload race, which remains UB by the standard. `run_tsan.sh` is the gate.
    Still outstanding: TSAN cannot cover the cross-process case at all, so the
    multi-process evidence remains empirical.
14. **Packaging is partly done.** `package.json` with dual CJS/ESM `exports`,
    hand-written `index.d.ts` guarded by `types/types.test.ts`, README, LICENSE
    and CI across ubuntu/macos/windows all exist, and a tarball install compiles
    the addon and works on Node, Bun and Deno. ESM TypeScript consumers are
    covered too as of decision 51, and prebuilds plus a publish workflow as of
    decision 52 -- a normal install now needs no compiler on the six platforms
    covered, and falls back to a source build elsewhere. Still unmoved:
    the sources live under `src/`, which the `exports` map hides from
    consumers but which is the wrong name for shipped code.
15. ~~**Two vendored-in-name-only dependencies.**~~ **Done.** LZ4 is now an
    optional build feature and the default build links nothing external;
    `src/vendor/rapidhash.h` is the upstream header verbatim (rapidhash V3,
    MIT, commit recorded in `vendor/README.md`) rather than a transcription.
    A checkout now builds with only a compiler and Node — verified by building
    from a clean export of the tree.

## 14. Assumptions

| Area | Assumption |
|---|---|
| L1 hit | ~21ns (measured, `Map` lookup) |
| L2 hit | **265ns @64B, 445ns @1KB** on a 200k-key random workload; 42–80ns only for a small in-order working set |
| `set` | 45ns @64B, 86ns @1KB, 620ns @16KB (**measured**, uncompressed) |
| Cross-worker write visibility | ~1 event-loop tick |
| Entry count | up to ~100k in L2 at typical value sizes |
| Worker count | up to 32 |
| Max key size | **1024 bytes** UTF-8; longer keys are rejected |
| Max value size | `min(dataBytes/2 - overhead, 4MB scratch)`, reported by `maxValueBytes()` |
| Durability | none — cache only; the arena dies with the primary |
| Security | uid-scoped, mode 0600; **any same-user process can read all cached data** |
| Maintenance | solo maintainer; one prebuild per platform, stable across Node majors |
| L3 shutdown | `close()` waits up to `l3CloseTimeoutMs` (default 5000ms) for the L3 queue to drain, then up to the same bound again for the adapter's own `close()` -- worst case ~2x `l3CloseTimeoutMs` -- and proceeds regardless; a `clear` outstanding when L3 is unreachable keeps retrying in the background past that point, unobserved (decision 71) |
