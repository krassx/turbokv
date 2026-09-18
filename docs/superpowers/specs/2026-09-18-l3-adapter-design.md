# L3: a remote tier behind L1 and L2

**Status:** design approved in conversation, pending written review
**Date:** 2026-09-18

## 1. Goal

turbokv has two tiers: **L1**, a per-process map, and **L2**, a shared-memory
arena the primary owns and workers map read-only. This adds **L3**, a remote tier
reached through an adapter the user supplies, with a built-in provider for
Valkey and Redis.

Two things follow from the decision to build it:

- **The tiers become one cache.** With an adapter attached, `clear()` empties
  every tier, and a value written through any method is visible through any
  other. turbokv does not treat some keys in L3 as "ours" and others as someone
  else's. The Valkey database belongs to the cache.
- **The synchronous and asynchronous forms of a method have identical
  effects.** The only difference is what the caller can wait for. A sync method
  returns what this process knows now and completes the L3 part in the
  background; an async method resolves when L3 has accepted the operation.

## 2. Scope

### Phase 0 — simplify, then fix

Prerequisites. None of this depends on L3; all of it makes L3 smaller.

1. **Remove `incr` and `cas`.** Neither can be made atomic across L1, L2, L3
   and many boxes without a global ordering this design does not have. A caller
   who needs an atomic counter has a Valkey client and can use `INCR` directly.
   Removes: the public methods, the `'i'` IPC operation, native `incr`/`cas`,
   their tests and type declarations. Reverses design-doc decision 1.
2. **Remove namespaces.** A namespace is a key prefix plus a one-byte tag used
   to charge a byte quota. The prefix is something callers can do themselves;
   the quota is the only capability they cannot reproduce, and it cannot exist
   in L3, because Valkey evicts across the whole database. With L3 attached,
   losing an entry from L2 costs a round trip rather than the data, which is a
   much weaker reason to keep the machinery. Removes: the `namespace` option,
   `clearNamespace()`, `namespaceStats()`, the `'n'` IPC operation, the arena
   header's namespace table, the namespace id in submission-ring records, the
   quota path through eviction, and `NamespaceOptions`/`NamespaceStat` from the
   declarations. Closes open item 16, the 15-namespace limit, and the overlap
   bug in §11.1. CLOCK second chance stays.
3. **Fix the stale L1 read across instances** (§11.2).

### Phase 1 — L3

Everything else in this document.

### Not in scope

- `incr`/`cas` equivalents in any form.
- `keys()` and `size` continue to describe **local** tiers only (§10.3).
- Refresh-ahead: on an invalidation, re-fetching the value before it is asked
  for (§6.4).
- An automatic `l1MaxValueBytes` policy. `minLevel` already covers the measured
  case; this remains an open item in the design doc.

## 3. Topology

Every process holds its own L3 connection.

```
box 1                                     box 2
 ┌─ primary ── L1 ─ L2(rw) ── client ─┐    ┌─ primary ── L1 ─ L2(rw) ── client ─┐
 ├─ worker  ── L1 ─ L2(ro) ── client ─┤    ├─ worker  ── L1 ─ L2(ro) ── client ─┤
 └─ worker  ── L1 ─ L2(ro) ── client ─┘    └─ worker  ── L1 ─ L2(ro) ── client ─┘
                                 │                                        │
                                 └──────────── Valkey ────────────────────┘
```

Writes originate in the process that called, because only that process can
await the result and act on a failure. Routing them through the primary would
make every worker write unawaitable: a worker's `set` returns as soon as the
bytes are in the submission ring, long before the primary drains it.

The primary keeps one additional connection per node, used only to receive
invalidations (§6).

**A box that loses its primary keeps L3.** L3 does not depend on the arena, so a
degraded worker runs with L1 and L3 and no L2.

## 4. The adapter contract

```js
{
  // Required
  async get(key, { willCache })                    // -> { value, ttlMs? } | undefined
  async set(key, value, { ttlMs, originId, willCache })   // -> void; throw to fail
  async delete(key, { originId })                  // -> void; throw to fail
  async clear()                                    // -> void; throw to fail

  // Optional
  async has(key)                                   // -> boolean; falls back to get()
  async subscribe(onRemoteChange, onResync)        // -> unsubscribe fn
  async close()
}
```

- **Errors throw.** turbokv catches them and applies the failure policy in §9.
- **`value` is the codec-encoded form** — what would go into the arena, minus
  LZ4, since compression is an arena-space optimisation rather than a wire
  format. A `string` normally, a `Buffer` for binary values. The consequence is
  user-visible and intended: with `JSON_CODEC` the Valkey holds readable JSON
  that other services can consume; with `V8_CODEC` it holds an opaque latin1
  blob. **The codec decides whether L3 is interoperable.**
- **`willCache`** says whether turbokv will keep this value in a local tier. The
  tracking provider maps it to opt-in tracking; the Streams provider ignores it.
  turbokv computes it from `minLevel` and from whether the node's invalidation
  connection is live.
- **`ttlMs` returned from `get` is optional.** Without it, a key that expires in
  Valkey in 2s but is cached locally for 30s is served 28s too long. Adapters
  that can report the remaining TTL cheaply do, and turbokv then caches the
  value for that long. In Streams mode the local lifetime is additionally capped
  at `l3TtlMs` (§6.3); in tracking mode no cap is needed, because expiry itself
  produces an invalidation (§12.4).
- **`originId`** is `native.arenaId()`: stable per arena, identical in the
  primary and every worker mapping it, and changed by a primary restart, which
  is correct because a restarted primary has an empty cache. It is the
  cross-box counterpart of the `writerId` already in the invalidation ring.
  Tracking does not need it; the Streams fallback does.
- **`has` is optional** because `EXISTS` is trivial for Valkey but not universal.
  Without it turbokv falls back to `get` with `willCache: false`, which is
  correct and transfers the value needlessly.
- **`subscribe` is called only by the primary.** An adapter without it still
  works; that deployment relies on `l3TtlMs` for cross-box staleness.
- **`onResync` means "invalidations were lost", not "I reconnected".** A
  provider that can replay (Streams) calls it only when entries it never read
  were trimmed. A provider that cannot (tracking) calls it on every
  disconnection, because it genuinely cannot tell.

## 5. Read and write paths

### 5.1 Writes fan out; reads chain

```
get:  L1 ──▶ L2 ──▶ L3           a hit fills the tiers below it
set:  L1 ──▶ L2
      └────▶ L3                  from the calling process, not via L2
```

L2 is passive shared memory. Nothing propagates out of it; some process must
act. Hanging L3 off the primary's drain of the submission ring would make every
worker write unawaitable, and the drain applies records inside C++ and returns
only a count, so the keys never reach JS.

### 5.2 `set`

Local first. The local write lands at full speed, so other workers on the box
see it after one drain rather than after a network round trip, and a remote
outage leaves the local cache working.

```js
const ok = this.set(key, value, opts);        // L1 + ring, unchanged, ~45ns
if (!ok) return false;                        // rejected locally, never sent
await l3Queue.set(key, enc, { ttlMs, originId, willCache });
```

L3-first was considered and rejected: when Valkey is unreachable it writes
nothing at all, not even locally, which turns a remote outage into a local
write outage.

With `minLevel: L3` nothing is written locally, so a failed L3 write means the
value was not stored anywhere. That is what the caller asked for.

### 5.3 `get`

```js
#drain()                        // invalidation ring, unchanged
L1 hit?  -> return
L2 hit?  -> fill L1 per minLevel, return
L3       -> adapter.get(key)
            hit  -> fill L2 and L1 per minLevel, return
            miss -> undefined
```

- **Simultaneous misses on one key inside a process share a single L3 request.**
  Across processes they do not, so the herd is bounded by the number of
  processes rather than by the request rate.
- **A worker fills L2 through the submission ring**, like any write, so a full
  ring sheds the promotion. The failure mode is "no promotion", which is benign.
- **Promotion is guarded** (§5.5).
- **Sync `get` fetches in the background on a miss** and fills local tiers, so
  the next `get` hits. A cache-aside caller that sees `undefined`, recomputes
  and calls `set` overwrites L3 with an equally valid value; the background
  fetch cannot then promote the older value over that `set`, because the guard
  sees the `set`'s ring record.

### 5.4 `delete`

Local delete, then `DEL` in L3. If L3 is unreachable, reads cannot refetch
either, so the delete holds for the whole outage. The only case where the old
value returns is reads succeeding while the `DEL` failed; the caller resolved
`false` and knows. No local tombstone: L3 is the store of record.

### 5.5 The promotion guard

A read that started before a write can return the older value after it, and
promoting that value overwrites the newer one in L2 — not stale, wrong, and
persistent until the next write.

```js
const mark = native.ringHead();          // before the await
const v = await adapter.get(key);
const r = native.ringRead(mark, N);
if (r.wrapped || matches(r, key)) return v;   // return it, do not promote
fill(v);
return v;
```

Checking for this key's hash rather than "did the head move" matters: under load
the head always moves, so the conservative version would never promote and L3
hits would never reach L2. `wrapped` falls back to conservative.

**Required change:** the guard must treat the ring's flush marker as matching
every key, not just compare hashes. Without it, a clear or a resync that happens
during a read still lets the read promote a value the flush was meant to remove.

### 5.6 The L3 queue

Per process, ordered per key, shared by reads that need ordering, writes,
deletes and clears.

- **Order per key is required.** `set(k, A); set(k, B)` must leave `B` in L3.
  Sent over a connection pool, the two could arrive in either order, leaving L3
  with `A` while the box holds `B`.
- **Pending writes to one key merge.** If `A` has not been sent when `B`
  arrives, only `B` goes out. For a hot key this saves both bytes and round
  trips. A merged `setAsync(A)` resolves with `B`'s outcome, so its promise
  means "L3 holds your value, or a later one from this process".
- **Bounded by `l3QueueMaxBytes`.** Past the bound, operations are shed under
  the §9 failure policy. This mirrors `writesShed` when the submission ring is
  full.
- **Retries are bounded, and the promise resolves once, when the operation is
  settled.** A transient failure is retried with backoff for up to
  `l3RetryMs`; the async form resolves `true` if a retry succeeds inside that
  budget and `false` when the operation is abandoned — budget exhausted, shed,
  or a non-retryable error such as a permission denial. So a promise never
  resolves `false` for an operation that later succeeds on its own. `clear` is
  the exception: it keeps retrying past the budget, because until it lands this
  process must serve misses (§10.2), and flushing twice is harmless.
- **Background outcomes never touch `lastError`.** The sync call has already
  returned, so a late failure would appear to a caller as the reason for an
  unrelated operation. They go to `stats` and to an optional error listener.

## 6. Coherence

### 6.1 Mode selection

The primary's provider tries `CLIENT TRACKING` when it connects. If the command
is refused, it uses Streams. **The server decides**, so every box talking to it
picks the same mode — a requirement, since a tracking box never writes to the
stream and a Streams box beside it would never hear its writes. An explicit
`mode` option exists for tests.

Floors: tracking needs Redis/Valkey ≥ 6.0; Streams needs ≥ 7.0, for
`XINFO STREAM`'s `max-deleted-entry-id`. The provider checks at connect and
refuses rather than degrading silently.

### 6.2 Tracking (default)

The server records which keys each connection read and invalidates only those
connections. That is the residency filter, built in, and it is why a custom
Valkey module is not needed — a module would also only run on self-hosted
servers, and would put our C code in the user's database process.

- **The primary owns the invalidation connections**, one per node, and turbokv
  supplies the handler. Users implement only the transport.
- **A remote invalidation is a local delete**, so no new native code:

  ```js
  native.del(key, 0);         // drops from L2, appends to the invalidation ring
  TurboKV.#localDrop(key);    // the primary's own L1: ring records with
                              // writerId 0 are skipped as "our own write"
  ```

  The second line is load-bearing. Without it a remote invalidation updates
  every worker's L1 but not the primary's. `applyBatch` already pairs
  `native.set` with `#localDrop` for the same reason.
- **Subscribed by default** where the adapter supports it, with
  `l3Subscribe: false` to opt out for a box that is the only writer to its keys.
  Correctness should not be something users remember to enable.
- **`onRemoteChange` is an observer, not a replacement.** An optional listener
  runs after the core invalidation and cannot suppress it; a user handler that
  skipped invalidations for some keys would reintroduce silent stale reads.
- **Writes register tracking.** Tracking starts on a *read*, so a value this box
  wrote but never read would not be invalidated when another box changes it.
  The provider therefore writes with `MULTI; SET … PX; <tracked read>; EXEC` —
  one round trip, one key, so it is cluster-safe.
- **Opt-in mode** keeps reads that will not fill a local tier from creating
  server-side tracking entries: `minLevel: L3` reads, and reads taken while a
  node's invalidation connection is down.

Accepted costs, all measured in §12:

| Cost | Effect |
|---|---|
| A sibling connection's write invalidates the key for the rest of the box | one extra miss |
| One invalidation per connection that read the key | usually one, because L2 satisfies the other workers |
| Reads that miss create tracking entries | server memory for keys turbokv never caches; opt-in mode removes it for scans |
| The tracking table is capped (1,000,000 keys by default) | the server invalidates older keys to make room: extra misses, never stale data |

### 6.3 Streams (fallback)

Only for servers that refuse `CLIENT TRACKING` — ElastiCache Serverless is the
known case, and it presents a single virtual shard, so the stream sits on one
shard and there is no hotspot to shard around.

- The stream is the invalidation ring one level up: a bounded log, a cursor per
  reader, and a way to detect falling behind.
- **Plain `XREAD`, not consumer groups.** Groups divide entries among consumers;
  every box must see every entry.
- **Ordering, not atomicity:** `SET` then `XADD`. Publishing before the value is
  visible is the order that sticks — another box drops the key, reads L3 before
  the `SET` lands, promotes the old value, and never hears again. Atomicity via
  `MULTI` is unavailable anyway where it matters, since a data key and the
  stream key are in different hash slots.
- **Retention is a time window** (`XADD … MINID`), trimmed on a timer rather
  than on every call, which saves ~40B per write.
- **Overrun detection is exact:** `XINFO STREAM`'s `max-deleted-entry-id`
  greater than our cursor means entries we never read were trimmed. `XREAD`
  itself gives no gap signal.
- **The cursor lives in memory.** A restarted primary has an empty cache, so it
  starts from `$`.
- **Replay may redeliver, which is harmless**, because invalidation is
  idempotent.
- **`l3TtlMs` stays** in this mode as the backstop for writes that bypass
  turbokv, which the stream never hears about.
- **`clear()` deletes the stream too.** The provider adds a clear entry after
  flushing; stream ids are time-based, so it sorts after every reader's cursor.

### 6.4 Invalidate, never push values

The message carries the key, not the new value.

- Publish order is not a total order. Two boxes write `k`; each subscriber keeps
  whichever arrived first, and they stay divergent. Re-reading from L3 makes L3
  the decider, so every box converges.
- Filling on receipt would need the writer's `minLevel` on the wire, which is
  exactly the "one caller's preference constrains another's" that keeps
  `minLevel` out of the record.

The cost is that after a remote write, the next read of that key on every other
box pays one round trip. Refresh-ahead — re-`get` on invalidation if the key was
resident — is the fix if the miss rate justifies it, and is out of scope.

## 7. Placement: `minLevel`

The shipped contract is unchanged. `minLevel` controls **fill**, not lookup
order: a `minLevel: L3` read still checks L1 first and returns a hit from there;
it just fills nothing on the way back. Its invariant — *"changes only where a
record lives and how fast it is reached, never which value is observed"* — is
what made the option safe to add, and remote invalidation is about correctness,
not placement, so it ignores `minLevel` entirely. A record is either resident
locally, in which case it must be dropped, or it is not, in which case the
invalidation is a no-op. `minLevel` is honoured where the information exists: at
the read that refills the key.

`#resolveLevel` needs reworking, because levels are no longer contiguous:

| State | L1 | L2 | L3 | highest |
|---|---|---|---|---|
| normal, adapter | ✓ | ✓ | ✓ | 3 |
| normal, no adapter | ✓ | ✓ | — | 2 |
| primary dead, adapter | ✓ | — | ✓ | **3** |
| primary dead, no adapter | ✓ | — | — | 1 |

So `minLevel: L3` on a degraded worker stays at 3 rather than clamping to 1, and
`minLevel: L2` clamps to 1. The clamp rule — down to the highest available level
— is unchanged; it simply cannot be computed as a single ceiling.

## 8. Recovery

### 8.1 What breaks

A dropped invalidation connection fails silently: the other connections keep
pointing at a dead client id, with no error, and every invalidation after that
is discarded (§12.4).

### 8.2 The node table

The arena header gains a fixed **4KB table** — up to 256 entries of
`{hash of host:port, client id}` — plus a **generation counter**.

1. The primary's provider notices a node's invalidation connection is gone.
   turbokv sets that entry to **0** and bumps the generation.
2. While an entry is 0, reads of keys on that node still work but pass
   `willCache: false`, so nothing is filled and no tracking entry is created.
   Fills already in flight are stopped by the guard (§5.5).
3. turbokv flushes the affected local entries (§8.4).
4. The provider reconnects and turbokv publishes the new client id.
5. Before its next tracked read, each process compares generations and redirects
   its tracking for that node. Redirecting works without switching tracking off,
   and keys tracked earlier are still delivered (§12.4).

Workers already map the arena read-only, so reading the table costs an atomic
load and no round trip. Keeping the map in Valkey instead would put it in the
database that `clear()` flushes.

**Redirects are keyed by node and never reused across nodes.** The server checks
only that a client id *exists*, not that it is the connection intended. Ids are
per node and start low, so an id from one node can exist on another as an
unrelated connection, and that redirect would be accepted (§12.5).

**Beyond 256 masters** the provider logs and uses Streams. A documented limit.

### 8.3 Topology changes

Failover and resharding take the same path: a promoted replica is a new master,
so the primary connects, rewrites its table entry, bumps the generation, and
flushes — the old node's tracked keys went with it.

### 8.4 Flush scope

A worker's own data connection dropping is worse than the primary's: the server
forgets everything that connection tracked and the worker cannot tell which keys
those were.

- **Default: drop only the affected node's keys.** A key's slot comes from its
  name and slots map to nodes, so the arena can walk its index and drop just
  those keys — the same walk the old namespace clear did, measured at 1.7–2.9ms
  over 200k entries, plus a CRC16 per key.
- **Fallback: flush the box**, when the topology is unknown or the server is
  not clustered.

Without this, one reconnect empties a whole box, and a cluster has
workers × nodes connections to churn.

## 9. Failure policy

| Failure | Local tiers | Result | Retried |
|---|---|---|---|
| L3 `set` fails | keep the value, capped at `l3FailTtlMs` | `false`, `lastError` | within `l3RetryMs`, or until superseded by a later write to the key |
| L3 `delete` fails | stays deleted | `false` | within `l3RetryMs` |
| L3 `clear` fails | stays cleared; L3 reads serve misses until it lands | `false` | indefinitely; flushing twice is harmless |
| Permission denied (e.g. `FLUSHDB` is in `@dangerous`) | as above | `false` | **no** — it will not fix itself |
| Queue over `l3QueueMaxBytes` | keep, capped at `l3FailTtlMs` | counted | no |
| L3 unreachable for reads | unchanged | miss | n/a |
| Invalidation connection lost | §8.4 | — | yes |

Keeping a failed write locally with a short TTL serves through an outage and then
converges. It is honest about the cost: after the TTL, a write reported as failed
does revert to L3's older value.

Everything here produces a **miss**, never a stale value, which is the rule the
code already states in `set()`: *"A miss is the failure mode this system is built
around; a stale value is not."*

## 10. API

### 10.1 Shape

Sync and async forms have identical effects; the async form is what you can
await. Every async method exists with or without an adapter, so code does not
depend on how the app is deployed.

| | Sync | Async |
|---|---|---|
| `set` | writes locally, returns the local result, queues the L3 write | resolves when L3 accepted it |
| `get` | returns what is local; on a miss, fetches from L3 in the background | resolves after consulting L3 |
| `delete` | deletes locally, queues the L3 delete | resolves when L3 deleted it |
| `has` | answers from local tiers; on a miss, checks L3 in the background | resolves after consulting L3 |
| `clear` | clears locally, queues the L3 clear | resolves when L3 was cleared |

`close()` starts returning a `Promise`, because an open Valkey connection keeps
the Node process alive, so shutdown must await the adapter's `close`. Callers
that ignore the return value are unaffected.

### 10.2 `clear()`

One cache, so it empties everything.

- **Locally:** today's `clearAll` — the arena is emptied and workers follow
  through the ring's flush marker.
- **In L3:** `FLUSHDB ASYNC`, sent to **every master**. Each node notifies its
  own tracking clients, including connections that tracked nothing, so every box
  hears once per node (§12.4, §12.5). Nodes are not flushed at the same instant,
  which is fine for operations running alongside a clear.
- **While the clear is pending**, this process serves misses from L3 rather than
  values the clear was meant to remove, and later operations queue behind it.

### 10.3 What stays local

`keys()` and `size` describe the local tiers. Enumerating L3 means `SCAN` across
the database, and these are debugging aids rather than data operations. This is a
documented deviation from §1's rule; see §14 for the alternative.

### 10.4 Stats

New counters: `l3Hits`, `l3Misses`, `l3SetFailed`, `l3DeleteFailed`,
`l3Invalidations`, `l3Resyncs`, `l3Shed`, `l3QueueBytes`, and whether each
node's invalidation connection is live. The last one matters operationally: a
silently dead subscription looks exactly like a quiet workload.

### 10.5 Configuration

Instance options, alongside `l1MaxBytes` and the rest. The adapter is an
instance, supplied per process, because each process owns its connection.

| Option | Default | Meaning |
|---|---|---|
| `l3` | none | the adapter instance; without it every async method still works and stops at L2 |
| `l3FailTtlMs` | 5,000 | how long a value whose L3 write failed stays in local tiers |
| `l3TtlMs` | 60,000 | local lifetime cap for L3-derived entries; **Streams mode only** (§6.3) |
| `l3QueueMaxBytes` | 8MB | bound on pending L3 operations, matching `maxInFlightBytes` |
| `l3RetryMs` | 2,000 | how long a transient L3 failure is retried before the operation is abandoned |
| `l3Subscribe` | `true` | receive invalidations; off for a box that is the sole writer of its keys |

Provider options, passed to `valkeyAdapter(client, opts)`:

| Option | Default | Meaning |
|---|---|---|
| `mode` | `'auto'` | `'auto'` picks tracking, falling back to Streams when `CLIENT TRACKING` is refused; `'tracking'` and `'streams'` force one, for tests |
| `streamRetentionMs` | 60,000 | invalidation history kept in the stream (Streams mode) |
| `streamTrimIntervalMs` | 1,000 | how often the stream is trimmed, rather than on every write (§6.3) |

Defaults are starting points, not measured optima; the implementation plan
includes setting them from the regression benches, the way the ring capacity and
second-chance budget were set.

## 11. Bugs found while designing this

### 11.1 Namespaces overlap (removed by Phase 0)

A local key is identified by its full string (prefix + key), and namespace names
were never checked for `:`. Verified:

| Call | Result |
|---|---|
| namespace `users` `set('42')`; default namespace `get('users:42')` | returns `users`' value |
| namespace `a:b` `set('c')`; namespace `a` `get('b:c')` | returns `a:b`'s value |

Removing namespaces removes the interpretation of prefixes, and with it the bug.

### 11.2 Stale L1 across instances in one process

Verified on the primary, two instances, same namespace and also the default one:

```
one.set('k','A'); one.get('k')   -> A
two.set('k','B'); one.get('k')   -> A     (stale)
two.delete('k');  one.get('k')   -> A     (a deleted value still served)
```

The primary skips ring records it wrote itself (`writerId === 0`), which assumes
one instance per process. Fix: a `set` or `delete` on the primary also drops the
key from the process's other instances, the way `applyBatch` does. Workers are
not yet checked. This is live in the published code path and breaks the
no-stale-values rule, which is why it is in Phase 0.

## 12. Measurements

Valkey 8.1.10 in Docker on macOS, `ioredis` 5.11.1, Node 24, plus the real
turbokv addon for the local side. Docker Desktop routes traffic through a VM, so
latencies are anchored to a measured `PING`: **137µs** for the Streams run,
**182µs** for the tracking run. Server CPU comes from Valkey's own counters and
excludes the network. All benches were throwaway; §13 makes the useful ones
permanent.

### 12.1 Tracking versus Streams

| | Streams | Tracking |
|---|---|---|
| Added to each write | `XADD`: +148B up, +22B back | `MULTI SET EXISTS EXEC`: +76B up, +31B back |
| Write latency | 1.36x `PING` pipelined, 2.59x sequential (cluster) | ~1.03x `PING`, same in cluster |
| Bytes to a box holding the key | ~100–130B | **79B** |
| Bytes to a box that does not | ~100–130B | **0B** |
| A key written repeatedly between reads | every write, to every box | **one invalidation until re-read** |
| Server CPU per write | `XADD` 0.4–2.1µs | +~0.17µs per box notified; no measurable `GET` cost |
| Receiving CPU at 1k/s | 58.7µs | **26.1µs** |
| Receiving CPU at 10k/s | 7.9µs | 7.1µs |
| Receiving CPU saturated | **0.59µs** | 2.8µs (one message per key, no batching) |
| Delivery p50 | 340–664µs | **296–329µs** |
| Delivery p99 | 680µs–2.8ms | **518–673µs** |
| Server memory | 53.5B per retained write | ~115B per cached key, +45.5B per extra holder |
| Direct writes, deletes, `FLUSHALL` | not seen | **seen** |
| Server-side expiry | not seen | **seen**, with or without access |
| Connection blip | replays | **loses invalidations; must flush** |
| ElastiCache Serverless | ✓ | ✗ |

Streams delivered ~4.33 million invalidations across all rates and reader counts
with none lost. Reconnects and retention overruns were not exercised.

### 12.2 The local side of an invalidation

| | Cost |
|---|---|
| Primary applies one (echo check, `delete`) | **~250ns** |
| A worker ingests one ring record | **~124ns** |
| A worker, key not resident | **zero** — deleting an absent key appends no ring record |
| Invalidation ring capacity | 65,536 records (125MB arena, 524,288 index slots) |

A burst of 200k applied invalidations wrapped the ring, which makes an idle
worker flush its whole L1, so the primary must apply them in bounded batches.

### 12.3 Scaling

Invalidation traffic is the limit of the design:

```
tracking: bytes/s ≈ writes/s × boxes holding the key × 79B
streams:  bytes/s ≈ writes/s × all boxes            × ~100–130B
```

At 10k writes/s across 10 boxes: Streams ~13MB/s regardless of locality;
tracking ~7.9MB/s if every box holds every key, ~0.8MB/s if each key lives on
one box. Locality comes from the load balancer — sticky sessions or shard-aware
routing concentrate keys; round-robin over a small hot keyspace does not.

### 12.4 Tracking behaviour, verified

| Question | Answer |
|---|---|
| Does `GET` start tracking? | yes |
| Does a plain `SET`? | **no** |
| Does `MULTI; SET; EXISTS; EXEC`? | **yes** (pipelining also works, but leaves a gap) |
| `NOLOOP` hides a connection's own writes? | yes |
| …and a sibling connection's? | **no** — one extra miss |
| Is it one-shot? | yes: a second write without a re-read sends nothing |
| Does expiry invalidate without an access? | **yes**, and on lazy access too |
| `DEL` and `FLUSHALL`? | yes; a flush arrives as an empty payload |
| `FLUSHDB ASYNC`? | notifies every tracking connection, **including one tracking nothing** |
| Reading an absent key? | **creates a tracking entry** (10,000 reads, 10,000 entries) |
| 4 connections of one box read one key? | **4 messages** |
| The invalidation connection dies? | **silent**: `CLIENT GETREDIR` still names the dead id and invalidations are dropped |
| Re-pointing to a new connection? | accepted without turning tracking off; keys tracked earlier still arrive |
| Default tracking table size | 1,000,000 keys |
| An idle connection on the server | ~17KB |

### 12.5 Cluster, verified on 3 nodes

| Question | Answer |
|---|---|
| Redirect within one node | works |
| Redirect across nodes | **rejected**: "The client ID you want redirect to does not exist" |
| Are client ids per node? | **yes** (55 and 6 in one run) |
| `MULTI SET + EXISTS` | works; one key, one slot |
| `FLUSHDB` on one node | notifies only that node's clients; other nodes' keys survive |

### 12.6 Why namespaces went

From the design doc (§9): a hot namespace writing ~20x the arena left **0 of
1,000** cold entries without quotas and **942 of 1,000** with them. That is the
whole irreproducible benefit, it is L2-only, and with L3 attached the evicted
entries are still in L3.

Namespace clears also iterated in every tier: the arena walked all 524,288 index
slots (1.7–2.9ms), L1 was dropped wholesale in every process, and in L3 a
`SCAN MATCH` visits every key in the database — a 1,000-key namespace cost 386ms
in a million-key database, with Valkey spending ~50ms per million keys scanned.

## 13. Testing

- **A Valkey service container in CI**, plus a 3-node cluster started in the job
  for the cluster paths.
- **Fault injection for every recovery path**, each verified to fail with the fix
  reverted, as the repo already requires: kill an invalidation connection, kill a
  worker's data connection, deny `FLUSHDB` to the user, trim the stream past a
  reader, wrap the invalidation ring, and fill the L3 queue.
- **The promotion guard gets a race test** that fails without the flush-marker
  check.
- **The throwaway benches become regression benches**, alongside the local ones.
- **Phase 0 needs its own regression tests**: the stale-L1-across-instances read
  (§11.2), and that the removed methods are gone from the public surface pin.

## 14. Open decisions for review

These are choices, not unknowns; each has a default.

1. **`keys()` and `size` stay local** (§10.3). The alternative is async variants
   backed by `SCAN`. Default: leave them local and document it.
2. **`has` on the adapter is optional**, with a `get` fallback that transfers the
   value. Default: optional.
3. **Opt-in tracking** (§6.2) needs one verification before it becomes the read
   path default: whether `CLIENT CACHING YES` is accepted inside `MULTI`. First
   task of the implementation plan. If it is not, the provider issues it as the
   preceding pipelined command instead.
4. **Phase 0 ordering.** Default: Phase 0 lands as its own commits before any L3
   work, because the stale read is live and the API shrinks first.

## 15. Documented limits

- Invalidation traffic grows with writes × boxes holding the key. Suits tens of
  boxes at moderate write rates; large fleets or write-heavy workloads will find
  this the constraint.
- Tracking: 256 masters, then Streams. The server's tracking table holds
  1,000,000 keys by default; past that it invalidates older keys, costing misses.
- Streams: needs Redis/Valkey ≥ 7.0, and cannot see writes that bypass turbokv,
  which is what `l3TtlMs` bounds.
- One reconnect of any process's connection costs a flush, narrowed to one
  node's keys where the topology is known.
- The codec decides whether L3 is interoperable.
