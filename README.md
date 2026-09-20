# turbokv

A layered in-memory KV cache for Node.js clusters.

**L1** is a per-process JS `Map` with a byte budget. **L2** is a shared-memory
arena the primary owns and workers map *read-only* — the fd is opened `O_RDONLY`,
so a worker cannot write the arena even deliberately. Worker writes travel to the
primary through per-worker shared-memory submission rings rather than the cluster
IPC channel.

```js
const { TurboKV } = require('turbokv');
// or: import { TurboKV } from 'turbokv';

// primary, before forking
const cache = TurboKV.open();
TurboKV.install(require('cluster'));

// anywhere
cache.set('user:42', 'ada', { ttlMs: 60_000 });
cache.get('user:42');        // 'ada'
```

## Why the rings

`process.send` was never bandwidth-limited — the raw channel carries 439 MB/s
under JSON. Its costs are that each send *synchronously freezes* the sending
worker's event loop while V8 serialises the batch (0.49ms p50, 1.15ms p99 for a
~525KB batch), and that it is the one channel the application shares for its own
messages. Measured against a control doing identical work that never reaches the
channel, cache traffic degraded an application's own IPC round trip from 0.69ms
to 13.59ms at p99. Rings replace that with a memcpy:

| | delivered | shed | worker loop p99 |
|---|---|---|---|
| cluster IPC | 551k writes/s | 41.7% | 5.70ms |
| shared memory | **1180k writes/s** | **0%** | **4.03ms** |

## Storage modes

| mode | stores | keeps `Date`/`Map`/`Set` | cost |
|---|---|---|---|
| `bytes` *(default)* | primitives + binary, no codec | rejects them | 1504 ns/op |
| `safe` | JSON | silently degrades them | 1789 ns/op |
| `direct` | `v8.serialize` | yes | 3250 ns/op |

`direct` uses each runtime's own `v8.serialize`, whose format differs between
Node and Bun. That is not reachable in practice: a cluster is built from
processes of one runtime, and an arena never outlives the primary that created
it — `create()` unlinks any prior segment and starts empty. Worth knowing only
if you attach to an arena from outside its own cluster, which is not a
supported arrangement.

## Runtime support

Requires Node 18 or newer (Node-API level 8).

| | Node | Bun | Deno |
|---|---|---|---|
| addon, cluster, both transports | yes | yes | yes |
| CJS + ESM entry points | yes | yes | yes |
| post-collection heap guard | yes | yes | yes |

### Installing

Prebuilt binaries ship inside the npm tarball, so a normal install needs no
compiler and no network fetch. The addon is Node-API, so one prebuild per
platform serves every supported Node major:

| | x64 | arm64 |
|---|---|---|
| Linux (glibc) | yes | yes |
| Linux (musl) | yes | — |
| macOS | yes | yes |
| Windows | yes | — |

Anything not listed falls back to compiling from source at install time, which
needs a compiler and Python, exactly as before.

> **Bun:** this used to need `"trustedDependencies": ["turbokv"]`, because
> Bun blocks lifecycle scripts by default and the addon was therefore never
> compiled. With prebuilds it no longer does — `bun add` still reports
> "Blocked 1 postinstall", and the package works anyway, because the binary is
> already there. On a platform with no prebuild the old caveat still applies.

Bun runs the entire suite green — every unit test, both transports, and the full
primary-death recovery sequence — at roughly 15% below Node's throughput. The
heap guard works on all three: it is driven by a `FinalizationRegistry` rather
than gc performance entries, which Bun and Deno accept but never emit.

The `direct` codec used to return zero-filled typed arrays on Deno. `v8.deserialize`
does not copy an `ArrayBufferView` out of its input — it returns a view over it —
and the buffer it was given was a slice of Node's shared 8KB pool, so the value's
correctness rested on the runtime deriving an address from a non-zero
`byteOffset`. Deno 2.8.3 adds that offset twice. Decoding into an unpooled,
exactly-sized buffer removes the dependency (and stops each cached typed array
pinning a pool slab on every runtime); see DESIGN.md decision 46.

## Releasing

Tag-driven. `git tag v0.1.0 && git push --tags` runs the release workflow: it
builds a prebuild on each target, verifies every one of them loads and passes
the suite, refuses to continue unless all six arrived and the tag matches
`package.json`, then publishes.

Authentication is **trusted publishing** ([npm OIDC][tp]) — the workflow proves
its identity to npm directly, so there is no long-lived token in repository
secrets to leak, steal or forget to rotate, and provenance is attached
automatically.

One exception: a trusted publisher is configured in an *existing* package's
settings, so the first version cannot use it. Publish once via
`workflow_dispatch` with `bootstrap: true` (which uses `NPM_TOKEN`), then:

1. npmjs.com → the package → Settings → Trusted Publisher → GitHub Actions
2. organization `krassx`, repository `turbokv`, workflow `release.yml`
3. delete the `NPM_TOKEN` secret and revoke the token

After that the bootstrap path cannot authenticate at all, which is the point.

[tp]: https://docs.npmjs.com/trusted-publishers/

## Layout

```
index.js  index.mjs  index.d.ts   entry points; consumers never see src/
binding.gyp                       addon build, at the package root
src/      turbokv.js           the JS layer (L1, coherence, transports)
          native.js               single place the addon is resolved
          binding.cc *.h          the arena, submission rings, platform layer
          vendor/                 rapidhash, verbatim upstream
test/     *_test.js  run.js       the suite; `npm test` runs run.js, so does CI
          *.cc                    standalone C++ tests (arena, rings)
          tsan/  types/           sanitizer gate, TypeScript declaration tests
bench/                            microbenchmarks and design experiments
loadtest/                         sustained multi-worker load harness (Docker)
scripts/                          build helpers
```

## Operational notes

- **Sizing**: on Linux the arena is backed by `/dev/shm`. Docker defaults it to
  64MB — pass `--shm-size` or `create()` fails with a message naming it.
- **Worker ids** start at 1. `0` is the primary and is rejected.
- **`set` never throws.** An unusable key, value or type returns `false` with the
  reason in `lastError`.
- **Primary death**: a worker detaches, keeps serving its warm L1, polls, and
  recovers when a heartbeat *advances* — then flushes L1 and re-claims a ring.
- **Routing cluster messages yourself**: `TurboKV.install(cluster)` is the easy
  path and does the whole job. If your application owns the primary's `message`
  handler instead, it must pass turbokv's messages to `TurboKV.applyBatch()` —
  **on both transports**. `'shm'` moves the *writes* off the channel; it does
  not take a worker off it. A `clearAll()`'s wipe, the two halves of its
  cluster-wide L3 clear generation, and the conditional re-time a worker asks
  for when an `l3` write fails all still travel as cluster messages and are
  applied nowhere else. Route the ring doorbell but not these and a value L3
  rejected stays in the shared arena with no expiry, for every process, until
  something overwrites it. Call `TurboKV.releaseWorker()` on `'exit'` and
  `'disconnect'` for the same reason.
- **`transport: 'ipc'` and the `l3` adapter**: with an adapter attached, prefer
  the default `'shm'` transport. The primary drains every worker's submission
  ring before it decides whether an L3 read may be promoted, so a worker's
  write is never overwritten by the pre-write value L3 was still serving. An
  IPC-transport worker's write sits in its outbox, or in the cluster channel,
  where the primary cannot reach it — so for the one hop until that batch is
  delivered the primary may promote over it. The worker's own value wins once
  the batch is applied; the window is bounded, not closed.
- **`bigint` values on `transport: 'ipc'`**: `process.send` serialises a batch
  as a unit and refuses a `bigint` under the default JSON serialization, so one
  such value drops **every other key's write in the same batch** — all of which
  already returned `true`. Fork with `serialization: 'advanced'`, or stay on
  `'shm'`, where values never touch the channel. `stats.flushDropped` counts it.
- **Watching for that**: `stats.l3FailTtlUnapplied` counts caps *proven* not to
  have landed, and `stats.l3FailTtlUnconfirmed` counts those whose outcome the
  worker could not establish. **Watch both, and expect the second one.** The
  proof needs the invalidation ring to still reach back to the moment the cap
  was taken; the ring holds at most 65536 records, on the order of 100ms of
  primary writes under load, against roughly 7 seconds from a cap's mark to its
  retirement at the default `l3FailTtlMs`. So on a busy box `Unapplied` goes
  quiet and `Unconfirmed` becomes the normal bucket — an operator watching only
  the first would see nothing during exactly the outage this exists for.

The full design, decision log and measurements — including what was built and
rejected — are in [DESIGN.md](DESIGN.md).

## License

MIT. Vendors [rapidhash](src/vendor/rapidhash.h) (MIT).
