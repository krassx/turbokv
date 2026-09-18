// The Node-API level this addon REQUIRES, and therefore the floor on which
// Node versions can load it at all. 8 is Node 18.0's level, matching the
// engines range; nothing here uses anything above NAPI 6 (napi_create_bigint_words
// and napi_get_value_bigint_words are the newest calls, both NAPI 6).
//
// This was 10 from the first prototype commit, with no comment and nothing
// depending on it. Node 18 and 20 cap at NAPI 9, so they REFUSED the module --
// and Node's refusal path is a null-pointer dereference, not a thrown error, so
// the symptom was a bare SIGSEGV during require() with no diagnostic. Reproduced
// with a ten-line addon that does nothing but declare NAPI 99: segfaults on Node
// 20, 22 AND 24 (x0 = 0 at the fault). So the declared version is ours to get
// right, and the silence when it is wrong is Node's.
#define NAPI_VERSION 8
#include <node_api.h>
// Compression is an optional build feature; see binding.gyp. Without it the
// addon has no external dependencies.
#ifdef TURBOKV_LZ4
#include <lz4.h>
#endif
#include <stdlib.h>
#include <time.h>
#include "store_ops.h"
#include "submit.h"
#include <vector>

static Store g;
static uint8_t *scratch = nullptr;
static const size_t SCRATCH = 4u << 20;
static uint8_t *cbuf = nullptr;
static uint32_t compressMin = 256;
static int compressAccel = 1;   // LZ4 acceleration: higher = faster, worse ratio



static bool strInfo(napi_env env, napi_value v, size_t *charLen, size_t *utf8Len);

// Keys are read as UTF-8 and length-checked. They used to be read with
// napi_get_value_string_latin1 into a fixed char[512]: a key of 512+ bytes was
// silently truncated to its prefix, and any character above U+00FF was folded
// to its low byte, so distinct keys collided and returned EACH OTHER'S VALUES.
static const size_t KEY_MAX = 1024;
// Expiry of the entry the last Get returned, so the JS layer can carry TTL into
// L1 on a refill. Read only on an L2 hit, so it costs nothing on the hot path.
static uint32_t g_lastExpiresAt = 0;
// Plain function rather than a statement-expression: `({ ... })` is a GNU
// extension that MSVC rejects, and it was the only thing in our own code
// standing between this file and a Windows compiler.
static bool isBinaryValue(napi_env env, napi_value v) {
  bool isBuf = false, isTa = false, isAb = false, isDv = false;
  napi_is_buffer(env, v, &isBuf);
  napi_is_typedarray(env, v, &isTa);
  napi_is_arraybuffer(env, v, &isAb);
  napi_is_dataview(env, v, &isDv);
  return isBuf || isTa || isAb || isDv;
}

// TTL from JS, clamped. napi_get_value_int32 applies ToInt32, so a 30-day TTL
// (2,592,000,000 ms) wrapped to a NEGATIVE value and became "no expiry" -- the
// value was immortal -- while a 50-day TTL became 25,032,704 ms, expiring in 7
// hours. Read it as a double and clamp to the range the wrap-aware comparison
// can represent, so an over-long TTL is capped rather than inverted.
static inline uint32_t readTtlMs(napi_env env, napi_value v) {
  double d = 0;
  if (napi_get_value_double(env, v, &d) != napi_ok) return 0;
  if (!(d > 0)) return 0;                       // also catches NaN
  if (d > (double)TC_TTL_MAX_MS) return TC_TTL_MAX_MS;
  return (uint32_t)d;
}

static bool readKey(napi_env env, napi_value v, char *buf, size_t *outLen) {
  size_t need = 0;
  if (napi_get_value_string_utf8(env, v, nullptr, 0, &need) != napi_ok) return false;
  if (need > KEY_MAX) return false;               // caller reports this, never truncates
  size_t got = 0;
  if (napi_get_value_string_utf8(env, v, buf, KEY_MAX + 1, &got) != napi_ok) return false;
  // V8 replaces every unpaired surrogate with U+FFFD when converting to UTF-8,
  // so '\uD800', '\uDC00' and '\uFFFD' all encode to the same three bytes and
  // returned each other's values -- the same aliasing class this function was
  // already fixed for once (latin1 folding). Only pay for the UTF-16 scan when
  // the encoded form actually contains U+FFFD, which real keys never do.
  bool maybeFolded = false;
  for (size_t i = 0; i + 2 < got; i++)
    if ((uint8_t)buf[i] == 0xEF && (uint8_t)buf[i + 1] == 0xBF && (uint8_t)buf[i + 2] == 0xBD) {
      maybeFolded = true; break;
    }
  if (maybeFolded) {
    size_t u16 = 0;
    if (napi_get_value_string_utf16(env, v, nullptr, 0, &u16) == napi_ok && u16 <= KEY_MAX) {
      static char16_t k16[KEY_MAX + 2];
      size_t got16 = 0;
      if (napi_get_value_string_utf16(env, v, k16, KEY_MAX + 2, &got16) == napi_ok) {
        for (size_t i = 0; i < got16; i++) {
          char16_t c = k16[i];
          if (c >= 0xD800 && c <= 0xDBFF) {
            if (i + 1 >= got16 || k16[i + 1] < 0xDC00 || k16[i + 1] > 0xDFFF) return false;
            i++;
          } else if (c >= 0xDC00 && c <= 0xDFFF) return false;
        }
      }
    }
  }
  *outLen = got;
  return true;
}
static void put(napi_env env, napi_value o, const char *k, double v);

// Every entry point must tolerate being called before an arena exists or
// after one was destroyed: both used to dereference a null header and SIGSEGV.
#define NEED_STORE(ret) if (!g.base || !g.h) { return ret; }

// Mutating entry points must refuse a read-only attachment. Without this a
// worker that reached set/del/incr/cas/clear/sweep/heartbeat -- or Get's
// non-LZ4 branch, which increments a counter in the header -- took a SIGBUS on
// the PROT_READ mapping instead of an exception it could handle.
#define NEED_WRITABLE(ret) if (!g.writable) { \
  napi_throw_error(env, nullptr, "turbokv: this process attached the arena read-only; " \
                                 "only the primary may write"); return ret; }

#define ARG(n) napi_value argv[n]; size_t argc = n; \
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);

static napi_value Create(napi_env env, napi_callback_info info) {
  ARG(4)
  char nm[64]; size_t l;
  napi_get_value_string_latin1(env, argv[0], nm, sizeof(nm), &l);
  int64_t total, slots; int32_t mode;
  napi_get_value_int64(env, argv[1], &total);
  napi_get_value_int64(env, argv[2], &slots);
  napi_get_value_int32(env, argv[3], &mode);
  if (!scratch) { scratch = (uint8_t *)malloc(SCRATCH); cbuf = (uint8_t *)malloc(SCRATCH); }
  bool ok = g.create(nm, (uint64_t)total, (uint64_t)slots, (uint8_t)mode);
  napi_value r; napi_get_boolean(env, ok, &r); return r;
}

static napi_value Attach(napi_env env, napi_callback_info info) {
  ARG(1)
  char nm[64]; size_t l;
  napi_get_value_string_latin1(env, argv[0], nm, sizeof(nm), &l);
  if (!scratch) { scratch = (uint8_t *)malloc(SCRATCH); cbuf = (uint8_t *)malloc(SCRATCH); }
  bool ok = g.attachReadOnly(nm);
  if (!ok && g.attachError == 1) {
    napi_throw_error(env, nullptr,
      "turbokv: this arena contains LZ4-compressed entries but the addon was "
      "built without LZ4. Rebuild with --turbokv_lz4=1, or recreate the arena "
      "with compression disabled.");
    return nullptr;
  }
  napi_value r; napi_get_boolean(env, ok, &r); return r;
}

static napi_value HasLz4(napi_env env, napi_callback_info) {
  napi_value r;
#ifdef TURBOKV_LZ4
  napi_get_boolean(env, true, &r);
#else
  napi_get_boolean(env, false, &r);
#endif
  return r;
}

static napi_value SetCompressMin(napi_env env, napi_callback_info info) {
  ARG(2) int32_t v; napi_get_value_int32(env, argv[0], &v); compressMin = (uint32_t)v;
  int32_t a = 0; if (argc > 1 && napi_get_value_int32(env, argv[1], &a) == napi_ok && a > 0) compressAccel = a;
  return nullptr;
}

// Encode a JS value into `scratch`, tagging its type. Extracted from Set so the
// shared-memory submission path encodes identically -- a second copy of this
// type ladder would drift, and the two paths must agree byte for byte or a
// value written through one and read through the other changes type.
static bool encodeValue(napi_env env, napi_value v, size_t *vlenOut, uint8_t *flagsOut) {
  // Encode by type, tagging the entry so the reader rebuilds the right JS
  // value. Doubles are stored as their 8 raw bytes: exact, and no parsing.
  napi_valuetype vt;
  napi_typeof(env, v, &vt);
  size_t vlen = 0;
  uint8_t flags = 0;
  if (vt == napi_string) {
    // ASCII is stored one byte per char and handed back as a one-byte V8
    // string; anything else is stored as UTF-8. Everything used to go through
    // latin1, which silently mangled non-ASCII.
    size_t charLen = 0, utf8Len = 0;
    if (!strInfo(env, v, &charLen, &utf8Len)) return false;
    const bool ascii = (utf8Len == charLen);
    if ((ascii ? charLen : utf8Len) + 1 > SCRATCH) return false;
    size_t got = 0;
    if (ascii) napi_get_value_string_latin1(env, v, (char *)scratch, SCRATCH, &got);
    else       napi_get_value_string_utf8(env, v, (char *)scratch, SCRATCH, &got);
    vlen = got;
    flags = FLAG_STRING | (ascii ? FLAG_LATIN1 : 0);
  } else if (vt == napi_number) {
    double d = 0; napi_get_value_double(env, v, &d);
    memcpy(scratch, &d, sizeof(d)); vlen = sizeof(d); flags = FLAG_NUMBER;
  } else if (vt == napi_boolean) {
    bool bv = false; napi_get_value_bool(env, v, &bv);
    scratch[0] = bv ? 1 : 0; vlen = 1; flags = FLAG_BOOL;
  } else if (vt == napi_object && isBinaryValue(env, v)) {
    // Binary values. Decision 4 lists Buffer/Uint8Array/ArrayBuffer as accepted
    // value types; the native layer only ever handled strings and scalars, so
    // primitives mode rejected them. Stored as raw bytes.
    void *data = nullptr; size_t len = 0;
    bool isBuf = false, isTa = false, isAb = false;
    napi_is_buffer(env, v, &isBuf);
    napi_is_typedarray(env, v, &isTa);
    napi_is_arraybuffer(env, v, &isAb);
    if (isBuf) { napi_get_buffer_info(env, v, &data, &len); }
    else if (isTa) {
      napi_typedarray_type t; size_t n = 0; napi_value ab; size_t off = 0;
      napi_get_typedarray_info(env, v, &t, &n, &data, &ab, &off);
      size_t elem = 1;
      switch (t) {
        case napi_int16_array: case napi_uint16_array: elem = 2; break;
        case napi_int32_array: case napi_uint32_array: case napi_float32_array: elem = 4; break;
        case napi_float64_array: case napi_bigint64_array: case napi_biguint64_array: elem = 8; break;
        default: elem = 1;
      }
      len = n * elem;
    } else if (isAb) { napi_get_arraybuffer_info(env, v, &data, &len); }
    else { napi_get_dataview_info(env, v, &len, &data, nullptr, nullptr); }
    if (!data || len + 1 > SCRATCH) return false;
    memcpy(scratch, data, len);
    vlen = len; flags = FLAG_BINARY;
  } else if (vt == napi_bigint) {
    // Arbitrary precision: sign byte followed by 64-bit words, little-endian.
    // Querying the word count requires BOTH sign_bit and words to be null;
    // passing a non-null sign_bit takes the other branch and fails CHECK_ARG.
    int sign = 0; size_t words = 0;
    if (napi_get_value_bigint_words(env, v, nullptr, &words, nullptr) != napi_ok ||
        8 + words * 8 > SCRATCH) return false;
    if (words && napi_get_value_bigint_words(env, v, &sign, &words,
                                             (uint64_t *)(scratch + 8)) != napi_ok) {
      return false;   // `return r` here converted a non-null napi_value to TRUE,
                      // reporting success with vlen/flags never assigned
    }
    memset(scratch, 0, 8);
    scratch[0] = (uint8_t)sign;          // written AFTER the call that fills it
    vlen = 8 + words * 8; flags = FLAG_BIGINT;
  } else if (vt == napi_null) {
    vlen = 0; flags = FLAG_NULL;
  } else {
    return false;                      // unsupported type
  }
  *vlenOut = vlen; *flagsOut = flags;
  return true;

}


// ---- shared-memory write submission -------------------------------------
//
// Workers push encoded records into their own SPSC ring; the primary drains and
// applies them. This replaces process.send on the hot write path, whose real
// cost was never bandwidth but a synchronous 0.49ms/1.15ms (p50/p99) freeze of
// the sending worker's event loop while V8 serialized each ~525KB batch -- on
// the channel the application shares for its own messages.
static Submit g_submit;
static int32_t g_ringIdx = -1;

// primary: submitCreate(name, ringCount, ringBytes)
// The ring segment this process created, so it can be unlinked when replaced or
// destroyed. Submit itself tracks no name.
static char g_prevRing[256] = {0};

static napi_value SubmitCreate(napi_env env, napi_callback_info info) {
  ARG(3)
  char name[256]; size_t nl = 0;
  napi_get_value_string_utf8(env, argv[0], name, sizeof name, &nl);
  int32_t count = 0, bytes = 0;
  napi_get_value_int32(env, argv[1], &count);
  napi_get_value_int32(env, argv[2], &bytes);
  // Unlink the ring this process created before, if any. Submit tracks no name
  // of its own, so the name can only be released here -- otherwise a process
  // that creates a second ring strands the first segment permanently.
  if (g_prevRing[0] && strcmp(g_prevRing, name) != 0) shmUnlink(g_prevRing);
  shmUnlink(name);                       // reclaim a crashed run's segment
  bool ok = g_submit.create(name, (uint32_t)count, (uint32_t)bytes, KEY_MAX, (uint32_t)SCRATCH);
  if (ok) snprintf(g_prevRing, sizeof g_prevRing, "%s", name);
  napi_value r; napi_get_boolean(env, ok, &r); return r;
}

// worker: submitOpen(name) then submitClaim() -> ring index, or -1
static napi_value SubmitOpen(napi_env env, napi_callback_info info) {
  ARG(1)
  char name[256]; size_t nl = 0;
  napi_get_value_string_utf8(env, argv[0], name, sizeof name, &nl);
  bool ok = g_submit.open(name);
  napi_value r; napi_get_boolean(env, ok, &r); return r;
}

// Claim a free ring by CAS on its owner field. The slot is assigned, never
// passed in by the caller -- which is also what makes a worker-id collision
// with the primary structurally impossible rather than merely rejected.
static napi_value SubmitClaim(napi_env env, napi_callback_info info) {
  napi_value r;
  if (!g_submit.base) { napi_create_int32(env, -1, &r); return r; }
  if (g_ringIdx >= 0) { napi_create_int32(env, g_ringIdx, &r); return r; }   // already claimed
  uint32_t me = platformPid();
  for (int pass = 0; pass < 2; pass++) {
    for (uint32_t i = 0; i < g_submit.ringCount; i++) {
      SubmitRing *ring = g_submit.ring(i);
      uint32_t expect = ring->owner.load(std::memory_order_acquire);
      // Pass 0 takes only free slots. Pass 1 reclaims slots whose owner process
      // no longer exists: without this a crashed worker holds its slot forever,
      // and after enough churn every new worker silently falls back to IPC.
      if (expect != 0) {
        if (pass == 0 || expect == me || platformPidAlive(expect)) continue;
      }
      if (ring->owner.compare_exchange_strong(expect, me,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        // A reclaimed slot's queued records are kept for the same reason: a
        // partially written record is never published, so everything between
        // tail and head is complete. Resetting tail here also raced the
        // primary's in-flight drain, which would then store a stale, smaller
        // tail back over it and leave the ring pointing mid-record.
        g_ringIdx = (int32_t)i;
        napi_create_int32(env, (int32_t)i, &r); return r;
      }
    }
  }
  napi_create_int32(env, -1, &r); return r;
}

// worker: submitSet(key, value, ttlMs) -> bool (false = shed, ring full)
static napi_value SubmitSet(napi_env env, napi_callback_info info) {
  ARG(3)
  napi_value r;
  if (!g_submit.base || g_ringIdx < 0) { napi_get_boolean(env, false, &r); return r; }
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  size_t vlen = 0; uint8_t flags = 0;
  if (!encodeValue(env, argv[1], &vlen, &flags)) { napi_get_boolean(env, false, &r); return r; }
  uint32_t ttlMs = argc > 2 ? readTtlMs(env, argv[2]) : 0;
  bool ok = submitPush(g_submit, (uint32_t)g_ringIdx, SUBMIT_OP_SET, flags, ttlMs,
                       key, (uint32_t)klen, scratch, (uint32_t)vlen);
  napi_get_boolean(env, ok, &r); return r;
}

// worker: submitDel(key) -> bool
static napi_value SubmitDel(napi_env env, napi_callback_info info) {
  ARG(1)
  napi_value r;
  if (!g_submit.base || g_ringIdx < 0) { napi_get_boolean(env, false, &r); return r; }
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  bool ok = submitPush(g_submit, (uint32_t)g_ringIdx, SUBMIT_OP_DEL, 0,
                       0, key, (uint32_t)klen, nullptr, 0);
  napi_get_boolean(env, ok, &r); return r;
}

// primary: submitDrain(maxRecords) -> records applied.
// Bounded on purpose: draining is synchronous work on the primary's event loop,
// so an unbounded drain would trade the worker's stall for a primary stall.
static napi_value SubmitDrain(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_WRITABLE(nullptr)
  napi_value r;
  if (!g_submit.base || !g.base) { napi_create_int32(env, 0, &r); return r; }
  int32_t budget = 4096;
  if (argc > 0) napi_get_value_int32(env, argv[0], &budget);
  if (budget <= 0) budget = 4096;
  int32_t applied = 0;
  const uint32_t cap = g_submit.ringBytes;
  for (uint32_t i = 0; i < g_submit.ringCount && applied < budget; i++) {
    SubmitRing *ring = g_submit.ring(i);
    uint64_t head = ring->head.load(std::memory_order_acquire);
    uint64_t tail = ring->tail.load(std::memory_order_relaxed);
    if (tail == head) continue;
    // A producer can never have more than `cap` bytes live. A larger span means
    // the worker corrupted its own head, so refuse to walk it: without this a
    // worker could set head to 2^50 and the loop below would step over that many
    // bytes synchronously, hanging the primary's event loop.
    if (head - tail > (uint64_t)cap) {
      ring->corrupt.fetch_add(1, std::memory_order_relaxed);
      ring->tail.store(head, std::memory_order_release);   // resynchronise; only this worker loses writes
      continue;
    }
    const int32_t before = applied;
    uint8_t *base = g_submit.ringData(i);
    // Bound the WORK, not just the records applied. SKIP records and implicit
    // gaps advance tail without incrementing `applied`, so a ring full of them
    // never reaches the budget -- measured 717M iterations against a budget of
    // 4096. Every step through the ring counts.
    int32_t steps = 0;
    const int32_t maxSteps = budget * 4 + 64;
    while (tail < head && applied < budget && steps < maxSteps) {
      steps++;
      uint32_t off = (uint32_t)(tail & (cap - 1));
      uint32_t gap = submitGapAt(off, cap);
      if (gap) { tail += gap; continue; }
      // SNAPSHOT the header, then validate and use ONLY the snapshot.
      //
      // Validating through the pointer and then reading the fields again is a
      // TOCTOU: `rec` points into a segment every worker maps read-write, and
      // the compiler emits fresh loads for each use (confirmed in the shipped
      // object code). A worker that flips valLen after validation passed gets
      // its value through to storeSet, where `align8(sizeof(Entry) + keyLen +
      // storedLen)` truncates through 2^32 to something small -- so the
      // allocation succeeds -- while the memcpy copies the full length. ASan
      // shows a ~4GB copy out of the ring and past the arena.
      //
      // This is the same rule the geometry already follows (see submit.h):
      // snapshot once, never re-read from shared memory. The key and value
      // BYTES stay where they are; a concurrent mutation of those only garbles
      // that worker's own value, within bounds already fixed by the snapshot.
      SubmitRec rec;
      memcpy(&rec, base + off, sizeof(SubmitRec));
      uint64_t avail = head - tail;
      if (avail > cap - off) avail = cap - off;
      // Ring contents are written by a worker and are therefore untrusted.
      // A rejected record stops this ring rather than the drain: the worker
      // loses its own writes, nothing else is affected.
      if (!submitValidate(g_submit, &rec, avail)) {
        ring->corrupt.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      if (rec.op != SUBMIT_OP_SKIP) {
        const uint8_t *k = base + off + sizeof(SubmitRec);
        const uint8_t *v = k + rec.keyLen;
        if (rec.op == SUBMIT_OP_SET) {
          uint32_t ttl = rec.ttlMs > TC_TTL_MAX_MS ? TC_TTL_MAX_MS : rec.ttlMs;
          uint32_t expiresAt = ttl ? nowRelMs(g) + ttl : 0;
          storeSet(g, k, (uint16_t)rec.keyLen, v, rec.valLen, rec.valLen,
                   rec.flags, expiresAt, (uint16_t)(i + 1));
        } else {
          storeDelete(g, k, (uint16_t)rec.keyLen, (uint16_t)(i + 1));
        }
        applied++;
      }
      tail += rec.len;
    }
    ring->tail.store(tail, std::memory_order_release);
    ring->applied.fetch_add((uint64_t)(applied - before), std::memory_order_relaxed);
  }
  napi_create_int32(env, applied, &r); return r;
}

// Is there anything to drain? Cheap enough to call every event-loop turn.
static napi_value SubmitPending(napi_env env, napi_callback_info info) {
  napi_value r;
  uint64_t pending = 0;
  if (g_submit.base)
    for (uint32_t i = 0; i < g_submit.ringCount; i++)
      pending += g_submit.ring(i)->head.load(std::memory_order_acquire) -
                 g_submit.ring(i)->tail.load(std::memory_order_relaxed);
  napi_create_double(env, (double)pending, &r); return r;
}

static napi_value SubmitStats(napi_env env, napi_callback_info info) {
  napi_value o; napi_create_object(env, &o);
  double pushed = 0, applied = 0, shed = 0, corrupt = 0, claimed = 0;
  if (g_submit.base) {
    for (uint32_t i = 0; i < g_submit.ringCount; i++) {
      SubmitRing *r = g_submit.ring(i);
      pushed += (double)r->pushed.load(std::memory_order_relaxed);
      applied += (double)r->applied.load(std::memory_order_relaxed);
      shed += (double)r->shed.load(std::memory_order_relaxed);
      corrupt += (double)r->corrupt.load(std::memory_order_relaxed);
      if (r->owner.load(std::memory_order_relaxed)) claimed++;
    }
  }
  napi_value v;
#define SETN(name, val) napi_create_double(env, (val), &v); napi_set_named_property(env, o, name, v);
  SETN("pushed", pushed) SETN("applied", applied) SETN("shed", shed)
  SETN("corrupt", corrupt) SETN("rings", claimed)
  SETN("enabled", g_submit.base ? 1 : 0) SETN("ringIndex", (double)g_ringIdx)
#undef SETN
  return o;
}

// Give this process's ring slot back so another worker can take it.
static napi_value SubmitRelease(napi_env env, napi_callback_info info) {
  if (g_submit.base && g_ringIdx >= 0) {
    SubmitRing *ring = g_submit.ring((uint32_t)g_ringIdx);
    // Do NOT discard queued records. They were published with a release store,
    // so each one is complete and valid, and the primary will apply them. The
    // previous `tail = head` here destroyed every write a worker had made but
    // the primary had not yet drained -- so close() silently lost data on the
    // default transport while the IPC path delivered it. The next owner
    // inheriting them is harmless: they are legitimate writes, applied in order.
    ring->owner.store(0, std::memory_order_release);
  }
  g_ringIdx = -1;
  return nullptr;
}

// Largest value a single record can carry. A value can be under the ARENA limit
// yet too large for a ring, in which case it could never be delivered and set()
// would keep reporting success -- the caller needs this bound at call time.
static napi_value SubmitMaxValue(napi_env env, napi_callback_info info) {
  napi_value r;
  uint32_t cap = g_submit.base ? g_submit.ringBytes : 0;
  uint32_t maxRec = cap ? (cap / 2) : 0;
  uint32_t maxVal = maxRec > (uint32_t)(sizeof(SubmitRec) + KEY_MAX + 8)
                  ? maxRec - (uint32_t)(sizeof(SubmitRec) + KEY_MAX + 8) : 0;
  napi_create_int32(env, (int32_t)maxVal, &r); return r;
}

static napi_value SubmitDestroy(napi_env env, napi_callback_info info) {
  if (g_submit.base) g_submit.close();
  // close() releases the mapping; the NAME outlives it until unlinked, and a
  // process that created a ring is the one responsible for removing it. Workers
  // use submitRelease() instead and must never unlink a ring they only opened.
  if (g_prevRing[0]) { shmUnlink(g_prevRing); g_prevRing[0] = '\0'; }
  g_ringIdx = -1;
  return nullptr;
}

// set(key, value, writerId, ttlMs) - value is a latin1 string in this prototype
static napi_value Set(napi_env env, napi_callback_info info) {
  ARG(4)
  NEED_STORE(nullptr)
  NEED_WRITABLE(nullptr)
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  size_t vlen = 0;
  uint8_t flags = 0;
  if (!encodeValue(env, argv[1], &vlen, &flags)) { napi_value r; napi_get_boolean(env, false, &r); return r; }
  const uint8_t *payload = scratch;
  uint32_t storedLen = (uint32_t)vlen, rawLen = (uint32_t)vlen;
  if ((flags & FLAG_STRING) && vlen >= compressMin) {
#ifdef TURBOKV_LZ4
    int c = LZ4_compress_fast((const char *)scratch, (char *)cbuf, (int)vlen, (int)SCRATCH, compressAccel);
    if (c > 0 && (uint32_t)c < rawLen - (rawLen >> 3)) {   // keep only if >12.5% smaller
      payload = cbuf; storedLen = (uint32_t)c; flags |= FLAG_COMPRESSED;
      g.h->features |= FEATURE_LZ4;      // record that compressed entries exist
    }
#endif
  }
  int32_t writerId = 0;
  if (argc > 2) napi_get_value_int32(env, argv[2], &writerId);
  uint32_t ttlMs = argc > 3 ? readTtlMs(env, argv[3]) : 0;
  uint32_t expiresAt = ttlMs ? nowRelMs(g) + ttlMs : 0;
  bool ok = storeSet(g, (const uint8_t *)key, (uint16_t)klen, payload, storedLen, rawLen,
                     flags, expiresAt, (uint16_t)writerId);
  napi_value r; napi_get_boolean(env, ok, &r); return r;
}

// get(key) -> string | undefined  (full path incl. decompress + V8 string build)
static napi_value Get(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_STORE(nullptr)
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  ReadResult rr;
  if (!storeGet(g, (const uint8_t *)key, (uint16_t)klen, scratch, SCRATCH, &rr, nowRelMs(g)))
    return nullptr;
  g_lastExpiresAt = rr.expiresAt;
  const char *src = (const char *)rr.buf;
  if (rr.flags & FLAG_COMPRESSED) {
#ifdef TURBOKV_LZ4
    int d = LZ4_decompress_safe((const char *)rr.buf, (char *)cbuf, (int)rr.storedLen, (int)SCRATCH);
    if (d < 0) return nullptr;
    src = (const char *)cbuf;
#else
    // A compressed entry in a build without LZ4: report a miss rather than
    // hand back compressed bytes. attach() refuses such an arena up front, so
    // this is a belt-and-braces path.
    if (g.writable) g.h->readsSkippedNoLz4++;   // header is read-only in a worker
    return nullptr;
#endif
  }
  // Every constructor's status is checked. These were ignored, so a failure left
  // `out` uninitialised and returned it.
  napi_value out = nullptr;
  if (rr.flags & FLAG_NUMBER) {
    double d = 0; memcpy(&d, src, sizeof(d));
    if (napi_create_double(env, d, &out) != napi_ok) return nullptr;
  } else if (rr.flags & FLAG_BOOL) {
    if (napi_get_boolean(env, src[0] != 0, &out) != napi_ok) return nullptr;
  } else if (rr.flags & FLAG_BINARY) {
    // Always handed back as a fresh Buffer. Decision 7 copies binary values on
    // every read because they are mutable and L1 shares its entry; a Buffer is
    // a Uint8Array subclass, so instanceof checks still hold.
    void *dst = nullptr;
    if (napi_create_buffer_copy(env, rr.rawLen, src, &dst, &out) != napi_ok) return nullptr;
  } else if (rr.flags & FLAG_BIGINT) {
    // rawLen 0 would ask for (0-1)/8 = 536,870,911 words. Unreachable from the
    // encoder, but this is the read path and the rest of it verifies rather than
    // trusts. The words start at offset 8 so the cast is aligned: they used to
    // sit at +1, which is UB on the misaligned load even though x64 and arm64
    // tolerate it, and the UBSan gate has no reason to keep tolerating it.
    if (rr.rawLen < 8 || (rr.rawLen & 7)) return nullptr;
    if (napi_create_bigint_words(env, src[0] & 1, (rr.rawLen - 8) / 8,
                                 (const uint64_t *)(src + 8), &out) != napi_ok) return nullptr;
  } else if (rr.flags & FLAG_NULL) {
    if (napi_get_null(env, &out) != napi_ok) return nullptr;
  } else if (rr.flags & FLAG_LATIN1) {
    if (napi_create_string_latin1(env, src, rr.rawLen, &out) != napi_ok) return nullptr;
  } else {
    if (napi_create_string_utf8(env, src, rr.rawLen, &out) != napi_ok) return nullptr;
  }
  return out;
}

// getLen(key) -> int  (arena lookup + decompress, no V8 string construction)
static napi_value GetLen(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_STORE(nullptr)
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  ReadResult rr;
  int32_t n = -1;
  if (storeGet(g, (const uint8_t *)key, (uint16_t)klen, scratch, SCRATCH, &rr, nowRelMs(g))) {
#ifdef TURBOKV_LZ4
    if (rr.flags & FLAG_COMPRESSED)
      LZ4_decompress_safe((const char *)rr.buf, (char *)cbuf, (int)rr.storedLen, (int)SCRATCH);
#else
    if (rr.flags & FLAG_COMPRESSED) { napi_value r; napi_create_int32(env, -1, &r); return r; }
#endif
    n = (int32_t)rr.rawLen;
  }
  napi_value out; napi_create_int32(env, n, &out); return out;
}

static napi_value Has(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_STORE(nullptr) char key[KEY_MAX + 1]; size_t klen = 0;
  napi_value r;
  if (!readKey(env, argv[0], key, &klen)) { napi_get_boolean(env, false, &r); return r; }
  napi_get_boolean(env, storeHas(g, (const uint8_t *)key, (uint16_t)klen, nowRelMs(g)), &r);
  return r;
}

static napi_value Del(napi_env env, napi_callback_info info) {
  ARG(2)
  NEED_STORE(nullptr) char key[KEY_MAX + 1]; size_t klen = 0;
  NEED_WRITABLE(nullptr)
  napi_value r;
  if (!readKey(env, argv[0], key, &klen)) { napi_get_boolean(env, false, &r); return r; }
  int32_t writerId = 0; if (argc > 1) napi_get_value_int32(env, argv[1], &writerId);
  napi_get_boolean(env, storeDelete(g, (const uint8_t *)key, (uint16_t)klen, (uint16_t)writerId), &r);
  return r;
}

// sweepExpired(cursorSlot, maxSlots) -> { removed, cursor, done }
// Expiry was lazy only, so an expired entry held its index slot and arena bytes
// until the tail happened to reach it, and `live` drifted high. The primary is
// the sole writer, so it can reclaim them directly.
static napi_value SweepExpired(napi_env env, napi_callback_info info) {
  ARG(2)
  NEED_STORE(nullptr)
  NEED_WRITABLE(nullptr)
  double cur = 0; int32_t maxSlots = 0;
  napi_get_value_double(env, argv[0], &cur);
  napi_get_value_int32(env, argv[1], &maxSlots);
  Header *h = g.h;
  uint32_t now = nowRelMs(g);
  uint64_t i = (uint64_t)cur, scanned = 0, removed = 0;
  for (; i < h->indexSlots && (int32_t)scanned < maxSlots; i++, scanned++) {
    uint64_t hv = g.idx[i].hash.load(std::memory_order_relaxed);
    if (hv == HASH_EMPTY || hv == HASH_TOMB) continue;
    uint64_t pos = g.idx[i].off.load(std::memory_order_relaxed);
    Entry *e = g.entryAt(pos);
    // tcExpired, not `exp > now`: this is the fifth expiry comparison and it
    // was the one left behind when the others were made wrap-aware. A plain
    // compare deletes every entry whose expiry crosses the uint32 wrap while it
    // is still valid -- once per 49.7 days of primary uptime, a whole TTL
    // window of live data swept away.
    if (!tcExpired(e->expiresAt, now)) continue;
    uint32_t bsz = e->blockSize; uint64_t eh = e->hash;
    indexRemove(g, i);
    h->live--; h->liveBytes -= bsz; h->evictions++;
    ringAppend(g, eh, ++h->inserts, 0);      // workers must drop their L1 copy
    removed++;
    i--;                                     // backward shift may refill this slot
  }
  napi_value o; napi_create_object(env, &o);
  put(env, o, "removed", (double)removed);
  put(env, o, "cursor", (double)(i >= h->indexSlots ? 0 : i));
  napi_value done; napi_get_boolean(env, i >= h->indexSlots, &done);
  napi_set_named_property(env, o, "done", done);
  return o;
}

// The primary stamps its liveness here; workers use it to notice a dead primary
// and stop trusting the arena. The field existed but nothing ever wrote it.
static napi_value Heartbeat(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  NEED_WRITABLE(nullptr)
  g.h->heartbeatNs.store(ticksNs(), std::memory_order_release);
  return nullptr;
}
static napi_value HeartbeatAgeMs(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  uint64_t hb = g.h->heartbeatNs.load(std::memory_order_acquire);
  napi_value r;
  // -1 and -2 mean DIFFERENT things and the caller must be able to tell them
  // apart. Both used to be -1, and the JS side treated -1 as "dead" -- so
  // `maintenance: false`, a public documented option, permanently degraded every
  // worker on its second read, with the message "heartbeat is in the future".
  //   -1  never stamped: there is no liveness signal at all, which is not the
  //       same as evidence of death.
  //   -2  stamped in the future: ticks never run backwards, so this is a corrupt
  //       or foreign header and is genuinely not healthy.
  if (!hb) { napi_create_double(env, -1, &r); return r; }
  uint64_t now = ticksNs();
  napi_create_double(env, now >= hb ? (double)((now - hb) / 1000000ull) : -2, &r);
  return r;
}

// The heartbeat stamp itself. Recovery needs to see it ADVANCE across two polls:
// a plausible-looking age proves nothing, because a dead primary's last stamp
// keeps looking recent until staleMs elapses, and a freshly created arena starts
// with a fresh one.
static napi_value HeartbeatRaw(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  napi_value r;
  napi_create_double(env, (double)g.h->heartbeatNs.load(std::memory_order_acquire), &r);
  return r;
}

// Identity of this arena creation, as hex. Distinguishes "the same primary came
// back" from "a different primary owns this name now".
static napi_value ArenaId(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  char buf[24];
  snprintf(buf, sizeof buf, "%llx", (unsigned long long)g.h->arenaId);
  napi_value r; napi_create_string_latin1(env, buf, NAPI_AUTO_LENGTH, &r); return r;
}

// Unmap without unlinking. A degraded worker MUST let go: on Windows
// CreateFileMappingA fails with ERROR_ALREADY_EXISTS while any process still
// holds a handle, so a worker clinging to a dead arena prevents a new primary
// from ever starting. It costs nothing - a degraded worker serves L1 only and
// never touches the arena.
static napi_value Detach(napi_env env, napi_callback_info) {
  if (g.base && !g.writable) g.destroy();
  return nullptr;
}

// scanKeys(cursorSlot, max) -> { keys: [...], cursor }
// Enumeration is possible because entries store the key text - decision 3 named
// this as a benefit of verifying keys, but nothing ever exposed it, so there
// was no way to see what a cache actually held.
static napi_value ScanKeys(napi_env env, napi_callback_info info) {
  ARG(2)
  NEED_STORE(nullptr)
  int32_t max = 0; double cur = 0;
  napi_get_value_double(env, argv[0], &cur);
  napi_get_value_int32(env, argv[1], &max);
  Header *h = g.h;
  napi_value arr; napi_create_array(env, &arr);
  uint32_t n = 0;
  uint64_t i = (uint64_t)cur;
  // Every handle created here lives until this callback returns, and `max` is
  // caller-controlled, so cap it rather than letting one call pin an unbounded
  // number of V8 handles. Callers page with the returned cursor anyway.
  if (max <= 0 || max > 10000) max = 10000;
  char kbuf[KEY_MAX];
  for (; i < h->indexSlots && (int32_t)n < max; i++) {
    uint64_t hv = g.idx[i].hash.load(std::memory_order_acquire);
    if (hv == HASH_EMPTY || hv == HASH_TOMB) continue;
    uint64_t pos = g.idx[i].off.load(std::memory_order_acquire);
    Entry *e = g.entryAt(pos);
    // COPY, then verify -- the same protocol storeGet uses. This used to read
    // keyLen and the key bytes directly after checking liveness BEFORE the read,
    // so a record evicted and overwritten in between handed back whatever now
    // occupied those bytes: measured 16 garbage keys (value bytes returned as
    // key text) out of 154M enumerated under concurrent primary writes.
    uint32_t s1 = e->seq.load(std::memory_order_acquire);
    if (s1 & 1u) continue;                       // write in progress
    uint16_t kl = e->keyLen;
    uint64_t ehash = e->hash;
    if (kl == 0 || kl > KEY_MAX) continue;
    memcpy(kbuf, g.keyOf(e), kl);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (e->seq.load(std::memory_order_acquire) != s1) continue;   // torn
    // Liveness AFTER the copy: a monotonic position proves the record was not
    // reused underneath us, which a seqlock alone cannot (see decision 19).
    if (h->tailPub.load(std::memory_order_acquire) > pos) continue;
    if (ehash != hv) continue;                   // slot no longer points here
    napi_value k;
    if (napi_create_string_utf8(env, kbuf, kl, &k) != napi_ok) continue;
    napi_set_element(env, arr, n++, k);
  }
  napi_value o; napi_create_object(env, &o);
  napi_set_named_property(env, o, "keys", arr);
  put(env, o, "cursor", (double)i);
  napi_value done; napi_get_boolean(env, i >= h->indexSlots, &done);
  napi_set_named_property(env, o, "done", done);
  return o;
}

static napi_value ClearAll(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_STORE(nullptr)
  NEED_WRITABLE(nullptr)
  int32_t writerId = 0; if (argc > 0) napi_get_value_int32(env, argv[0], &writerId);
  storeClear(g, (uint16_t)writerId);
  return nullptr;
}

// probe(key) -> int  (index probe + memcmp only; no value copy, no decompress)
static napi_value Probe(napi_env env, napi_callback_info info) {
  ARG(1)
  NEED_STORE(nullptr)
  char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  uint64_t hash = rapidhash_withSeed(key, klen, 0);
  if (hash <= HASH_TOMB) hash += 2;
  int64_t slot = g.findSlot(hash, (const uint8_t *)key, (uint16_t)klen);
  napi_value out; napi_create_int32(env, (int32_t)slot, &out); return out;
}

static void put(napi_env env, napi_value o, const char *k, double v) {
  napi_value n; napi_create_double(env, v, &n); napi_set_named_property(env, o, k, n);
}

// Returns a freshly allocated flat string. A V8 SlicedString keeps its parent
// alive, so caching a 100-byte substring of a 4MB document retains all 4MB -
// measured. Round-tripping through a buffer produces a SeqString that owns only
// its own characters, making the byte accounting exact.
// A V8 string is one-byte only if it is pure ASCII as far as we can tell from
// Node-API: latin1 chars 128-255 are one byte in V8 but two in UTF-8, so
// utf8Len == charLen implies ASCII, and anything else is treated as two-byte.
static bool strInfo(napi_env env, napi_value v, size_t *charLen, size_t *utf8Len) {
  if (napi_get_value_string_utf8(env, v, nullptr, 0, utf8Len) != napi_ok) return false;
  if (napi_get_value_string_utf16(env, v, nullptr, 0, charLen) != napi_ok) return false;
  return true;
}

static napi_value Flatten(napi_env env, napi_callback_info info) {
  ARG(1)
  if (!scratch) { scratch = (uint8_t *)malloc(SCRATCH); cbuf = (uint8_t *)malloc(SCRATCH); }
  size_t charLen = 0, utf8Len = 0;
  if (!strInfo(env, argv[0], &charLen, &utf8Len)) return argv[0];
  if (charLen == 0) return argv[0];
  napi_value out;
  if (utf8Len == charLen) {                       // ASCII: one-byte round trip
    if (charLen + 1 > SCRATCH) return argv[0];
    size_t got = 0;
    napi_get_value_string_latin1(env, argv[0], (char *)scratch, SCRATCH, &got);
    if (napi_create_string_latin1(env, (const char *)scratch, got, &out) != napi_ok) return argv[0];
  } else {                                        // two-byte round trip
    if ((charLen + 1) * 2 > SCRATCH) return argv[0];
    size_t got = 0;
    napi_get_value_string_utf16(env, argv[0], (char16_t *)scratch, SCRATCH / 2, &got);
    if (napi_create_string_utf16(env, (const char16_t *)scratch, got, &out) != napi_ok) return argv[0];
  }
  return out;
}

// Exact V8 heap cost of a primitive. Verified against measured heapUsed to
// within 1% for flat strings; Smis and singletons genuinely cost nothing.
static napi_value PrimBytes(napi_env env, napi_callback_info info) {
  ARG(1)
  napi_valuetype t; napi_typeof(env, argv[0], &t);
  double bytes = 0;
  if (t == napi_string) {
    size_t charLen = 0, utf8Len = 0;
    if (strInfo(env, argv[0], &charLen, &utf8Len)) {
      size_t body = (utf8Len == charLen) ? charLen : charLen * 2;
      bytes = (double)((16 + body + 7) & ~(size_t)7);
    }
  } else if (t == napi_number) {
    double d; napi_get_value_double(env, argv[0], &d);
    bool smi = d == (double)(int32_t)d && d >= -1073741824.0 && d <= 1073741823.0;
    bytes = smi ? 0 : 16;
  } else if (t == napi_object) {
    bool isBuf = false, isTa = false, isAb = false;
    napi_is_buffer(env, argv[0], &isBuf);
    napi_is_typedarray(env, argv[0], &isTa);
    napi_is_arraybuffer(env, argv[0], &isAb);
    if (isBuf || isTa || isAb) {
      size_t len = 0; void *d = nullptr;
      if (isBuf) napi_get_buffer_info(env, argv[0], &d, &len);
      else if (isAb) napi_get_arraybuffer_info(env, argv[0], &d, &len);
      else { napi_typedarray_type tt; size_t n = 0; napi_value ab; size_t off = 0;
             napi_get_typedarray_info(env, argv[0], &tt, &n, &d, &ab, &off); len = n; }
      bytes = (double)(len + 96);      // ArrayBuffer + view headers
    }
  } else if (t == napi_bigint) {
    size_t words = 0;
    napi_get_value_bigint_words(env, argv[0], nullptr, &words, nullptr);
    bytes = (double)(16 + words * 8);
  }
  napi_value r; napi_create_double(env, bytes, &r); return r;
}

// hashKey(key) -> hex string, so JS can map ring records back to L1 entries
static napi_value HashKey(napi_env env, napi_callback_info info) {
  ARG(1) char key[KEY_MAX + 1]; size_t klen = 0;
  if (!readKey(env, argv[0], key, &klen)) return nullptr;
  uint64_t hv = rapidhash_withSeed(key, klen, 0);
  if (hv <= HASH_TOMB) hv += 2;
  char buf[24]; snprintf(buf, sizeof(buf), "%llx", (unsigned long long)hv);
  napi_value r; napi_create_string_latin1(env, buf, NAPI_AUTO_LENGTH, &r); return r;
}

// ringRead(cursor, max) -> { head, wrapped, hashes: [hex...] }
// Workers drain this to invalidate their L1. `wrapped` means the worker fell so
// far behind that records were lost, and it must flush L1 wholesale.
// Cheap head read: one relaxed load, no allocation. The drain fast path.
static napi_value RingHead(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  napi_value r;
  napi_create_double(env, (double)g.h->ringHead.load(std::memory_order_acquire), &r);
  return r;
}

static napi_value RingRead(napi_env env, napi_callback_info info) {
  ARG(2)
  NEED_STORE(nullptr)
  double cd; int32_t maxN;
  napi_get_value_double(env, argv[0], &cd);
  napi_get_value_int32(env, argv[1], &maxN);
  uint64_t cursor = (uint64_t)cd;
  Header *h = g.h;
  uint64_t head = h->ringHead.load(std::memory_order_acquire);
  // `>=`, not `>`: ringAppend writes the record at `head & mask` BEFORE
  // publishing head+1, so the slot exactly ringCap behind the head is the one
  // being overwritten right now. Treating it as readable returns a torn record.
  bool wrapped = (head - cursor) >= h->ringCap;
  if (wrapped) cursor = head > h->ringCap ? head - h->ringCap : 0;

  napi_value arr; napi_create_array(env, &arr);
  napi_value writers; napi_create_array(env, &writers);
  uint32_t n = 0;
  for (uint64_t p = cursor; p < head && (int32_t)n < maxN; p++, n++) {
    RingRec *r = &g.ring[p & (h->ringCap - 1)];
    char buf[24]; snprintf(buf, sizeof(buf), "%llx", (unsigned long long)r->hash);
    napi_value s; napi_create_string_latin1(env, buf, NAPI_AUTO_LENGTH, &s);
    napi_set_element(env, arr, n, s);
    napi_value w; napi_create_int32(env, r->writerId, &w);
    napi_set_element(env, writers, n, w);
  }
  napi_value o; napi_create_object(env, &o);
  put(env, o, "head", (double)(cursor + n));
  put(env, o, "ringHead", (double)head);
  // The writer can lap us DURING the loop above -- an 8192-record ring is only a
  // few milliseconds of primary writes, and one preemption inside this call is
  // enough. Without re-checking, the reader returns records from a newer lap
  // while believing it drained the older ones, never reports `wrapped`, and the
  // worker's L1 keeps stale values with nothing left to correct them.
  {
    uint64_t head2 = h->ringHead.load(std::memory_order_acquire);
    if (!wrapped && (head2 - cursor) >= h->ringCap) wrapped = true;
  }
  napi_value w; napi_get_boolean(env, wrapped, &w);
  napi_set_named_property(env, o, "wrapped", w);
  napi_set_named_property(env, o, "hashes", arr);
  napi_set_named_property(env, o, "writers", writers);
  return o;
}

static napi_value ClearHints(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  for (uint64_t i = 0; i < g.h->indexSlots; i++) g.hints[i].store(0, std::memory_order_relaxed);
  return nullptr;
}
static napi_value HintsSet(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  uint64_t n = 0;
  for (uint64_t i = 0; i < g.h->indexSlots; i++) if (g.hints[i].load(std::memory_order_relaxed)) n++;
  napi_value r; napi_create_double(env, (double)n, &r); return r;
}
static napi_value SetBackwardShift(napi_env env, napi_callback_info info) {
  ARG(1) bool v; napi_get_value_bool(env, argv[0], &v); g_backwardShift = v; return nullptr;
}
static napi_value SetSecondChanceBudget(napi_env env, napi_callback_info info) {
  ARG(1) int32_t v; napi_get_value_int32(env, argv[0], &v); g_secondChanceBudget = v; return nullptr;
}
// How far the ring head has run ahead, and its capacity: enough to tell whether
// a worker would have lost records.
static napi_value RingStats(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  napi_value o; napi_create_object(env, &o);
  put(env, o, "head", (double)g.h->ringHead.load(std::memory_order_acquire));
  put(env, o, "capacity", (double)g.h->ringCap);
  return o;
}
static napi_value SetSuppressRefBit(napi_env env, napi_callback_info info) {
  ARG(1) bool v; napi_get_value_bool(env, argv[0], &v); g_suppressRefBit = v; return nullptr;
}

// missProbe(key) -> probe cost for a key that is NOT present
// Deliberately writes through the mapping, to prove a read-only worker faults.
static napi_value Poke(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  volatile uint8_t *p = (volatile uint8_t *)g.base + g.h->dataOff;
  *p = 0x42;
  napi_value r; napi_get_boolean(env, true, &r); return r;
}

// Largest value the arena can accept, so a worker can reject locally instead of
// queuing something the primary will silently drop.
static napi_value EpochMs(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  // Ticks, not wall clock. Callers must treat this as opaque: it is only
  // meaningful relative to another ticksNs() reading in this boot.
  napi_value r; napi_create_double(env, (double)(g.h->epochTicksNs / 1000000ull), &r); return r;
}
// Milliseconds REMAINING on the value the last get() returned, or 0 for no TTL.
//
// This used to hand back the raw arena-relative expiresAt, which the JS layer
// added to the arena epoch to get an absolute wall-clock time. That only worked
// while the epoch was wall clock; it is a tick reading now, and the two domains
// cannot be mixed. A remaining duration is domain-independent, so the caller
// adds it to whatever clock it keeps L1 expiry in.
static napi_value LastTtlRemainingMs(napi_env env, napi_callback_info) {
  napi_value r;
  double v = 0;
  if (g_lastExpiresAt && g.base && g.h) {
    int32_t d = (int32_t)(g_lastExpiresAt - nowRelMs(g));
    v = d > 0 ? (double)d : 1;      // already expiring: 1ms, never 0 (0 means immortal)
  }
  napi_create_double(env, v, &r); return r;
}
static napi_value KeyMaxBytes(napi_env env, napi_callback_info) {
  napi_value r; napi_create_double(env, (double)KEY_MAX, &r); return r;
}

static napi_value MaxValueBytes(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  // Bounded by the log allocator AND by the native scratch buffer, which the
  // JS-side check previously ignored.
  double byArena = (double)(g.h->dataBytes / 2) - (double)sizeof(Entry) - (double)KEY_MAX;
  double byScratch = (double)SCRATCH - 1024.0;
  double n = byArena < byScratch ? byArena : byScratch;
  napi_value r; napi_create_double(env, n < 0 ? 0 : n, &r); return r;
}

static napi_value Stats(napi_env env, napi_callback_info) {
  NEED_STORE(nullptr)
  napi_value o; napi_create_object(env, &o);
  Header *h = g.h;
  put(env, o, "mode", h->mode);
  put(env, o, "live", (double)h->live);
  put(env, o, "inserts", (double)h->inserts);
  put(env, o, "evictions", (double)h->evictions);
  put(env, o, "reappends", (double)h->reappends);
  put(env, o, "reappendSkippedNoRoom", (double)h->reappendSkippedNoRoom);
  put(env, o, "dropped", (double)h->dropped);
  put(env, o, "tailAdvances", (double)h->tailAdvances);
  put(env, o, "tailLive", (double)h->tailLive);
  put(env, o, "liveBytes", (double)h->liveBytes);
  put(env, o, "dataBytes", (double)h->dataBytes);
  put(env, o, "logHead", (double)h->logHead);
  put(env, o, "logTail", (double)h->logTail);
  put(env, o, "indexSlots", (double)h->indexSlots);
  put(env, o, "ringHead", (double)h->ringHead.load());
  return o;
}
static napi_value Destroy(napi_env env, napi_callback_info) {
  g.destroy(); g.h = nullptr; g.idx = nullptr; g.data = nullptr; g.hints = nullptr;
  return nullptr;
}

#define FN(name, fn) { napi_value f; napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, nullptr, &f); \
                       napi_set_named_property(env, exports, name, f); }
static napi_value Init(napi_env env, napi_value exports) {
  // Hooks prefixed __unsafe MUTATE global algorithm state or write directly into
  // the arena, and exist only so the tests can force conditions that cannot be
  // reached through the public API (a read-only mapping faulting, eviction with
  // reference bits suppressed, a chosen second-chance budget). They are not
  // reachable from the package's `exports`: the public class no longer hands out
  // the addon, so a consumer would have to reach into src/ deliberately.
  FN("create", Create) FN("attach", Attach) FN("set", Set) FN("get", Get)
  FN("submitCreate", SubmitCreate) FN("submitOpen", SubmitOpen)
  FN("submitClaim", SubmitClaim) FN("submitSet", SubmitSet)
  FN("submitDel", SubmitDel) FN("submitDrain", SubmitDrain)
  FN("submitPending", SubmitPending) FN("submitStats", SubmitStats)
  FN("submitDestroy", SubmitDestroy) FN("submitRelease", SubmitRelease)
  FN("submitMaxValue", SubmitMaxValue)
  FN("getLen", GetLen) FN("has", Has) FN("del", Del) FN("clearAll", ClearAll) FN("scanKeys", ScanKeys) FN("sweepExpired", SweepExpired) FN("heartbeat", Heartbeat) FN("heartbeatAgeMs", HeartbeatAgeMs) FN("probe", Probe) FN("stats", Stats) FN("maxValueBytes", MaxValueBytes) FN("lastTtlRemainingMs", LastTtlRemainingMs) FN("epochMs", EpochMs) FN("heartbeatRaw", HeartbeatRaw)
  FN("arenaId", ArenaId) FN("detach", Detach) FN("keyMaxBytes", KeyMaxBytes)
  FN("destroy", Destroy) FN("__unsafePokeArena", Poke)
  FN("__unsafeSuppressRefBit", SetSuppressRefBit) FN("__unsafeSecondChanceBudget", SetSecondChanceBudget) FN("ringStats", RingStats) FN("__unsafeBackwardShift", SetBackwardShift) FN("__unsafeClearHints", ClearHints) FN("hashKey", HashKey) FN("flatten", Flatten) FN("primBytes", PrimBytes) FN("ringRead", RingRead) FN("ringHead", RingHead) FN("hintsSet", HintsSet) FN("setCompressMin", SetCompressMin) FN("hasLz4", HasLz4)
  return exports;
}
NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
