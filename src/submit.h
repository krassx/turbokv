// Worker -> primary write submission over shared memory.
//
// Replaces the cluster IPC hot path. process.send() was never a throughput
// problem in the way first assumed (the channel carries 439 MB/s under JSON,
// 1738 MB/s under 'advanced'), but every send SYNCHRONOUSLY freezes the sending
// worker's event loop while V8 serializes the batch: measured 0.49ms p50 and
// 1.15ms p99 for a ~525KB batch. Everything else that worker is doing waits
// behind that, including the application's own cluster messages, because
// process.send is the shared channel and not turbokv's private pipe.
//
// Here a write is a memcpy into a ring the primary already has mapped.
//
// ONE SPSC RING PER WORKER, not a single MPSC ring. Three reasons:
//   - no atomic contention between producers on a shared head
//   - no head-of-line blocking: a producer preempted between reserving space
//     and publishing would stall every other producer's records behind it
//   - per-worker accounting and liveness (a dead worker's ring is skippable)
//
// Positions are MONOTONIC byte counts, never wrapped indices - the same idiom
// the index already uses for log positions (decision 19). head - tail is then
// unambiguously the number of live bytes, with no empty/full ambiguity.
//
// TRUST: workers map this segment READ-WRITE (the arena itself stays read-only
// to them, which is the isolation that matters). A buggy or hostile worker can
// therefore write nonsense anywhere in it -- INCLUDING the SubmitHeader at
// offset 0. So the consumer must never re-read geometry or validation bounds
// from shared memory: it snapshots them once into Submit's own fields and uses
// those. Record fields are bounds-checked against that snapshot before use, and
// head-tail is clamped to the ring capacity, which a producer can never
// legitimately exceed. A bad worker can still corrupt its own ring and lose its
// own writes; it must not be able to fault, hang, or misdirect the primary.
#pragma once
#include <atomic>
#include <stdint.h>
#include <string.h>
#include "platform.h"

static const uint32_t TCS_MAGIC  = 0x54435331;   // "TCS1"
static const uint32_t TCS_LAYOUT = 2;   // 2: namespace id removed from the record header

enum { SUBMIT_OP_SET = 1, SUBMIT_OP_DEL = 2, SUBMIT_OP_SKIP = 3 };

// 24 bytes, 8-byte aligned; key bytes then value bytes follow inline.
struct SubmitRec {
  uint32_t len;        // total record size incl. this header, 8-byte aligned
  uint8_t  op;
  uint8_t  flags;      // value type tag, same FLAG_* bits the arena uses
  // The namespace id lived here (TCS_LAYOUT 1). It is named rather than left as
  // an anonymous hole so every byte of a record is written: this segment is
  // shared and reused lap after lap, so an unwritten hole carries whatever the
  // previous record put there. Nothing reads it, but leaving it uninitialised
  // is what MSan reports and what a future field would inherit.
  uint16_t reserved2;
  uint32_t keyLen;
  uint32_t valLen;
  uint32_t ttlMs;      // 0 = no expiry
  uint32_t reserved;
};
// Load-bearing: submitPush reserves and the consumer steps by exactly this many
// bytes, submitGapAt derives the implicit wrap gap from it, and submitValidate
// bounds a record's payload against it. A field added here is a TCS_LAYOUT bump.
static_assert(sizeof(SubmitRec) == 24, "SubmitRec is the ring's record stride: changing it is a TCS_LAYOUT change");

// One per worker slot. Padded so head and tail never share a cache line: they
// are written by different processes on different cores, and false sharing here
// would cost more than the whole design saves.
struct SubmitRing {
  alignas(64) std::atomic<uint64_t> head;     // producer publishes here
  alignas(64) std::atomic<uint64_t> tail;     // consumer publishes here
  alignas(64) std::atomic<uint32_t> owner;    // pid of the claiming worker, 0 = free
  std::atomic<uint32_t> needsWake;            // consumer is about to sleep
  std::atomic<uint64_t> shed;                 // records the producer could not fit
  std::atomic<uint64_t> pushed;
  std::atomic<uint64_t> applied;
  std::atomic<uint32_t> corrupt;              // records the consumer rejected
};

struct SubmitHeader {
  uint32_t magic, layout;
  uint32_t ringCount;
  uint32_t ringBytes;      // power of two, per-ring data capacity
  uint64_t dataOff;        // byte offset of ring 0's data from the segment base
  uint64_t ringsOff;       // byte offset of the SubmitRing array
  uint32_t maxKey, maxVal; // validation bounds, set by the primary at create
  uint64_t reserved[8];
};

struct Submit {
  void*  base = nullptr;
  size_t bytes = 0;
  ShmHandle h{};
  SubmitHeader* hdr = nullptr;
  SubmitRing*   rings = nullptr;
  uint8_t*      data = nullptr;

  // TRUSTED geometry, private to this process. The SubmitHeader lives at offset
  // 0 of a segment every worker maps READ-WRITE, so a worker can rewrite it. If
  // the consumer re-reads ringCount/ringBytes/maxKey/maxVal from there on each
  // drain, then a worker setting ringCount huge walks the primary off the end of
  // the mapping, a non-power-of-two ringBytes breaks the `off & (cap-1)` mask,
  // and huge maxKey/maxVal disable validation entirely. Read the header exactly
  // once -- at create (from our own arguments) or at open (from a primary we
  // trust) -- and use these copies everywhere afterwards.
  uint32_t ringCount = 0, ringBytes = 0, maxKey = 0, maxVal = 0;

  SubmitRing* ring(uint32_t i) const { return &rings[i]; }
  uint8_t* ringData(uint32_t i) const { return data + (size_t)i * ringBytes; }

  static size_t sizeFor(uint32_t ringCount, uint32_t ringBytes) {
    size_t off = sizeof(SubmitHeader);
    off = (off + 63) & ~(size_t)63;
    off += (size_t)ringCount * sizeof(SubmitRing);
    off = (off + 63) & ~(size_t)63;
    return off + (size_t)ringCount * ringBytes;
  }

  // Derive every offset from the trusted geometry, never from header fields:
  // dataOff and ringsOff live in worker-writable memory too. They remain in the
  // header for debuggability and layout versioning, but nothing computes a
  // pointer from them.
  void wire() {
    hdr = (SubmitHeader*)base;
    size_t off = (sizeof(SubmitHeader) + 63) & ~(size_t)63;
    rings = (SubmitRing*)((uint8_t*)base + off);
    off += (size_t)ringCount * sizeof(SubmitRing);
    off = (off + 63) & ~(size_t)63;
    data = (uint8_t*)base + off;
  }

  bool create(const char* name, uint32_t ringCount_, uint32_t ringBytes_,
              uint32_t maxKey_, uint32_t maxVal_) {
    if (ringCount_ == 0 || ringBytes_ == 0 || (ringBytes_ & (ringBytes_ - 1))) return false;
    if (ringBytes_ < 4096 || ringCount_ > 4096) return false;
    if (base) close();          // same guard as open(); a second create replaces the first
    bytes = sizeFor(ringCount_, ringBytes_);
    base = shmCreate(name, bytes, &h);
    if (!base) return false;
    memset(base, 0, bytes);
    SubmitHeader* sh = (SubmitHeader*)base;
    sh->magic = TCS_MAGIC; sh->layout = TCS_LAYOUT;
    sh->ringCount = ringCount_; sh->ringBytes = ringBytes_;
    sh->maxKey = maxKey_; sh->maxVal = maxVal_;
    size_t off = (sizeof(SubmitHeader) + 63) & ~(size_t)63;
    off += (size_t)ringCount_ * sizeof(SubmitRing);
    off = (off + 63) & ~(size_t)63;
    sh->dataOff = off;
    sh->ringsOff = (sizeof(SubmitHeader) + 63) & ~(size_t)63;
    ringCount = ringCount_; ringBytes = ringBytes_; maxKey = maxKey_; maxVal = maxVal_;
    wire();
    return true;
  }

  bool open(const char* name) {
    // Workers need WRITE here; the arena stays read-only for them.
    size_t probe = sizeof(SubmitHeader);
    void* p = shmOpenRW(name, probe, false, &h);
    if (!p) return false;
    SubmitHeader probeHdr = *(SubmitHeader*)p;
    shmClose(p, probe, &h);
    if (probeHdr.magic != TCS_MAGIC || probeHdr.layout != TCS_LAYOUT) return false;
    // Sanity-check even the primary's header: this is read once, and a corrupt
    // or stale segment must fail here rather than produce a bad mask later.
    if (probeHdr.ringCount == 0 || probeHdr.ringCount > 4096) return false;
    if (probeHdr.ringBytes < 4096 || (probeHdr.ringBytes & (probeHdr.ringBytes - 1))) return false;
    // Close any mapping we already hold. Overwriting `base` leaked the previous
    // one, and open() is called again on every recovery and on every second
    // cache in a worker -- measured six full copies (8MB -> 48MB) after five
    // re-opens, unbounded for an app that re-attaches repeatedly.
    if (base) { shmClose(base, bytes, &h); base = nullptr; }
    bytes = sizeFor(probeHdr.ringCount, probeHdr.ringBytes);
    base = shmOpenRW(name, bytes, false, &h);
    if (!base) return false;
    ringCount = probeHdr.ringCount; ringBytes = probeHdr.ringBytes;
    maxKey = probeHdr.maxKey; maxVal = probeHdr.maxVal;
    wire();
    return true;
  }

  void close() {
    if (base) { shmClose(base, bytes, &h); base = nullptr; }
  }
};

static inline uint32_t submitAlign8(uint32_t n) { return (n + 7u) & ~7u; }

// PRODUCER (worker). Returns false when the ring cannot take the record, which
// the caller reports as a shed write - never as an error, and never by blocking.
static inline bool submitPush(Submit& s, uint32_t idx, uint8_t op, uint8_t flags,
                              uint32_t ttlMs,
                              const void* key, uint32_t keyLen,
                              const void* val, uint32_t valLen) {
  SubmitRing* r = s.ring(idx);
  const uint32_t cap = s.ringBytes;
  const uint32_t need = submitAlign8((uint32_t)sizeof(SubmitRec) + keyLen + valLen);
  if (need > cap / 2) { r->shed.fetch_add(1, std::memory_order_relaxed); return false; }

  const uint64_t head = r->head.load(std::memory_order_relaxed);
  const uint64_t tail = r->tail.load(std::memory_order_acquire);
  uint64_t h = head;
  uint32_t off = (uint32_t)(h & (cap - 1));

  // A record never straddles the end of the buffer: pad the remainder with a
  // SKIP so the consumer can step over it. Simpler and cheaper to validate than
  // a split memcpy, at the cost of at most `need` wasted bytes per wrap.
  // Two reasons to skip to the start: the record would straddle the end, or the
  // remainder is too small to even hold a SKIP header. Records are 8-aligned but
  // the header is 24 bytes, so a remainder of 8 or 16 is reachable -- writing a
  // SKIP there overran the ring and produced a record the consumer rejected,
  // wedging that ring permanently. When the remainder cannot hold a header the
  // gap is left IMPLICIT: the consumer derives it from the same offset with the
  // same rule, so nothing needs to be written to describe it.
  const uint32_t remain = cap - off;
  uint32_t pad = 0;
  if (remain < sizeof(SubmitRec) || off + need > cap) pad = remain;

  if ((uint64_t)(cap - (h - tail)) < (uint64_t)need + pad) {
    r->shed.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  uint8_t* base = s.ringData(idx);
  if (pad) {
    if (pad >= sizeof(SubmitRec)) {
      SubmitRec* skip = (SubmitRec*)(base + off);
      skip->len = pad; skip->op = SUBMIT_OP_SKIP; skip->flags = 0; skip->reserved2 = 0;
      skip->keyLen = 0; skip->valLen = 0; skip->ttlMs = 0; skip->reserved = 0;
    }
    // else: implicit gap, see submitGapAt
    h += pad;
    off = 0;
  }
  SubmitRec* rec = (SubmitRec*)(base + off);
  rec->len = need; rec->op = op; rec->flags = flags; rec->reserved2 = 0;
  rec->keyLen = keyLen; rec->valLen = valLen; rec->ttlMs = ttlMs; rec->reserved = 0;
  if (keyLen) memcpy(base + off + sizeof(SubmitRec), key, keyLen);
  if (valLen) memcpy(base + off + sizeof(SubmitRec) + keyLen, val, valLen);

  // Release: everything above must be visible before the consumer can see the
  // new head. This single store is what publishes the record.
  r->head.store(h + need, std::memory_order_release);
  r->pushed.fetch_add(1, std::memory_order_relaxed);
  return true;
}

// Bytes the consumer must step over without reading a record, because the
// producer could not place one there. Must mirror submitPush exactly.
static inline uint32_t submitGapAt(uint32_t off, uint32_t cap) {
  const uint32_t remain = cap - off;
  return remain < sizeof(SubmitRec) ? remain : 0;
}

// Every field is attacker-controlled. Reject rather than trust.
static inline bool submitValidate(const Submit& s, const SubmitRec* rec, uint64_t avail) {
  if (rec->len < sizeof(SubmitRec) || rec->len > avail) return false;
  if (rec->len & 7u) return false;
  if (rec->op == SUBMIT_OP_SKIP) return rec->keyLen == 0 && rec->valLen == 0;
  if (rec->op != SUBMIT_OP_SET && rec->op != SUBMIT_OP_DEL) return false;
  if (rec->keyLen == 0 || rec->keyLen > s.maxKey) return false;
  if (rec->valLen > s.maxVal) return false;
  uint64_t need = (uint64_t)sizeof(SubmitRec) + rec->keyLen + rec->valLen;
  if (need > rec->len) return false;                 // payload must fit the record
  if (submitAlign8((uint32_t)need) != rec->len) return false;
  return true;
}
