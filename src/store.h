// turbokv L2 arena.
//
// One open-addressed index over a circular append-only log that evicts from the
// tail, with a bounded second-chance re-append (MODE_LOG2). A size-class slab
// allocator and a plain log were measured against it and removed in decision 49;
// the mode field survives so a future allocator can be added without a layout
// change, but LOG2 is the only value create() accepts.
//
// The primary is the sole writer; workers map the segment PROT_READ and use the
// per-entry seqlock to detect torn reads. This prototype exercises both roles.
#pragma once
#include <atomic>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "platform.h"
#include "vendor/rapidhash.h"

static const uint32_t TC_MAGIC = 0x54430001;
static const uint32_t TC_LAYOUT = 6;   // 2: BigInt words at 8; 3: tick epoch + arenaId; 4: data region no longer power-of-two; 5: slab state out of Header; 6: namespace table removed
static const uint32_t FEATURE_LZ4 = 1;
static const uint64_t HASH_EMPTY = 0;
static const uint64_t HASH_TOMB  = 1;
// Ring sentinel: 'drop your entire L1', used by clearAll.
static const uint64_t RING_FLUSH_ALL = 0xFFFFFFFFFFFFFFFFull;

enum { MODE_LOG2 = 2 };   // log + bounded second-chance re-append. Values 0/1 were
                          // SLAB and LOG; they are retired, not reusable.

// Value type travels WITH the bytes, so a worker reading the arena directly
// reconstructs the right JS type. Without this, non-string primitives lived
// only in L1: lost on eviction and invisible to other processes.
enum { FLAG_COMPRESSED = 1, FLAG_STRING = 2, FLAG_LATIN1 = 4,
       FLAG_NUMBER = 8, FLAG_BOOL = 16, FLAG_NULL = 32, FLAG_BIGINT = 64,
       FLAG_BINARY = 128 };   // Buffer / TypedArray / ArrayBuffer / DataView

// 40 bytes, 8-byte aligned. Key bytes then value bytes follow inline.
struct Entry {
  std::atomic<uint32_t> seq;   // even = stable, odd = write in progress
  uint32_t slot;               // owning index slot, so eviction can unlink
  uint64_t hash;
  uint32_t version;
  uint32_t expiresAt;
  uint32_t rawLen;
  uint32_t storedLen;
  uint32_t blockSize;          // total bytes incl. header (slab class size, or log record size)
  uint16_t keyLen;
  uint8_t  flags;
};
// Load-bearing, not cosmetic: keyOf/valOf place the key and value at
// sizeof(Entry), logGapAt decides a wrap remainder against it, and logAlloc
// sizes every block from it -- so a field added here silently moves every
// offset in the data region AND changes what an existing arena means. Only the
// tail padding after `flags` keeps this at 40; adding a field is a TC_LAYOUT
// bump, and this assert is what forces that conversation.
static_assert(sizeof(Entry) == 40, "Entry is the data region's stride: changing it is a TC_LAYOUT change");

struct IndexSlot {
  std::atomic<uint64_t> hash;  // 0 empty, 1 tombstone, else hash
  // MONOTONIC log position of the entry, not a physical offset. Physical
  // address is pos % dataBytes. Because the position never wraps, a reader
  // can prove a record has not been evicted by checking logTail <= pos, which a
  // physical offset cannot express (offsets are reused every time the log wraps).
  std::atomic<uint64_t> off;
};

struct RingRec { uint64_t hash; uint32_t version; uint16_t writerId; uint16_t _pad; };

// Index load factor above which an insert evicts first, so the index can never
// saturate before the data region does (which silently failed inserts before).
static const double MAX_LOAD = 0.75;
static const uint64_t MIN_DATA_BYTES = 1u << 16;

struct Header {
  // magic is ATOMIC and published LAST. create() used to write it first and the
  // geometry after, so a reader attaching in that window saw a header that
  // validated while indexSlots and dataBytes were still zero -- bind() then
  // computed pointers from them and probing used a mask of 2^64-1. Nothing
  // caught it; the attach happened to be refused only because the hints segment
  // did not exist yet, which is an accident, not a check. The recovery poll
  // (a worker re-attaching by name every second) would hit that window
  // repeatedly and deliberately.
  std::atomic<uint32_t> magic;
  uint32_t layout;
  uint8_t  mode;
  uint8_t  _pad[7];
  uint64_t totalBytes;
  uint64_t indexOff;  uint64_t indexSlots;   // power of two
  uint64_t dataOff;   uint64_t dataBytes;
  uint64_t ringOff;   uint64_t ringCap;      // power of two
  uint64_t hintsBytes;                       // size of the separate hints segment
  // Expiries are milliseconds from here, measured on the TICK clock, not wall
  // clock: an NTP step used to shift every TTL and could mark a healthy primary
  // dead in every worker simultaneously.
  uint64_t epochTicksNs;
  // Identity of THIS creation. A worker that lost its primary and re-attaches by
  // name needs to tell "the same primary resumed" (SIGSTOP, a long GC, laptop
  // sleep) from "a different primary now owns this name" - the recovery actions
  // are identical, but conflating them in the logs makes an outage unreadable.
  // epochMs alone could not do it: wall clock, and two creates can land in one ms.
  uint64_t arenaId;
  uint32_t primaryPid;
  uint32_t _pad2;
  std::atomic<uint64_t> tailPub;   // logTail, republished for readers
  std::atomic<uint64_t> ringHead;
  std::atomic<uint64_t> heartbeatNs;

  // log state
  uint64_t logHead, logTail;   // monotonic byte counters; % dataBytes to index

  // stats
  uint64_t inserts, evictions, live, liveBytes, allocBytes;
  uint64_t maxLive, indexEvictions, shiftMoves;

  uint64_t reappends, reappendSkippedNoRoom, dropped, tailAdvances, tailLive;
  // Set the first time a compressed entry is written. An attaching process
  // built without LZ4 cannot read those entries, so it refuses the arena
  // rather than silently reporting misses.
  uint32_t features;
  uint64_t readsSkippedNoLz4;
};

struct Store {
  uint8_t *base = nullptr;
  size_t   mapBytes = 0;
  bool     writable = false;
  Header  *h = nullptr;
  IndexSlot *idx = nullptr;
  uint8_t *data = nullptr;
  RingRec *ring = nullptr;
  // Reference bits: the ONLY thing a worker may write. Genuinely concurrent -
  // the primary clears and relocates them while every worker sets them - so
  // they are atomics. Relaxed is enough: a lost or stale hint costs eviction
  // quality, never correctness, and a relaxed byte access compiles to a plain
  // load/store. TSAN flagged the plain-uint8_t version as a data race.
  std::atomic<uint8_t> *hints = nullptr;
  std::atomic<uint8_t> *hintsMap = nullptr;
  size_t   hintsMapBytes = 0;
  char     name[64] = {0};
  char     hintsName[80] = {0};
  int      attachError = 0;   // 1 = arena needs LZ4 and this build lacks it
  ShmHandle baseHandle, hintsHandle;

  inline Entry *entryAt(uint64_t pos) const { return (Entry *)(data + (pos % h->dataBytes)); }
  inline uint8_t *keyOf(Entry *e) const { return (uint8_t *)e + sizeof(Entry); }
  inline uint8_t *valOf(Entry *e) const { return (uint8_t *)e + sizeof(Entry) + e->keyLen; }

  // ---- lifecycle -------------------------------------------------------
  bool create(const char *nm, uint64_t totalBytes, uint64_t indexSlots, uint8_t mode) {
    if (indexSlots < 16 || (indexSlots & (indexSlots - 1))) return false;   // power of two
    // Release whatever this process already owns. `g` is process-global, so a
    // second create() REPLACES the first -- and without this the old mapping
    // and its shm name both stayed for the life of the process. attachReadOnly
    // and Submit::open each carry this same guard already; create() was the one
    // that did not, and it is the one that also has a name to unlink. A suite
    // that creates an arena per test therefore accumulated every one of them,
    // which is why it needed gigabytes of /dev/shm to finish.
    //
    // Before the new name is written: destroy() unlinks `name`, which must
    // still be the OLD one.
    if (base) destroy();
    snprintf(name, sizeof(name), "%s", nm);
    base = (uint8_t *)shmCreate(nm, totalBytes, &baseHandle);
    if (!base) return false;
    mapBytes = totalBytes; writable = true;
    memset(base, 0, sizeof(Header));

    h = (Header *)base;
    h->magic.store(0, std::memory_order_relaxed);   // published at the very end
    h->layout = TC_LAYOUT; h->mode = mode;
    h->totalBytes = totalBytes;
    h->indexOff = (sizeof(Header) + 63) & ~63ull;
    h->indexSlots = indexSlots;
    uint64_t indexBytes = indexSlots * sizeof(IndexSlot);
    h->ringOff = h->indexOff + indexBytes;
    // Ring capacity is a TIME budget, not a count. A worker that fails to drain
    // before the head laps it must flush its entire L1, and the head is
    // appended only by the primary, so its rate is the primary's apply rate -
    // measured at ~650k records/s. 8192 records was therefore only ~12.7ms of
    // headroom, about one minor GC; a major GC would flush every worker's L1.
    // 65536 records is 1MB and ~100ms, which covers a major GC, capped at 4% of
    // the arena so a small arena does not spend itself on the ring.
    {
        uint64_t want = 65536, byArena = (uint64_t)(totalBytes * 0.04) / sizeof(RingRec);
        uint64_t cap = want < byArena ? want : byArena;
        uint64_t p = 8192; while (p * 2 <= cap && p < 262144) p *= 2;
        h->ringCap = p;
    }
    uint64_t ringBytes = h->ringCap * sizeof(RingRec);
    uint64_t pg = platformGranularity();
    h->hintsBytes = (indexSlots + pg - 1) & ~(pg - 1);      // one byte per index slot
    h->dataOff = (h->ringOff + ringBytes + 63) & ~63ull;

    // Metadata must actually fit, with room left for data. Without this check a
    // too-small segment underflows `totalBytes - dataOff` into a huge unsigned
    // value and create() hangs or scribbles past the mapping.
    if (h->dataOff + MIN_DATA_BYTES > totalBytes) {
      shmClose(base, totalBytes, &baseHandle); base = nullptr; shmUnlink(nm); return false;
    }
    // The data region takes everything that is left, 8-byte aligned.
    //
    // It used to be rounded DOWN to a power of two so the log could mask with
    // (dataBytes-1). That silently discarded up to half the arena: a 24MB, 26MB,
    // 28MB or 32MB request all yielded exactly 16MB of data, so capacity could
    // only be doubled, never tuned, and the sizing formulas in DESIGN 7 named
    // numbers nobody actually got. The log uses a modulo now. Measured on a
    // dependent chain that is +2.96ns per computation and roughly +9ns on an L2
    // read (1.8-3.4%); L1 hits do not touch it at all. The index and the
    // invalidation ring are still powers of two, because open addressing probes
    // with a mask and that one is on every lookup.
    uint64_t avail = (totalBytes - h->dataOff) & ~7ull;
    h->dataBytes = avail;

    h->logHead = 0; h->logTail = 0;
    h->tailPub.store(0, std::memory_order_relaxed);
    h->epochTicksNs = ticksNs();
    h->arenaId = h->epochTicksNs ^ ((uint64_t)platformPid() << 32) ^ nowNs();
    h->primaryPid = platformPid();
    h->maxLive = (uint64_t)(indexSlots * MAX_LOAD);
    if (!openHints(nm, true)) {
      // Returning false here used to leave `base` mapped and `h` set while idx
      // and data stayed null, so NEED_STORE passed and the first get()
      // dereferenced null. It also leaked the segment: nothing unlinked it, and
      // this is reachable without misuse -- macOS caps shm names at 31 chars and
      // the hints name appends ".h", so any name of 30+ chars fails HERE, after
      // the arena object already exists.
      shmClose(base, mapBytes, &baseHandle);
      base = nullptr; h = nullptr;
      shmUnlink(nm);
      return false;
    }
    bind();
    memset(idx, 0, indexBytes);
    memset(hints, 0, h->hintsBytes);
    // Everything above must be visible before any reader can validate this
    // header, so this store is the publication point.
    h->magic.store(TC_MAGIC, std::memory_order_release);
    return true;
  }

  bool attachReadOnly(const char *nm) {
    uint64_t sz = 0;
    // Same leak as Submit::open: re-attaching without releasing the previous
    // mapping strands it for the life of the process.
    if (base) { shmClose(base, mapBytes, &baseHandle); base = nullptr; h = nullptr; }
    base = (uint8_t *)shmOpenRead(nm, &baseHandle, &sz);
    if (!base) return false;
    mapBytes = (size_t)sz; writable = false;
    h = (Header *)base;
    if (h->magic.load(std::memory_order_acquire) != TC_MAGIC || h->layout != TC_LAYOUT ||
        !geometryOk(h, mapBytes)) {
      shmClose(base, mapBytes, &baseHandle); base = nullptr; h = nullptr; return false;
    }
    // Refuse an arena holding compressed entries this build cannot decompress,
    // rather than attaching and reporting silent misses for them.
#ifndef TURBOKV_LZ4
    if (h->features & FEATURE_LZ4) {
      shmClose(base, mapBytes, &baseHandle); base = nullptr; attachError = 1; return false;
    }
#endif
    // Hints live in their OWN segment, opened read-write. The arena fd above is
    // O_RDONLY, so a worker cannot map the arena writable even deliberately -
    // the isolation is a property of the descriptor, not just of the mapping.
    if (!openHints(nm, false)) {
      shmClose(base, mapBytes, &baseHandle);
      base = nullptr; h = nullptr;
      return false;
    }
    bind();
    return true;
  }

  // The arena is written only by the primary and the fd is read-only to
  // everyone else, so this is NOT the hostile-input case the submission ring
  // faces: it guards a half-written header, a layout the build does not match,
  // and a truncated segment. Every field bind() turns into a pointer or a mask
  // is checked against the mapping we actually got.
  static bool geometryOk(const Header *hh, uint64_t mapBytes) {
    auto pow2 = [](uint64_t v) { return v && !(v & (v - 1)); };
    const uint64_t T = hh->totalBytes;
    if (hh->mode != MODE_LOG2) return false;
    // <= not ==: Windows rounds a mapped view up to the allocation granularity.
    if (T < sizeof(Header) || T > mapBytes) return false;
    if (!pow2(hh->indexSlots) || hh->indexSlots < 16 || hh->indexSlots > (1ull << 32)) return false;
    if (!pow2(hh->ringCap) || hh->ringCap == 0 || hh->ringCap > (1ull << 24)) return false;
    // dataBytes is no longer a power of two (see create), so validate the shape
    // that actually matters: 8-byte aligned, non-trivial, and inside the mapping.
    if (hh->dataBytes < 4096 || (hh->dataBytes & 7)) return false;
    const uint64_t ib = hh->indexSlots * sizeof(IndexSlot);   // both bounded above, cannot overflow
    const uint64_t rb = hh->ringCap * sizeof(RingRec);
    if ((hh->indexOff & 63) || hh->indexOff < sizeof(Header) || hh->indexOff > T || ib > T - hh->indexOff) return false;
    if (hh->ringOff < hh->indexOff + ib || hh->ringOff > T || rb > T - hh->ringOff) return false;
    if ((hh->dataOff & 63) || hh->dataOff < hh->ringOff + rb || hh->dataOff > T ||
        hh->dataBytes > T - hh->dataOff) return false;
    if (hh->hintsBytes < hh->indexSlots) return false;
    return true;
  }

  void bind() {
    idx   = (IndexSlot *)(base + h->indexOff);
    ring  = (RingRec *)(base + h->ringOff);
    data  = base + h->dataOff;
    hints = hintsMap;
  }

  // The hints segment is advisory data only: losing it costs eviction quality,
  // never correctness.
  bool openHints(const char *nm, bool create) {
    char hn[80];
    snprintf(hn, sizeof(hn), "%.60s.h", nm);
    if (create) snprintf(hintsName, sizeof(hintsName), "%s", hn);
    hintsMapBytes = h->hintsBytes;
    hintsMap = (std::atomic<uint8_t> *)shmOpenRW(hn, hintsMapBytes, create, &hintsHandle);
    if (!hintsMap) return false;
    return true;
  }

  void destroy() {
    if (hintsMap) { shmClose(hintsMap, hintsMapBytes, &hintsHandle); hintsMap = nullptr; }
    if (base) shmClose(base, mapBytes, &baseHandle);
    if (writable && name[0]) shmUnlink(name);
    if (writable && hintsName[0]) shmUnlink(hintsName);
    base = nullptr;
  }

  // ---- index -----------------------------------------------------------
  // Linear probe. Returns slot index holding `hash` with a matching key, or -1.
  int64_t findSlot(uint64_t hash, const uint8_t *key, uint16_t keyLen) const {
    uint64_t mask = h->indexSlots - 1;
    uint64_t i = hash & mask;
    for (uint64_t probes = 0; probes <= mask; probes++, i = (i + 1) & mask) {
      uint64_t hv = idx[i].hash.load(std::memory_order_acquire);
      if (hv == HASH_EMPTY) return -1;
      if (hv != hash) continue;                     // different key in this slot
      uint64_t pos = idx[i].off.load(std::memory_order_acquire);
      Entry *e = entryAt(pos);
      if (e->keyLen == keyLen && memcmp(keyOf(e), key, keyLen) == 0) return (int64_t)i;
    }
    return -1;
  }

  int64_t findFreeSlot(uint64_t hash) const {
    uint64_t mask = h->indexSlots - 1;
    uint64_t i = hash & mask;
    for (uint64_t probes = 0; probes <= mask; probes++, i = (i + 1) & mask) {
      uint64_t hv = idx[i].hash.load(std::memory_order_relaxed);
      if (hv == HASH_EMPTY || hv == HASH_TOMB) return (int64_t)i;
    }
    return -1;
  }
};
