#pragma once
#include "store.h"

// Test hook: simulate the real topology, where reads happen in workers holding a
// PROT_READ mapping and therefore CANNOT set reference bits.
static bool g_suppressRefBit = false;

static inline uint64_t align8(uint64_t v) { return (v + 7) & ~7ull; }

// Milliseconds since this arena was created, as a uint32.
static inline uint32_t nowRelMs(const Store &s) {
  return (uint32_t)((ticksNs() - s.h->epochTicksNs) / 1000000ull);
}

// Has `exp` passed, given the current relative time? 0 means "no expiry".
//
// A plain `exp <= now` is wrong across the uint32 wrap at 49.7 days of primary
// uptime: an entry whose expiry crosses 2^32 gets a SMALL exp while `now` is
// still large, so it reads as already expired and is dead on arrival for the
// whole length of its TTL. Measured at an uptime of 2^32-1000ms: a 5s TTL
// produced expiresAt=4000 and has() returned false immediately.
//
// Comparing the DIFFERENCE as a signed value is wrap-correct as long as no TTL
// exceeds 2^31 ms (~24.8 days), which storeSet's callers clamp to.
static inline bool tcExpired(uint32_t exp, uint32_t now) {
  return exp != 0 && (int32_t)(now - exp) >= 0;
}

// Largest TTL the wrap-aware comparison above can represent unambiguously.
static const uint32_t TC_TTL_MAX_MS = 0x7FFFFFFFu;

struct ReadResult {
  bool     hit = false;
  uint32_t expiresAt = 0;
  uint32_t rawLen = 0, storedLen = 0;
  uint8_t  flags = 0;
  uint8_t *buf = nullptr;   // caller-owned scratch, filled with stored bytes
};

// ------------------------------------------------------------ index ----
// Knuth 6.4 Algorithm R - backward-shift deletion for linear probing.
// Closes the gap by relocating entries instead of leaving a tombstone, so probe
// chains stay at their natural length forever instead of degrading with churn.
//
// Readers run concurrently. A relocated entry may be observed once, or not at
// all if a scan passes its old slot before the move and its new slot after.
// "Not at all" is a miss, never a wrong value, because the key is memcmp-verified.
static bool g_backwardShift = true;   // bisect hook
// Measured: hit rate saturates at 8 re-appends per allocation (85.6% at 0,
// 85.9% at 1, 86.3% from 8 upward, flat to 8192). 16 gives margin at no cost.
static int  g_secondChanceBudget = 16;

static inline void indexRemove(Store &s, uint64_t i) {
  Header *h = s.h;
  if (!g_backwardShift) {               // old behaviour: leave a tombstone
    s.idx[i].hash.store(HASH_TOMB, std::memory_order_release);
    if (s.hints) s.hints[i].store(0, std::memory_order_relaxed);
    return;
  }
  uint64_t mask = h->indexSlots - 1;
  uint64_t j = i;
  for (;;) {
    s.idx[i].hash.store(HASH_EMPTY, std::memory_order_release);
    if (s.hints) s.hints[i].store(0, std::memory_order_relaxed);
    uint64_t hv;
    for (;;) {
      j = (j + 1) & mask;
      hv = s.idx[j].hash.load(std::memory_order_acquire);
      if (hv == HASH_EMPTY) return;
      uint64_t k = hv & mask;                 // home slot of the entry sitting at j
      bool mustStay = (i <= j) ? (i < k && k <= j) : (i < k || k <= j);
      if (!mustStay) break;
    }
    uint64_t pos = s.idx[j].off.load(std::memory_order_relaxed);
    s.idx[i].off.store(pos, std::memory_order_release);
    if (s.hints) s.hints[i].store(s.hints[j].load(std::memory_order_relaxed), std::memory_order_relaxed);
    s.entryAt(pos)->slot = (uint32_t)i;                           // entry tracks its slot
    s.idx[i].hash.store(hv, std::memory_order_release);
    h->shiftMoves++;
    i = j;
  }
}

// Unlink an index slot. The log reclaims the block when the tail reaches it, so
// there is no free list to return it to.
static inline void unlinkSlot(Store &s, uint64_t slot) {
  Header *h = s.h;
  uint64_t pos = s.idx[slot].off.load(std::memory_order_relaxed);
  indexRemove(s, slot);
  Entry *e = s.entryAt(pos);
  h->live--;
  h->liveBytes -= e->blockSize;
  h->evictions++;
}

// ----------------------------------------------------------------- log ----
static const uint32_t SLOT_PAD = 0xFFFFFFFFu;

// Advance the tail past one record.
// In MODE_LOG2, a live entry whose reference bit is set is re-appended at the
// head instead of dropped (its bit is cleared), giving the log CLOCK-style
// second chance. `budget` caps re-appends so a hot arena still makes progress.
// Bytes the log must step over without a record header, because a header does
// not fit in what is left before the wrap. Records are 8-aligned and the header
// is 40 bytes, so a remainder of 8, 16, 24 or 32 is reachable -- writing a pad
// header there wrote up to 32 bytes PAST the data region, and the tail walk then
// read blockSize from outside it. Today that lands in mapping slack (dataOff is
// never page-aligned), so it neither crashes nor corrupts; it becomes a SIGBUS
// the day the header grows or the layout is page-aligned. The gap is left
// IMPLICIT and both the allocator and the tail walk derive it from the same
// rule, exactly as the submission ring does.
static inline uint32_t logGapAt(uint64_t phys, uint64_t dataBytes) {
  uint64_t remain = dataBytes - phys;
  return remain < sizeof(Entry) ? (uint32_t)remain : 0;
}

static inline void logDropTail(Store &s, int *budget) {
  Header *h = s.h;
  h->tailAdvances++;
  uint64_t tailPos = h->logTail;
  uint64_t phys = tailPos % h->dataBytes;
  uint32_t gap = logGapAt(phys, h->dataBytes);
  if (gap) {                                  // implicit wrap gap: no header here
    h->logTail += gap;
    h->tailPub.store(h->logTail, std::memory_order_release);
    return;
  }
  Entry *e = s.entryAt(tailPos);
  uint32_t bsz = e->blockSize;
  if (bsz == 0 || bsz > h->dataBytes) { h->logTail = h->logHead;
    h->tailPub.store(h->logTail, std::memory_order_release); return; }  // corrupt guard
  if (e->slot != SLOT_PAD) {
    uint64_t slot = e->slot;
    bool liveHere = slot < h->indexSlots &&
        s.idx[slot].off.load(std::memory_order_relaxed) == tailPos &&
        s.idx[slot].hash.load(std::memory_order_relaxed) == e->hash;
    if (liveHere) h->tailLive++;
    bool protect = liveHere && s.hints[slot].load(std::memory_order_relaxed);
    if (liveHere && protect && budget && *budget > 0) {
      uint64_t newPos = h->logHead;
      uint64_t hp = newPos % h->dataBytes;
      uint64_t freeBytes = h->dataBytes - (h->logHead - h->logTail);
      // Room must be verified BEFORE writing. This branch only runs while the
      // log is under allocation pressure - precisely when free space is scarce -
      // so writing bsz bytes at the head unchecked overwrites live records near
      // the tail whose index slots still point at them. That is a silent
      // data-corruption bug, not merely a lost entry.
      // ZERO-COPY SECOND CHANCE. This branch runs from the eviction loop, so
      // free space is short by definition - which is why the copying path was
      // firing on only 1.4% of live tail entries and second chance was
      // effectively dead in a full log, exactly when eviction matters.
      //
      // When the log is full the head lands on the tail's own bytes
      // (hp == phys). The record does not need to move at all: positions are
      // monotonic, so re-publishing it at the new position and advancing both
      // pointers gives it another lap for free. No memcpy, no room required.
      if (hp == phys && bsz <= h->dataBytes) {
        (*budget)--; h->reappends++;
        s.hints[slot].store(0, std::memory_order_relaxed);   // chance consumed
        s.idx[slot].off.store(newPos, std::memory_order_release);
        h->logHead += bsz;
        h->logTail += bsz;
        h->tailPub.store(h->logTail, std::memory_order_release);
        return;
      }
      if (freeBytes < bsz) h->reappendSkippedNoRoom++;
      if (freeBytes >= bsz && hp + bsz <= h->dataBytes && hp != phys) {
        (*budget)--; h->reappends++;
        Entry *dst = s.entryAt(hp);
        uint32_t dseq = dst->seq.load(std::memory_order_relaxed);
        dst->seq.store(dseq | 1, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_release);
        memcpy((uint8_t *)dst + 8, (uint8_t *)e + 8, bsz - 8);  // everything after seq+slot
        dst->slot = slot;
        s.hints[slot].store(0, std::memory_order_relaxed);      // second chance consumed
        std::atomic_thread_fence(std::memory_order_release);
        dst->seq.store((dseq | 1) + 1, std::memory_order_release);
        s.idx[slot].off.store(newPos, std::memory_order_release);
        h->logHead += bsz;
        h->logTail += bsz;
        h->tailPub.store(h->logTail, std::memory_order_release);
        return;
      }
    }
    if (liveHere) {
      indexRemove(s, slot);
      h->live--; h->liveBytes -= bsz;
      h->evictions++; h->dropped++;
    }
  }
  h->logTail += bsz;
  h->tailPub.store(h->logTail, std::memory_order_release);
}

static inline int64_t logAlloc(Store &s, uint32_t need) {
  Header *h = s.h;
  need = (uint32_t)align8(need);
  if (need > h->dataBytes / 2) return -1;
  int budget = g_secondChanceBudget;   // bounded second-chance re-appends per allocation
  const uint64_t D = h->dataBytes;

  // The head position must be recomputed on every iteration: in MODE_LOG2 a
  // second-chance re-append advances logHead, invalidating any position we
  // captured before the eviction loop ran.
  for (uint64_t guard = 0; guard < 1u << 22; guard++) {
    uint64_t phys = h->logHead % D;
    uint64_t freeBytes = h->dataBytes - (h->logHead - h->logTail);

    uint32_t gap = logGapAt(phys, h->dataBytes);
    if (gap) {                                 // too little room even for a header
      if (freeBytes < gap) { logDropTail(s, &budget); continue; }
      h->logHead += gap;                       // implicit; nothing is written
      continue;
    }
    if (phys + need > h->dataBytes) {          // would straddle the wrap: pad to the end
      uint32_t pad = (uint32_t)(h->dataBytes - phys);
      if (freeBytes < pad) { logDropTail(s, &budget); continue; }
      Entry *p = s.entryAt(phys);
      uint32_t pseq = p->seq.load(std::memory_order_relaxed);
      p->seq.store(pseq | 1, std::memory_order_release);
      p->slot = SLOT_PAD; p->blockSize = pad; p->keyLen = 0; p->hash = 0;
      p->storedLen = 0; p->rawLen = 0;
      p->seq.store((pseq | 1) + 1, std::memory_order_release);
      h->logHead += pad;
      continue;
    }
    if (freeBytes < need) { logDropTail(s, &budget); continue; }

    uint64_t pos = h->logHead;
    h->logHead += need;
    h->allocBytes += need;
    return (int64_t)pos;
  }
  return -1;
}

// ----------------------------------------------------------------- ops ----
// Single writer, so the head is bumped with a plain load/release-store rather
// than fetch_add. The record must be written BEFORE the head is published:
// publishing first left a window in which a reader saw the slot's previous lap
// (or zeros) and permanently missed one invalidation.
static inline void ringAppend(Store &s, uint64_t hash, uint32_t version, uint16_t writerId) {
  Header *h = s.h;
  uint64_t pos = h->ringHead.load(std::memory_order_relaxed);
  RingRec *r = &s.ring[pos & (h->ringCap - 1)];
  r->hash = hash; r->version = version; r->writerId = writerId;
  std::atomic_thread_fence(std::memory_order_release);
  h->ringHead.store(pos + 1, std::memory_order_release);
}

// Sole-writer path. Returns false if the value could not be allocated.
static inline bool storeSet(Store &s, const uint8_t *key, uint16_t keyLen,
                            const uint8_t *val, uint32_t storedLen, uint32_t rawLen,
                            uint8_t flags, uint32_t expiresAt, uint16_t writerId) {
  Header *h = s.h;
  uint64_t hash = rapidhash_withSeed(key, keyLen, 0);
  if (hash <= HASH_TOMB) hash += 2;   // reserve 0/1 as sentinels

  // Keep the index below its load ceiling. The old loop gave up after a fixed
  // 4096 iterations, so under an overwrite-heavy workload (where the tail is
  // mostly dead records and `live` does not fall) it expired with the index
  // still full, and `live` crept to 100%. Bound by real progress instead:
  // logDropTail always advances the tail, so this terminates when the tail
  // catches the head.
  {
    // Index pressure, not data pressure. A re-append frees no index SLOT, so
    // second chance cannot relieve this directly -- which is why this loop used
    // to pass a null budget and drop unconditionally. But that made the whole
    // eviction policy vanish whenever the index was the binding constraint.
    // The figure that established this was measured in the namespace era and
    // cannot be restated for today's code: a cold namespace lost all 500 of its
    // QUOTA-protected entries while liveBytes sat at 0.45MB of 32MB (decision
    // 38). Quotas are gone (decision 63); what the budget still rescues is the
    // CLOCK reference bit, which was dropped by the same mechanism. autoSize()
    // gives one slot per 512B, so any workload averaging under ~384B is
    // index-bound in production and never saw the eviction policy at all.
    //
    // Give it a bounded budget instead. Re-appending a protected entry lets the
    // scan step PAST it to find a droppable one, which does free a slot. Once
    // the budget is spent the loop falls back to unconditional drops, so
    // progress is still guaranteed even if every entry is protected.
    int idxBudget = g_secondChanceBudget;
    uint64_t guard = 0, guardMax = (uint64_t)h->indexSlots * 2 + 64;
    while (h->live >= h->maxLive && h->logTail < h->logHead && guard++ < guardMax) {
      logDropTail(s, idxBudget > 0 ? &idxBudget : nullptr);
      h->indexEvictions++;
    }
  }

  uint32_t need = (uint32_t)align8(sizeof(Entry) + keyLen + storedLen);
  int64_t off = logAlloc(s, need);
  if (off < 0) return false;                   // nothing touched yet

  // The slot must be secured BEFORE the old entry is unlinked, and the log
  // block must not be left unwritten. Previously the existing entry was
  // unlinked first, so a later failure destroyed the old value; and a failed
  // findFreeSlot returned with `need` bytes of never-initialised header at the
  // head, whose garbage blockSize desynced the tail walk and bricked the arena.
  int64_t slot = s.findFreeSlot(hash);
  if (slot < 0) {
    {                                          // leave a skippable PAD record
      Entry *p = s.entryAt((uint64_t)off);
      uint32_t pseq = p->seq.load(std::memory_order_relaxed);
      p->seq.store(pseq | 1, std::memory_order_release);
      p->slot = SLOT_PAD; p->blockSize = (uint32_t)align8(need);
      p->keyLen = 0; p->hash = 0; p->storedLen = 0; p->rawLen = 0;
      p->seq.store((pseq | 1) + 1, std::memory_order_release);
    }
    return false;
  }

  int64_t existing = s.findSlot(hash, key, keyLen);
  if (existing >= 0) {
    unlinkSlot(s, (uint64_t)existing);
    slot = s.findFreeSlot(hash);               // the unlink may have shifted slots
    if (slot < 0) return false;
  }

  Entry *e = s.entryAt((uint64_t)off);
  uint32_t seq = e->seq.load(std::memory_order_relaxed);
  e->seq.store(seq | 1, std::memory_order_release);       // mark unstable
  std::atomic_thread_fence(std::memory_order_release);

  e->slot = (uint32_t)slot; e->hash = hash; e->version = ++h->inserts;
  e->expiresAt = expiresAt; e->rawLen = rawLen; e->storedLen = storedLen;
  e->blockSize = (uint32_t)align8(need);
  e->keyLen = keyLen; e->flags = flags;
  memcpy(s.keyOf(e), key, keyLen);
  memcpy(s.valOf(e), val, storedLen);

  std::atomic_thread_fence(std::memory_order_release);
  e->seq.store((seq | 1) + 1, std::memory_order_release); // stable again

  s.idx[slot].off.store((uint64_t)off, std::memory_order_release);
  s.idx[slot].hash.store(hash, std::memory_order_release);
  s.hints[slot].store(1, std::memory_order_relaxed);   // a fresh entry gets one chance
  h->live++; h->liveBytes += e->blockSize;
  ringAppend(s, hash, e->version, writerId);
  return true;
}

// Remove a key. Sole-writer path, like storeSet.
static inline bool storeDelete(Store &s, const uint8_t *key, uint16_t keyLen, uint16_t writerId) {
  Header *h = s.h;
  uint64_t hash = rapidhash_withSeed(key, keyLen, 0);
  if (hash <= HASH_TOMB) hash += 2;
  int64_t slot = s.findSlot(hash, key, keyLen);
  if (slot < 0) return false;
  unlinkSlot(s, (uint64_t)slot);
  ringAppend(s, hash, ++h->inserts, writerId);
  return true;
}

// Drop everything. The log is NOT rewound: logTail is advanced to logHead so
// every previously published position becomes stale under the 
// liveness rule. Rewinding to zero would move the tail BACKWARDS and let a
// reader trust a stale position pointing at reused bytes.
static inline void storeClear(Store &s, uint16_t writerId) {
  Header *h = s.h;
  memset(s.idx, 0, h->indexSlots * sizeof(IndexSlot));
  if (s.hints) for (uint64_t i = 0; i < h->indexSlots; i++) s.hints[i].store(0, std::memory_order_relaxed);
  h->logTail = h->logHead;
  h->tailPub.store(h->logTail, std::memory_order_release);
  h->live = 0; h->liveBytes = 0;
  ringAppend(s, RING_FLUSH_ALL, ++h->inserts, writerId);   // tells workers to drop L1
}

// Existence check: index probe plus key compare plus expiry, with no value copy
// and no promotion. Deliberately does not touch the CLOCK reference bit.
static inline bool storeHas(Store &s, const uint8_t *key, uint16_t keyLen, uint32_t nowMs) {
  Header *h = s.h;
  uint64_t hash = rapidhash_withSeed(key, keyLen, 0);
  if (hash <= HASH_TOMB) hash += 2;
  int64_t slot = s.findSlot(hash, key, keyLen);
  if (slot < 0) return false;
  uint64_t pos = s.idx[slot].off.load(std::memory_order_acquire);
  Entry *e = s.entryAt(pos);
  uint32_t exp = e->expiresAt;
  if (h->tailPub.load(std::memory_order_seq_cst) > pos) return false;
  return !tcExpired(exp, nowMs);
}

// Reader path. Safe against a concurrent writer reusing the block underneath us:
// copy first, then re-check the sequence, and discard a torn read.
static inline bool storeGet(Store &s, const uint8_t *key, uint16_t keyLen,
                            uint8_t *scratch, size_t scratchCap, ReadResult *out,
                            uint32_t nowMs) {
  Header *h = s.h;
  uint64_t hash = rapidhash_withSeed(key, keyLen, 0);
  if (hash <= HASH_TOMB) hash += 2;
  uint64_t mask = h->indexSlots - 1;
  uint64_t i = hash & mask;

  for (uint64_t probes = 0; probes <= mask; probes++, i = (i + 1) & mask) {
    uint64_t hv = s.idx[i].hash.load(std::memory_order_acquire);
    if (hv == HASH_EMPTY) return false;
    if (hv != hash) continue;
    uint64_t pos = s.idx[i].off.load(std::memory_order_acquire);
    // One modulo per read, not one per retry: the position cannot change under
    // us, so its physical offset cannot either.
    uint64_t phys = pos % h->dataBytes;
    Entry *e = (Entry *)(s.data + phys);

    // A tight 8-spin is shorter than a single incr, so a concurrent update storm
  // made 0.04% of reads of a permanently-present key report as missing -- which
  // silently resets a get-or-compute caller's counter. Retry far longer and
  // yield, since the writer always finishes.
  for (int retry = 0; retry < 4096; retry++) {
    if (retry > 64) platformSleepUs(1);
      // Retrying is only ever right when a WRITER is mid-update. If the index
      // observation is stale -- we matched idx[i].hash, then the primary evicted
      // that slot and the head lapped these bytes before we loaded idx[i].off --
      // the bytes here are somebody's payload, so `seq` and the lengths are
      // arbitrary and the checks below fail identically on all 4096 iterations,
      // 4031 of them sleeping. Measured: 11.7ms for ONE get on macOS, and
      // platformSleepUs is a 1-15ms Sleep() on Windows, so seconds of a blocked
      // event loop. Only reachable by racing an eviction, so it is a tail-latency
      // cliff rather than a steady cost -- but the predicate is permanently
      // false, and waiting on it cannot help.
      //
      // The tail only advances, so tail > pos proves the record is dead NOW.
      // relaxed is enough: a stale (smaller) tail only declines the shortcut,
      // and the authoritative seq_cst check after the copy is unchanged. Gated
      // on `retry` so the first, overwhelmingly common iteration pays nothing.
      if (retry && h->tailPub.load(std::memory_order_relaxed) > pos) return false;
      uint32_t s1 = e->seq.load(std::memory_order_acquire);
      if (s1 & 1) continue;                                  // writer mid-update
      uint16_t kl = e->keyLen; uint32_t sl = e->storedLen, rl = e->rawLen;
      uint8_t fl = e->flags; uint32_t exp = e->expiresAt; uint64_t eh = e->hash;
      // Defensive: a torn or corrupt length must never drive a memcpy.
      if ((uint64_t)sizeof(Entry) + kl + sl > h->dataBytes - phys) continue;
      if (sl > scratchCap || kl != keyLen) break;
      if (memcmp(s.keyOf(e), key, keyLen) != 0) break;
      memcpy(scratch, s.valOf(e), sl);
      std::atomic_thread_fence(std::memory_order_acquire);
      uint32_t s2 = e->seq.load(std::memory_order_acquire);
      if (s1 != s2 || eh != hash) continue;                  // torn - retry

      // The seqlock alone is NOT sufficient. It protects an in-place rewrite of
      // this entry, but if the entry was evicted and the log head wrapped over
      // its bytes, this address is no longer an Entry header at all - e->seq is
      // then somebody else's payload, which can read as stable and even twice in
      // a row. Because the same keys are rewritten repeatedly, those bytes
      // frequently hold an OLDER copy of the same key, so memcmp passes too.
      //
      // The log position is monotonic and never reused, so it CAN prove
      // liveness: the bytes at `pos` still belong to record `pos` exactly while
      // logTail <= pos. Loading the published tail after the copy therefore
      // proves the record was live for the whole copy (tail only increases).
      // seq_cst so the copy cannot be reordered after this load.
      if (h->tailPub.load(std::memory_order_seq_cst) > pos) return false;
      if (tcExpired(exp, nowMs)) return false;               // lazily expired
      // Reference bit lives in the hints region, which workers map READ-WRITE
      // even though the rest of the segment is read-only to them. Load first:
      // a hot entry is already marked, so the store (and the cache-line
      // ping-pong between workers) is skipped.
      if (s.hints && !g_suppressRefBit && !s.hints[i].load(std::memory_order_relaxed))
        s.hints[i].store(1, std::memory_order_relaxed);
      out->hit = true; out->rawLen = rl; out->storedLen = sl; out->expiresAt = exp;
      out->flags = fl; out->buf = scratch;
      return true;
    }
    return false;
  }
  return false;
}
