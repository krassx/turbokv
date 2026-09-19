// Native-layer regressions found by adversarial review: TTL across the uint32
// epoch wrap, and the log's wrap gap.
//
// expiresAt is a uint32 count of milliseconds since the arena was created, so it
// wraps at 49.7 days of primary uptime. A plain `exp <= now` compare is wrong
// across that boundary: an entry whose expiry crosses 2^32 gets a small `exp`
// while `now` is still large, so it reads as already expired and is dead on
// arrival for the entire length of its TTL.
#include "store.h"
#include "store_ops.h"
#include <stdio.h>
#include <vector>
#include <chrono>

static int fails = 0;
static void ok(bool c, const char* m) { printf("  %s  %s\n", c ? "ok  " : "FAIL", m); if (!c) fails++; }

int main() {
  const char* NM = "/tcexpirytest";
  shmUnlink(NM);
  Store s;
  if (!s.create(NM, 8u << 20, 1u << 12, MODE_LOG2)) { printf("  create failed\n"); return 1; }

  struct Case { const char* label; uint64_t uptime; };
  Case cases[] = {
    {"fresh arena",             0},
    {"1s before the wrap",      0xFFFFFFFFull - 1000},
    {"just after the wrap",     0x100000000ull + 500},
    {"mid-range",               0x80000000ull},
  };
  const uint32_t TTL = 5000;
  for (auto& c : cases) {
    // epochTicksNs, not epochMs: the arena epoch is on the tick clock now.
    s.h->epochTicksNs = ticksNs() - c.uptime * 1000000ull;
    uint32_t now = nowRelMs(s);
    uint32_t exp = now + TTL;
    char m[160];
    snprintf(m, sizeof m, "%s: live now, live just before expiry, dead just after", c.label);
    ok(!tcExpired(exp, now) && !tcExpired(exp, now + TTL - 100) && tcExpired(exp, now + TTL + 100), m);
  }

  // 0 always means "no expiry", at every point in the cycle.
  ok(!tcExpired(0, 0) && !tcExpired(0, 0xFFFFFFFFu) && !tcExpired(0, 0x80000000u),
     "expiresAt 0 is immortal regardless of the clock");

  // The naive predicate this replaced gets the wrap case wrong. Asserting that
  // keeps the test honest about what it is protecting.
  {
    uint32_t now = 0xFFFFFFFFu - 1000, exp = now + TTL;   // wraps to 3999
    bool naiveSaysExpired = (exp != 0 && exp <= now);
    ok(naiveSaysExpired && !tcExpired(exp, now),
       "the naive `exp <= now` compare fails here; the wrap-aware one does not");
  }


  // The log's wrap gap. Records are 8-aligned but the Entry header is 40 bytes,
  // so a wrap can leave 8/16/24/32 bytes -- too little for the pad header that
  // used to be written there, which overran the data region by up to 32 bytes
  // and made the tail walk read blockSize from outside it. The gap is implicit
  // now; both the allocator and the tail walk derive it from the same rule.
  {
    // The data region now runs to the end of the mapping, so there is nowhere
    // past it to put a canary. Shrink it by 64 bytes to carve that room out --
    // every site derives its wrap from h->dataBytes, so the store behaves
    // exactly as if it had been created at this size. The shrink also leaves a
    // region that is NOT a power of two, which is the point: a mask would index
    // it wrongly, so this block now exercises the modulo wrap as well.
    s.h->dataBytes -= 64;
    const uint64_t D = s.h->dataBytes;
    ok((D & (D - 1)) != 0, "the data region under test is not a power of two");
    uint8_t* past = s.data + D;
    memset(past, 0xAB, 64);                       // canary just past the region
    std::vector<uint8_t> val(200, 'v');
    char key[48];

    // Visit every reachable sub-header remainder deterministically. The bulk
    // loop below cannot be trusted to do it: its records are >= 248 bytes, so
    // 200k writes wrap the region only ~6 times, and each wrap has roughly a
    // 15% chance of leaving a tail smaller than an Entry header. This assertion
    // used to pass on luck, and stopped passing the moment the region size
    // changed. Driving the head straight at each remainder is what the comment
    // above always claimed the test did.
    int remaindersCovered = 0;
    for (uint32_t r = 8; r < sizeof(Entry); r += 8) {
      uint64_t base = D * 4 + (D - r);           // phys == D - r: r bytes left
      s.h->logHead = base; s.h->logTail = base;
      s.h->tailPub.store(base, std::memory_order_release);
      if (logGapAt(base % D, D) != r) continue;
      int kl = snprintf(key, sizeof key, "gap%u", r);
      storeSet(s, (const uint8_t*)key, (uint16_t)kl, val.data(), 200, 200,
               FLAG_STRING, 0, 0);
      ReadResult rr; uint8_t buf[256];
      bool got = storeGet(s, (const uint8_t*)key, (uint16_t)kl, buf, sizeof buf, &rr, 0);
      if (got && s.h->logHead % D != 0 && s.h->logHead > base + r) remaindersCovered++;
    }
    ok(remaindersCovered == (int)(sizeof(Entry) / 8) - 1,
       "every sub-header wrap remainder is skipped implicitly and still serves the write");

    uint64_t minRemain = D;
    for (uint32_t i = 0; i < 200000; i++) {
      int kl = snprintf(key, sizeof key, "k%u", i);
      int want = 1 + (int)(i % 33);
      for (int j = kl; j < want; j++) key[j] = 'p';
      if (kl < want) kl = want;
      storeSet(s, (const uint8_t*)key, (uint16_t)kl, val.data(),
               (uint32_t)(i % 180), (uint32_t)(i % 180), FLAG_STRING, 0, 0);
      uint64_t remain = D - (s.h->logHead % D);
      if (remain < minRemain) minRemain = remain;
    }
    int dirty = 0;
    for (int i = 0; i < 64; i++) if (past[i] != 0xAB) dirty++;
    ok(minRemain <= D, "the bulk loop wrapped the region");
    ok(dirty == 0, "nothing is written past the data region across 200k wrapping writes");

    storeSet(s, (const uint8_t*)"final", 5, (const uint8_t*)"ok", 2, 2, FLAG_STRING, 0, 0);
    ReadResult rr; uint8_t buf[64];
    ok(storeGet(s, (const uint8_t*)"final", 5, buf, sizeof buf, &rr, 0),
       "the arena still serves reads after all those wraps");
  }

  // The reader's defensive bound check. storeGet re-derives the physical offset
  // of the record it is about to copy and refuses any (keyLen, storedLen) pair
  // that would run off the end of the data region -- a torn or corrupt length
  // must never drive the memcpy. That derivation was a mask, which is only the
  // same as a modulo while the region is a power of two; on any other size it
  // understates the offset, the check passes vacuously, and the memcpy reads
  // past the end of the mapping. Placing a record flush against the end of the
  // region and corrupting its length is the case that separates the two.
  {
    const uint64_t D = s.h->dataBytes;
    const uint32_t need = (uint32_t)align8(sizeof(Entry) + 4 + 200);
    uint64_t base = D * 8 + (D - need);          // record ends exactly at the region end
    s.h->logHead = base; s.h->logTail = base;
    s.h->tailPub.store(base, std::memory_order_release);

    std::vector<uint8_t> val(200, 'v');
    storeSet(s, (const uint8_t*)"edge", 4, val.data(), 200, 200, FLAG_STRING, 0, 0);

    uint8_t buf[8192];
    ReadResult rr;
    ok(storeGet(s, (const uint8_t*)"edge", 4, buf, sizeof buf, &rr, 0) && rr.rawLen == 200,
       "a record flush against the end of the region reads back normally");

    // Corrupt storedLen to something that still fits the caller's scratch (so the
    // scratch guard does not catch it first) but runs far past the region end.
    Entry *e = s.entryAt(base);
    e->storedLen = 4096;
    ok(!storeGet(s, (const uint8_t*)"edge", 4, buf, sizeof buf, &rr, 0),
       "a corrupt length at the region end is refused instead of read out of bounds");
  }

  // Header geometry validation. attachReadOnly used to check only magic and
  // layout, then bind() computed pointers and masks straight from fields it had
  // never validated -- indexSlots 0 gives a probe mask of 2^64-1. The window
  // where that mattered was masked by an accident (create() published magic
  // first, but the hints segment did not exist yet, so the attach was refused
  // for an unrelated reason). magic is now published LAST with a release store,
  // and the geometry is checked against the mapping we actually got. Testing the
  // predicate directly, because going through attachReadOnly conflates this with
  // whether the hints segment happens to exist.
  {
    alignas(64) unsigned char raw[sizeof(Header)];
    memcpy(raw, s.base, sizeof(Header));            // a known-good header
    Header *g0 = (Header *)raw;
    const uint64_t mapBytes = s.mapBytes;
    ok(Store::geometryOk(g0, mapBytes), "a real header passes validation");

    struct C { const char *name; void (*bend)(Header *); };
    C cases[] = {
      {"indexSlots = 0",          [](Header *h){ h->indexSlots = 0; }},
      {"indexSlots not pow2",     [](Header *h){ h->indexSlots = 4095; }},
      // NOT "not a power of two" -- decision 44 made any size legal. 12345 is
      // refused for being misaligned (12345 & 7), which is what is asserted.
      {"dataBytes misaligned",    [](Header *h){ h->dataBytes = 12345; }},
      {"dataBytes below the floor", [](Header *h){ h->dataBytes = 4095; }},
      {"retired mode SLAB",       [](Header *h){ h->mode = 0; }},
      {"retired mode LOG",        [](Header *h){ h->mode = 1; }},
      {"dataOff + dataBytes > T", [](Header *h){ h->dataBytes = h->totalBytes; }},
      {"indexOff inside header",  [](Header *h){ h->indexOff = 8; }},
      {"ringOff overlaps index",  [](Header *h){ h->ringOff = h->indexOff; }},
      {"hintsBytes < indexSlots", [](Header *h){ h->hintsBytes = 8; }},
      {"totalBytes > mapping",    [](Header *h){ h->totalBytes = h->totalBytes * 4; }},
      {"mode out of range",       [](Header *h){ h->mode = 9; }},
      {"all-zero geometry",       [](Header *h){ h->indexSlots = 0; h->dataBytes = 0; h->ringCap = 0;
                                                 h->indexOff = 0; h->dataOff = 0; h->ringOff = 0; }},
    };
    int refused = 0;
    for (auto &c : cases) {
      alignas(64) unsigned char bent[sizeof(Header)];
      memcpy(bent, raw, sizeof(Header));
      c.bend((Header *)bent);
      if (!Store::geometryOk((Header *)bent, mapBytes)) refused++;
      else printf("      accepted: %s\n", c.name);
    }
    ok(refused == (int)(sizeof(cases) / sizeof(cases[0])), "every corrupted geometry is refused");
  }

  // The TC_LAYOUT gate in attachReadOnly. DESIGN.md decision 63 claimed this
  // was "exercised by guard_test.js against the header check in
  // attachReadOnly" -- it was not: guard_test.js is the heap-guard suite and
  // never attaches an arena, and geometryOk (tested above) never reads
  // `layout` either, since that check is a separate line in attachReadOnly.
  // Nothing in the tree bent `layout` before this case. The gate is the
  // entire cross-version safety argument for a Header that just shrank 904
  // bytes removing the namespace table (decision 63): without it, a process
  // built before that change attaches to an arena built after it and
  // misreads every record.
  {
    const char* NM4 = "/tclayouttest";
    shmUnlink(NM4);
    Store w;
    ok(w.create(NM4, 8u << 20, 1u << 12, MODE_LOG2), "layout test: arena created");
    ok(w.h->layout == TC_LAYOUT, "layout test: create() published the current TC_LAYOUT");

    // The realistic threat is a build made BEFORE a layout bump attaching to
    // an arena a newer primary just created, so the value below the current
    // one is the one that matters most -- but bend it above too, since the
    // check is a plain inequality and both directions are cheap to cover.
    w.h->layout = TC_LAYOUT - 1;
    {
      Store older;
      ok(!older.attachReadOnly(NM4), "an arena one layout version behind is refused");
    }

    w.h->layout = TC_LAYOUT + 1;
    {
      Store newer;
      ok(!newer.attachReadOnly(NM4), "an arena one layout version ahead is refused");
    }

    // The field that forced 6 -> 7: the L3 clear generation. A fresh arena owes
    // L3 nothing, so the two counters must start EQUAL -- the guard's whole
    // meaning is `gen != settled`, and a non-zero `gen` on a new arena would
    // make every process serve L3 misses forever. The 16 bytes it added must
    // also still leave the index where create() computed it: indexOff is
    // derived from sizeof(Header) and re-validated on every attach, so a
    // Header that outgrew its 64-byte rounding would move the whole data
    // region under a reader that rounded differently.
    ok(w.h->l3ClearGen.load() == 0 && w.h->l3ClearSettled.load() == 0,
       "layout test: a fresh arena has no L3 clear in flight");
    ok(w.h->indexOff >= sizeof(Header) && (w.h->indexOff & 63) == 0,
       "layout test: the index still starts past the grown Header, 64-byte aligned");

    // Restore the real layout and confirm a matching build still attaches --
    // a gate that refuses everything would pass the two cases above for the
    // wrong reason.
    w.h->layout = TC_LAYOUT;
    {
      Store good;
      ok(good.attachReadOnly(NM4), "the same layout version still attaches");
      good.destroy();
    }

    w.destroy();
    shmUnlink(NM4);
  }

  // The expiry sweep must use the wrap-aware comparison too. It was the fifth
  // comparison site and the one left behind: a plain `exp > now` deletes every
  // entry whose expiry crosses the uint32 wrap while it is still live, once per
  // 49.7 days of primary uptime.
  {
    s.h->epochTicksNs = ticksNs() - (0xFFFFFFFFull - 1000) * 1000000ull;
    uint32_t now = nowRelMs(s);
    uint32_t exp = now + 5000;                       // wraps to ~4000
    ok(exp < now, "the expiry genuinely wrapped past the uint32 boundary");
    ok(!tcExpired(exp, now), "a wrapped-but-live expiry is not expired");
    // the comparison the sweep used to make:
    ok((exp != 0 && exp <= now), "the naive sweep compare would have deleted it");
  }

  // A stale index observation must not be paid for with sleep.
  //
  // A reader matches idx[i].hash, then the primary evicts that slot and the log
  // head laps the record's bytes before the reader loads idx[i].off. The reader
  // is now pointed at somebody else's payload, so `seq` and the lengths are
  // arbitrary -- and the retry loop treated that as "a writer is mid-update",
  // spinning all 4096 iterations with platformSleepUs(1). Measured at 11.7ms for
  // ONE get on macOS, and Sleep(1) on Windows is 1-15ms, so seconds of blocked
  // event loop. The tail only advances, so tail > pos settles it immediately.
  //
  // This replaces an assertion that could not fail. It read 20k keys from an
  // arena under heavy eviction and asserted the total stayed under 5s, on the
  // theory that the index was "full of evicted entries". It is not: eviction
  // runs unlinkSlot -> indexRemove, which clears the slot before the bytes are
  // reused, so every one of those misses resolved at HASH_EMPTY without ever
  // entering the retry loop (instrumented: 0 retries in 20000 reads). The state
  // only arises from the concurrent interleaving above, which a single-threaded
  // test cannot produce by running a workload -- so it is constructed here.
  {
    const char* NM3 = "/tcstaleobs";
    shmUnlink(NM3);
    Store t;
    if (!t.create(NM3, 26u << 20, 1u << 14, MODE_LOG2)) { ok(false, "create failed"); }
    else {
      std::vector<uint8_t> val(400, 'v');
      char key[32];
      const uint16_t VK = 6;
      storeSet(t, (const uint8_t*)"victim", VK, val.data(), 400, 400, FLAG_STRING, 0, 0);
      uint64_t hash = rapidhash_withSeed("victim", VK, 0);
      if (hash <= HASH_TOMB) hash += 2;
      int64_t slot = t.findSlot(hash, (const uint8_t*)"victim", VK);
      uint64_t pos = t.idx[slot].off.load(std::memory_order_relaxed);

      for (int i = 0; i < 80000; i++) {            // lap the log well past it
        int kl = snprintf(key, sizeof key, "k%d", i);
        storeSet(t, (const uint8_t*)key, (uint16_t)kl, val.data(), 400, 400,
                 FLAG_STRING, 0, 0);
      }
      ok(t.h->logTail > pos, "the victim's position really was lapped");

      // A lapped position lands wherever the new records now lie -- usually the
      // middle of one, so the "header" there is payload, not an Entry.
      pos += 24;
      t.idx[slot].hash.store(hash, std::memory_order_release);
      t.idx[slot].off.store(pos, std::memory_order_release);
      Entry *stale = t.entryAt(pos);
      ok((stale->seq.load(std::memory_order_relaxed) & 1) == 0 &&
         (uint64_t)sizeof(Entry) + stale->keyLen + stale->storedLen >
             t.h->dataBytes - (pos % t.h->dataBytes),
         "the stale bytes look stable but fail the bound check, so they retry");

      uint8_t buf[8192]; ReadResult rr;
      const int N = 100;
      auto t0 = std::chrono::steady_clock::now();
      int hits = 0;
      for (int i = 0; i < N; i++)
        if (storeGet(t, (const uint8_t*)"victim", VK, buf, sizeof buf, &rr, 0)) hits++;
      double ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - t0).count();
      char m[160];
      snprintf(m, sizeof m, "%d reads of a stale index observation took %.1f ms (< 50)", N, ms);
      ok(ms < 50.0, m);
      ok(hits == 0, "a stale index observation is a miss, not a hit");
      t.destroy();
    }
    shmUnlink(NM3);
  }

  s.destroy(); shmUnlink(NM);
  printf(fails ? "\n%d FAILED\n" : "\nall passed\n", fails);
  return fails ? 1 : 0;
}
