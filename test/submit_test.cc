// Correctness of the submission ring, independent of the cache.
#include "store.h"
#include "submit.h"
#include <thread>
#include <vector>
#include <stdio.h>
#include <string>

static int fails = 0;
static void ok(bool c, const char* m) { printf("  %s  %s\n", c ? "ok  " : "FAIL", m); if (!c) fails++; }

// Consumer: drain one ring, invoking fn per valid record. Returns records applied.
template <class F>
static uint32_t drain(Submit& s, uint32_t idx, F fn, uint32_t maxRecs = 1u << 30) {
  SubmitRing* r = s.ring(idx);
  const uint32_t cap = s.hdr->ringBytes;
  uint64_t head = r->head.load(std::memory_order_acquire);
  uint64_t tail = r->tail.load(std::memory_order_relaxed);
  uint8_t* base = s.ringData(idx);
  uint32_t n = 0;
  while (tail < head && n < maxRecs) {
    uint32_t off = (uint32_t)(tail & (cap - 1));
    uint32_t gap = submitGapAt(off, cap);
    if (gap) { tail += gap; continue; }
    SubmitRec* rec = (SubmitRec*)(base + off);
    uint64_t avail = head - tail;
    if (avail > cap - off) avail = cap - off;
    if (!submitValidate(s, rec, avail)) { r->corrupt.fetch_add(1, std::memory_order_relaxed); break; }
    if (rec->op != SUBMIT_OP_SKIP) { fn(rec, base + off + sizeof(SubmitRec)); n++; }
    tail += rec->len;
  }
  r->tail.store(tail, std::memory_order_release);
  r->applied.fetch_add(n, std::memory_order_relaxed);
  return n;
}

int main() {
  const char* NM = "/tcsubtest";
  shmUnlink(NM);
  Submit s;
  ok(s.create(NM, 4, 1 << 16, 1024, 4096), "create 4 rings x 64KB");

  // 1. round trip
  {
    const char* k = "alpha"; const char* v = "value-one";
    ok(submitPush(s, 0, SUBMIT_OP_SET, 2, 0, k, 5, v, 9), "push one record");
    std::string gotK, gotV;
    uint32_t n = drain(s, 0, [&](SubmitRec* r, uint8_t* p) {
      gotK.assign((char*)p, r->keyLen); gotV.assign((char*)p + r->keyLen, r->valLen); });
    ok(n == 1 && gotK == "alpha" && gotV == "value-one", "record round-trips intact");
  }

  // 2. wrap: push far more bytes than the ring holds, draining as we go, and
  //    verify every record arrives exactly once and in order.
  {
    const uint32_t N = 20000;
    uint32_t sent = 0, got = 0; bool order = true;
    std::vector<uint8_t> val(200, 'x');
    while (sent < N) {
      char key[32]; int kl = snprintf(key, sizeof key, "k%u", sent);
      if (submitPush(s, 1, SUBMIT_OP_SET, 2, 0, key, kl, val.data(), (uint32_t)val.size())) sent++;
      else {
        drain(s, 1, [&](SubmitRec* r, uint8_t* p) {
          char want[32]; int wl = snprintf(want, sizeof want, "k%u", got);
          if ((int)r->keyLen != wl || memcmp(p, want, wl)) order = false;
          got++; });
      }
    }
    drain(s, 1, [&](SubmitRec* r, uint8_t* p) {
      char want[32]; int wl = snprintf(want, sizeof want, "k%u", got);
      if ((int)r->keyLen != wl || memcmp(p, want, wl)) order = false;
      got++; });
    ok(sent == N, "producer eventually placed every record");
    ok(got == N, "consumer received every record exactly once");
    ok(order, "records arrive in FIFO order across many wraps");
  }

  // 3. a full ring sheds, it does not corrupt or block
  {
    std::vector<uint8_t> big(4000, 'y');
    uint32_t accepted = 0;
    for (int i = 0; i < 1000; i++)
      if (submitPush(s, 2, SUBMIT_OP_SET, 2, 0, "k", 1, big.data(), (uint32_t)big.size())) accepted++;
    ok(accepted > 0 && accepted < 1000, "full ring sheds rather than overruns");
    ok(s.ring(2)->shed.load() == 1000 - accepted, "every shed write is counted");
    uint32_t n = drain(s, 2, [](SubmitRec*, uint8_t*) {});
    ok(n == accepted, "everything accepted is still readable after shedding");
  }

  // 4. hostile records are rejected, not trusted
  {
    SubmitRing* r = s.ring(3);
    uint8_t* base = s.ringData(3);
    struct Case { const char* name; uint32_t len; uint8_t op; uint32_t keyLen; uint32_t valLen; };
    Case cases[] = {
      {"len below header",        8, SUBMIT_OP_SET,    4,   0},
      {"len not 8-aligned",      33, SUBMIT_OP_SET,    4,   0},
      {"keyLen beyond maxKey",   64, SUBMIT_OP_SET, 99999,  0},
      {"valLen beyond maxVal",   64, SUBMIT_OP_SET,    4, 999999},
      {"payload exceeds len",    32, SUBMIT_OP_SET,  100,   0},
      {"unknown opcode",         32,           77,     4,   0},
    };
    int rejected = 0;
    for (auto& c : cases) {
      r->head.store(0); r->tail.store(0); r->corrupt.store(0);
      SubmitRec* rec = (SubmitRec*)base;
      rec->len = c.len; rec->op = c.op; rec->flags = 0;
      rec->keyLen = c.keyLen; rec->valLen = c.valLen; rec->ttlMs = 0; rec->reserved = 0;
      r->head.store(4096, std::memory_order_release);       // claim bytes are live
      uint32_t n = drain(s, 3, [](SubmitRec*, uint8_t*) {});
      if (n == 0 && r->corrupt.load() == 1) rejected++;
      else printf("      not rejected: %s\n", c.name);
    }
    ok(rejected == (int)(sizeof(cases) / sizeof(cases[0])), "every malformed record is rejected");
  }

  // 5. concurrent single-producer / single-consumer
  {
    const uint32_t N = 300000;
    std::atomic<uint32_t> received{0};
    std::atomic<bool> done{false};
    bool order = true;
    uint32_t expect = 0;
    std::thread prod([&] {
      std::vector<uint8_t> v(120, 'z');
      for (uint32_t i = 0; i < N; ) {
        char key[32]; int kl = snprintf(key, sizeof key, "k%u", i);
        if (submitPush(s, 0, SUBMIT_OP_SET, 2, 0, key, kl, v.data(), (uint32_t)v.size())) i++;
        else std::this_thread::yield();
      }
      done.store(true, std::memory_order_release);
    });
    while (received.load() < N) {
      drain(s, 0, [&](SubmitRec* r, uint8_t* p) {
        char want[32]; int wl = snprintf(want, sizeof want, "k%u", expect);
        if ((int)r->keyLen != wl || memcmp(p, want, wl)) order = false;
        expect++; received.fetch_add(1, std::memory_order_relaxed); });
    }
    prod.join();
    ok(received.load() == N, "SPSC: consumer saw all 300k records");
    ok(order, "SPSC: no reordering, no tearing under concurrency");
    ok(s.ring(0)->corrupt.load() == 0, "SPSC: no record ever failed validation");
  }

  // 6. VARIED record sizes across many wraps.
  //    Records are 8-aligned but the header is 24 bytes, so a wrap can leave a
  //    remainder of 8 or 16 -- too small to hold even a SKIP header. Fixed-size
  //    records may never produce that remainder, which is why cases 2 and 5
  //    passed while the real cross-process run wedged after 26k records. Cycling
  //    the key length walks every possible remainder.
  {
    const uint32_t N = 60000;
    uint32_t sent = 0, got = 0; bool order = true;
    s.ring(3)->head.store(0); s.ring(3)->tail.store(0); s.ring(3)->corrupt.store(0);
    std::vector<uint8_t> val(64, 'q');
    auto consume = [&]() {
      drain(s, 3, [&](SubmitRec* r, uint8_t* p) {
        char want[80]; int wl = snprintf(want, sizeof want, "k%u", got);
        for (int j = wl; j < (int)(1 + got % 57); j++) want[j] = 'p';
        int wantLen = wl > (int)(1 + got % 57) ? wl : (int)(1 + got % 57);
        if ((int)r->keyLen != wantLen || memcmp(p, want, wl)) order = false;
        got++; });
    };
    // Stall guard: a wedged ring makes the producer spin forever, so without a
    // bound this test HANGS instead of failing -- which is a worse outcome than
    // a red line. If a whole drain pass frees nothing, the ring is stuck.
    uint32_t stall = 0;
    while (sent < N && stall < 3) {
      char key[80]; int kl = snprintf(key, sizeof key, "k%u", sent);
      int want = 1 + sent % 57;
      for (int j = kl; j < want; j++) key[j] = 'p';
      if (kl < want) kl = want;
      if (submitPush(s, 3, SUBMIT_OP_SET, 2, 0, key, kl, val.data(), (uint32_t)(sent % 300))) { sent++; stall = 0; }
      else { uint32_t was = got; consume(); if (got == was) stall++; }
    }
    ok(stall < 3, "varied-size records: ring never wedges (producer always makes progress)");
    consume();
    ok(got == N, "varied-size records: every one received across wraps");
    ok(order, "varied-size records: order and contents intact");
    ok(s.ring(3)->corrupt.load() == 0, "varied-size records: none rejected as corrupt");
  }

  // 7. HOSTILE HEADER. The SubmitHeader sits at offset 0 of a segment every
  //    worker maps read-write, so it is attacker-controlled. The consumer must
  //    never re-read geometry or validation bounds from it. Before this was
  //    fixed, a worker setting ringCount huge walked the primary off the end of
  //    the mapping (SIGSEGV) and huge maxKey/maxVal disabled validation.
  {
    Submit wrk;
    ok(wrk.open(NM), "second mapping opens read-write, as a worker's does");
    wrk.hdr->ringCount = 0x40000000; wrk.hdr->ringBytes = 0xFFFFFFFF;
    wrk.hdr->maxKey = 0xFFFFFFFF; wrk.hdr->maxVal = 0xFFFFFFFF;
    wrk.hdr->dataOff = (uint64_t)1 << 40;
    ok(s.ringCount == 4 && s.ringBytes == (1u << 16) && s.maxKey == 1024 && s.maxVal == 4096,
       "consumer geometry is unaffected by header corruption");
    // A record that only passes if maxKey/maxVal came from the corrupt header.
    SubmitRec probe{};
    probe.len = 4096; probe.op = SUBMIT_OP_SET; probe.keyLen = 100000; probe.valLen = 0;
    ok(!submitValidate(s, &probe, 4096), "validation bounds are not attacker-controlled");
    wrk.close();
  }

  // 8. SKIP FLOOD. SKIP records advance tail without applying anything, so a
  //    budget counted only in applied records never bites: a worker filling its
  //    ring with valid SKIPs and claiming a huge head made the consumer walk
  //    2^50 bytes synchronously (measured 717M iterations against budget 4096).
  {
    const uint32_t cap = s.ringBytes;
    uint8_t* base = s.ringData(2);
    for (uint32_t off = 0; off + sizeof(SubmitRec) <= cap; off += 24) {
      SubmitRec* r = (SubmitRec*)(base + off);
      r->len = 24; r->op = SUBMIT_OP_SKIP; r->flags = 0;
      r->keyLen = 0; r->valLen = 0; r->ttlMs = 0; r->reserved = 0;
    }
    s.ring(2)->tail.store(0);
    s.ring(2)->head.store((uint64_t)1 << 50, std::memory_order_release);
    uint64_t head = s.ring(2)->head.load(std::memory_order_acquire);
    uint64_t tail = s.ring(2)->tail.load(std::memory_order_relaxed);
    ok(head - tail > (uint64_t)cap, "a corrupt head is detectable as > ring capacity");
    // The consumer's guard: refuse the span rather than walking it.
    long steps = 0;
    if (head - tail <= (uint64_t)cap) {
      const long maxSteps = 4096L * 4 + 64;
      while (tail < head && steps < maxSteps) { steps++; tail += 24; }
    }
    ok(steps == 0, "oversized head is refused, not walked");
  }

  // 9. The consumer must use the fields it VALIDATED, not fresh loads of the
  //    same worker-writable words. Validating through the pointer and then
  //    re-reading is a TOCTOU: a worker that flips valLen after validation
  //    passed gets its value to storeSet, where the size arithmetic truncates
  //    through 2^32 so the allocation succeeds while the copy uses the full
  //    length -- a multi-gigabyte memcpy out of the ring and past the arena.
  {
    s.ring(1)->head.store(0); s.ring(1)->tail.store(0);
    const char* v16 = "0123456789abcdef";
    ok(submitPush(s, 1, SUBMIT_OP_SET, 2, 0, "k", 1, v16, 16), "pushed a record to attack");
    uint8_t* base = s.ringData(1);
    SubmitRec* live = (SubmitRec*)base;
    uint64_t head = s.ring(1)->head.load(std::memory_order_acquire);

    SubmitRec snap;                                  // what the drain must do
    memcpy(&snap, base, sizeof(SubmitRec));
    ok(submitValidate(s, &snap, head), "the snapshot validates");
    live->valLen = 0xFFFFFFD0u;                      // a racing worker flips it
    ok(snap.valLen == 16, "the snapshot is unaffected by a post-validation write");
    ok(submitValidate(s, &snap, head), "the snapshot still validates after the flip");
    // and the hostile value would NOT have validated, which is the whole point
    SubmitRec after;
    memcpy(&after, base, sizeof(SubmitRec));
    ok(!submitValidate(s, &after, head), "the flipped record would have been rejected on its own");
    live->valLen = 16;
  }

  s.close();
  shmUnlink(NM);
  printf(fails ? "\n%d FAILED\n" : "\nall passed\n", fails);
  return fails ? 1 : 0;
}
