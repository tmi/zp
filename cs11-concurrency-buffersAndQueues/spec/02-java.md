# Concurrency micro-benchmark — Java implementation spec

## 0. What this is (read first)

This is an **artificial concurrency micro-benchmark**, not production code. Its
only purpose is to compare the wall-clock cost of moving messages through a
`producer → transformer → consumer` pipeline under several coordination
strategies. Because it is artificial:

- Do **not** add logging, metrics, config files, graceful shutdown, error
  recovery, or generalized abstractions. Keep it blunt and direct.
- Do **not** try to "win" the benchmark by exploiting the JVM, the allocator or
  the CPU beyond what a competent engineer would naturally write. No `Unsafe`
  tricks, no hand-rolled off-heap layouts, no intrinsics abuse. Equally, do not
  deliberately pessimize any variant.
- Busy-wait (spin) is expected and preferred over sleeping/parking. We assume the
  machine has enough cores to run `P + T + 1` threads concurrently.
- Use **manual thread management** (`new Thread(...)`), never a thread pool or
  executor.

Target: a recent JDK (21+). Single self-contained source file is fine.

## 1. Folder layout

Create a folder `java/` as a sibling of this `spec/` folder. It is **completely
self-contained** — it shares nothing with the other language folders. Structure:

```
java/
  run.sh            # thin wrapper used by the harness (see §9)
  src/Bench.java    # the whole benchmark
```

`run.sh` must read parameters from environment variables (see §2), build if
needed, and run the program. It must print **only** the program's stdout to
stdout; send any build noise to stderr.

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p build
if [ ! -f build/Bench.class ] || [ src/Bench.java -nt build/Bench.class ]; then
  javac -d build src/Bench.java 1>&2
fi
exec java -Xms1g -Xmx1g -cp build Bench
```

(`-Xms1g -Xmx1g` just reduces GC-driven variance; it is not gaming. Adjust if a
sweep needs more heap.)

## 2. Parameters (all via environment variables)

| Var     | Meaning                                             | Example      |
|---------|-----------------------------------------------------|--------------|
| `N`     | number of messages                                  | `1000000`    |
| `K`     | floats per message                                  | `64`         |
| `I`     | transformer complexity (max norm order)             | `8`          |
| `P`     | producer thread count                               | `1`          |
| `T`     | transformer thread count                            | `4`          |
| `M`     | method: `B A R Qbl Qul Quf` (see §6)                | `A`          |
| `ALLOC` | `allocate` or `pool` (see §7)                       | `pool`       |
| `RING`  | capacity of ring / bounded queues (power of two)    | `1024`       |
| `S`     | suite: `test` or `<warmup>/<real>`                  | `3/10`       |
| `SEED`  | RNG seed (optional, default `42`)                   | `42`         |

Parse them once at startup. Missing required vars → print an error to stderr and
exit non-zero. `RING` is required only for `R` and `Qbl` (bounded); ignore it
otherwise. `RING` must be a power of two for `R`.

## 3. Message, transform, and the correctness oracle

- A **message** is a `float[K]`.
- The **producer** for message id `i` fills the array. Two input modes:
  - **all-ones** (used when `S == test`): every element `= 1.0f`.
  - **random** (all other `S`): fill with a cheap PRNG (e.g. xorshift seeded from
    `SEED + producerIndex`), values in `[0,1)`. The RNG must be negligible vs the
    transform; do not use `java.util.Random` in the hot loop (it's synchronized).
- The **transformer** maps `float[K] -> double`:

```
double transform(float[] x, int I) {
    double total = 0.0;
    for (int i = 1; i <= I; i++) {
        double s = 0.0;
        for (int k = 0; k < x.length; k++) {
            s += Math.pow(Math.abs((double) x[k]), (double) i);   // runtime exponent
        }
        total += Math.pow(s, 1.0 / i);
    }
    return total;
}
```

  Use `Math.pow` with a **runtime** exponent `i` (do not special-case small `i`;
  we want the real transcendental cost so the knob `I` is meaningful).

- The **consumer** keeps a running `double sum` of the transformer outputs.

**Oracle:** with all-ones input, every message's transform output is exactly the
same value `v = Σ_{i=1..I} K^(1/i)`. Summing `N` identical `double` values is
order-independent (all addends are bit-identical), so **every method must print
the same `sum`** in `test` mode. This is the correctness check.

## 4. Output format (exactly this)

Per run, print one line to stdout:

```
RESULT <elapsed_ns> <sum>
```

- `<elapsed_ns>`: `long` from `System.nanoTime()` (see §8).
- `<sum>`: the consumer's final `double`, formatted `%.6f`.

For `S == test`: run **once** with all-ones input; print a single `RESULT` line.

For `S == "<W>/<R>"`: run `W` warmup runs then print a line containing exactly
`---`, then run `R` measured runs. Every run (warmup and measured) prints its own
`RESULT` line. Warmup and measured runs use **random** input. Each run is fully
independent (fresh queues/arrays/threads; pool preallocated fresh per run in the
main thread before timing — see §7/§8).

Nothing else may go to stdout. Diagnostics → stderr.

## 5. Threading & timing protocol (identical for all concurrent methods)

1. In the **main thread**, do all setup for the run: allocate the coordination
   structures (arrays / queues / rings), and if `ALLOC == pool`, preallocate the
   `N` message buffers (see §7). This is **outside** the timed region.
2. Create and start the **consumer** thread (1) and the **transformer** threads
   (`T`). They will immediately begin spinning/blocking on empty inputs.
3. Record `start = System.nanoTime()`.
4. Create and start the **producer** threads (`P`).
5. The consumer, after consuming exactly `N` outputs, records
   `end = System.nanoTime()` and stores it in a field the main thread reads.
6. Main thread `join()`s all threads, then prints `RESULT (end - start) sum`.

Producer `p` (0-indexed) owns the contiguous id range
`[p*N/P, (p+1)*N/P)` (integer arithmetic; this partitions `[0,N)` exactly).

**Baseline `B`** is different: no threads. Record `start`, run the serial loop
`for i in 0..N: sum += transform(produce(i))`, record `end`, print. (In `pool`
mode still preallocate buffers before `start`.)

Spin-waits must call `Thread.onSpinWait()` in the wait loop body. Never
`Thread.sleep`.

## 6. Methods

All non-baseline methods share the **same termination mechanism**: a single
shared atomic **claim counter** `inClaim` (start 0). Each transformer does:

```
long idx = inClaim.getAndIncrement();
if (idx >= N) return;   // all work claimed → this transformer exits
// ... obtain exactly one input, transform it, publish one output ...
```

Because producers emit exactly `N` items and transformers claim exactly `N`
tickets, every claiming transformer eventually receives exactly one item — no
check-then-pop race, no lost or duplicated work, no deadlock.

`inClaim` (and every other shared mutable counter) must be **cache-line padded**
to avoid false sharing. Use a padded holder, e.g.:

```
final class PaddedAtomicLong extends java.util.concurrent.atomic.AtomicLong {
    @SuppressWarnings("unused") volatile long p1,p2,p3,p4,p5,p6,p7;
}
```

### 6.A — `A`: full-size lock-free slot array

Preallocate (in main, untimed) four structures of length `N`:

- `Object[] pbuf` — message buffers (`float[]`), one slot per id.
- `AtomicIntegerArray pReady` — publication flags, init 0.
- `double[] cval` — transformer outputs, one slot per id.
- `AtomicIntegerArray cReady` — publication flags, init 0.

**Producer** `p` (writes by id, so no shared write index, no contention):

```
for (int id = start; id < end; id++) {
    float[] b = (ALLOC==pool) ? (float[])pbuf[id] : new float[K];
    fill(b, id);                    // all-ones or random
    if (ALLOC!=pool) pbuf[id] = b;  // plain array store...
    pReady.set(id, 1);              // ...made visible by this volatile (release) store
}
```

**Transformer:**

```
while (true) {
    long idx = inClaim.getAndIncrement();
    if (idx >= N) break;
    while (pReady.get((int)idx) == 0) Thread.onSpinWait();   // volatile (acquire) load
    double out = transform((float[]) pbuf[(int)idx], I);
    cval[(int)idx] = out;           // plain store...
    cReady.set((int)idx, 1);        // ...published by this volatile store
}
```

**Consumer:**

```
double sum = 0.0;
for (int idx = 0; idx < N; idx++) {
    while (cReady.get(idx) == 0) Thread.onSpinWait();
    sum += cval[idx];
}
end = System.nanoTime();
```

Correctness note: `AtomicIntegerArray` element accesses have volatile (release/
acquire) semantics in the JMM, so the plain writes to `pbuf[idx]`/`cval[idx]`
that *precede* the flag store are guaranteed visible to the reader that *observes*
the flag. **Do not** try to make the payload arrays "volatile" — a `volatile
float[]` only makes the *reference* volatile, not the elements; the flag arrays
are what provides the happens-before. Each slot is written by exactly one thread
and read by exactly one thread, so there are no write-write or read-write races.

### 6.R — `R`: Vyukov bounded MPMC ring buffer

Two rings of capacity `CAP = RING` (power of two, mask `CAP-1`): `inRing`
(payload = `float[]`) and `outRing` (payload = `double`). Implement the canonical
Vyukov bounded MPMC queue. Each ring has:

- `AtomicLongArray seq` of length `CAP`, cell `j` initialized to `j`.
- payload storage (`Object[]` for `inRing`, `double[]` for `outRing`).
- `PaddedAtomicLong enqPos`, `PaddedAtomicLong deqPos` (both start 0).

```
boolean enqueue(item):
    long pos = enqPos.get();
    for (;;) {
        int j = (int)(pos & mask);
        long dif = seq.get(j) - pos;                 // acquire load
        if (dif == 0) { if (enqPos.compareAndSet(pos, pos+1)) break; }
        else if (dif < 0) return false;              // full
        else pos = enqPos.get();
    }
    data[(int)(pos & mask)] = item;
    seq.set((int)(pos & mask), pos + 1);             // release store
    return true;

boolean dequeue(out):   // returns item via holder, or false if empty
    long pos = deqPos.get();
    for (;;) {
        int j = (int)(pos & mask);
        long dif = seq.get(j) - (pos + 1);           // acquire load
        if (dif == 0) { if (deqPos.compareAndSet(pos, pos+1)) break; }
        else if (dif < 0) return false;              // empty
        else pos = deqPos.get();
    }
    out = data[(int)(pos & mask)];
    seq.set((int)(pos & mask), pos + CAP);           // release store
    return true;
```

`AtomicLongArray` get/set are volatile, which satisfies the required acquire/
release ordering (stronger than necessary — fine).

- **Producer:** produce message; `while (!inRing.enqueue(b)) Thread.onSpinWait();`
- **Transformer:** claim ticket via `inClaim`; if `< N`, spin-dequeue one item
  from `inRing`, transform, spin-enqueue the `double` to `outRing`.
- **Consumer:** `for c in 0..N`: spin-dequeue one `double` from `outRing`, add to
  `sum`. Then record `end`.

Single consumer is fine (Vyukov MPMC supports it).

### 6.Q — queue-family variants

Two queues per run: `in` (producer→transformer, payload `float[]`) and `out`
(transformer→consumer, payload boxed `Double`). Same claim-counter termination.

- **`Qbl`** (bounded, locking): `new ArrayBlockingQueue<>(RING)` for both. Producer
  `put` (blocks when full → backpressure); transformer after claiming does `take`
  from `in`, `put` to `out`; consumer `take`s `N` from `out`. `take` blocks, so no
  spin needed here.
- **`Qul`** (unbounded, locking): `new LinkedBlockingQueue<>()` for both. Same as
  above with `put`/`take`.
- **`Quf`** (unbounded, lock-free): `new ConcurrentLinkedQueue<>()` for both. These
  are non-blocking, so use spin-poll: `while ((x = in.poll()) == null)
  Thread.onSpinWait();` and `out.add(x)`. Consumer spin-polls `N` items.

**Skip `Qbf` (bounded lock-free) explicitly.** The JDK has no standard bounded
lock-free queue; that role is exactly what method `R` fills. Do not fake it. If
`M == Qbf`, print an error to stderr and exit non-zero.

Boxing `double` to `Double` for the queues is acceptable here (it is part of what
"using the stdlib queue" costs); do not micro-optimize it away.

## 7. Allocation modes (`ALLOC`)

- **`allocate`**: producers allocate each `float[K]` inside the timed region
  (GC/allocator cost is included and measured).
- **`pool`**: the **main thread**, before `start`, preallocates all `N` buffers
  (for `A`: fill `pbuf` with `new float[K]`; for `R`/`Q`: an array
  `pool[N]` of `float[K]` indexed by id). Producers then fill `pool[id]` in place
  and pass its reference on. No per-message allocation in the timed region.

The publication handshake (flags / seq / queue ops) provides the visibility for
the in-place writes in `pool` mode exactly as in `allocate` mode.

## 8. Timing details

- Use `System.nanoTime()` only. `start` in main immediately before spawning
  producers (§5 step 3); `end` in the consumer immediately after the `N`-th output
  is consumed. Report `end - start`.
- Thread-spawn cost of producers is intentionally inside the measured region.
- No JVM warmup logic beyond the `S` warmup runs; the harness handles warmup.

## 9. Harness interface

The outer harness sets the env vars and invokes `java/run.sh` once per
configuration. `run.sh` must emit only `RESULT ...` lines and the `---` marker on
stdout. It must exit non-zero on any error (including unsupported `M`).

## 10. Implementation checklist (avoid these bugs)

- [ ] `inClaim` uses `getAndIncrement`; transformers stop when `idx >= N`. No
      `while(total>0){ pop() }` check-then-pop pattern anywhere.
- [ ] Every shared counter (`inClaim`, ring `enqPos`/`deqPos`) is cache-line
      padded.
- [ ] Payload arrays are plain; publication/visibility is via
      `AtomicIntegerArray`/`AtomicLongArray` (or the queue). No `volatile T[]`
      used as if elements were volatile.
- [ ] Every spin loop body calls `Thread.onSpinWait()`; no `sleep`/`park`.
- [ ] `pool` preallocation and all structure allocation happen before `start`.
- [ ] `RING` is a power of two for `R`; `Qbf` is refused.
- [ ] `test` mode: single run, all-ones, and all methods print identical `sum`.
- [ ] Consumer captures `end`; main prints `RESULT (end-start) sum` with `%.6f`.
