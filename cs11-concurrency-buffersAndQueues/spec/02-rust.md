# Concurrency micro-benchmark — Rust implementation spec

## 0. What this is (read first)

This is an **artificial concurrency micro-benchmark**, not production code. Its
only purpose is to compare the wall-clock cost of moving messages through a
`producer → transformer → consumer` pipeline under several coordination
strategies. Because it is artificial:

- Do **not** add logging, config, graceful shutdown, error recovery, or
  generalized abstractions. Keep it blunt and direct. `unwrap()`/`expect()` on
  setup is fine.
- Do **not** try to "win" by exploiting the CPU/allocator beyond what a competent
  engineer would naturally write. No inline asm, no SIMD intrinsics, no exotic
  allocators. Equally, do not deliberately pessimize any variant.
- Busy-wait (spin) is expected and preferred over sleeping/parking. Assume the
  machine has enough cores for `P + T + 1` threads.
- Use **manual thread management** (`std::thread::spawn`), never a thread pool or
  `rayon`.

Target: stable Rust. Allowed dependencies: `crossbeam-channel` and
`crossbeam-utils` only (for the lock-free queue variants and `CachePadded`). No
other crates.

## 1. Folder layout

Create a folder `rust/` as a sibling of this `spec/` folder. It is **completely
self-contained** — shares nothing with the other language folders.

```
rust/
  run.sh
  Cargo.toml        # deps: crossbeam-channel, crossbeam-utils
  src/main.rs
```

`run.sh` (thin wrapper for the harness — builds once, runs the binary directly so
`cargo`'s own overhead is not measured; build noise → stderr):

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --quiet 1>&2
exec ./target/release/bench
```

Name the binary `bench` in `Cargo.toml`.

## 2. Parameters (all via environment variables)

| Var     | Meaning                                             | Example      |
|---------|-----------------------------------------------------|--------------|
| `N`     | number of messages                                  | `1000000`    |
| `K`     | floats per message                                  | `64`         |
| `I`     | transformer complexity (max norm order)             | `8`          |
| `P`     | producer thread count                               | `1`          |
| `T`     | transformer thread count                            | `4`          |
| `M`     | method: `B A R Qbl Qbf Qul Quf` (see §6)            | `A`          |
| `ALLOC` | `allocate` or `pool` (see §7)                       | `pool`       |
| `RING`  | capacity of ring / bounded queues (power of two)    | `1024`       |
| `S`     | suite: `test` or `<warmup>/<real>`                  | `3/10`       |
| `SEED`  | RNG seed (optional, default `42`)                   | `42`         |

Parse once at startup with `std::env::var`. Missing required vars → eprintln +
`std::process::exit(1)`. `RING` required for `R`, `Qbl`, `Qbf`; must be a power of
two for `R`.

## 3. Message, transform, and the correctness oracle

- A **message** is a `Vec<f32>` of length `K` (or `Box<[f32]>`; pick one and be
  consistent).
- The **producer** for id `i` fills it. Two input modes:
  - **all-ones** (`S == test`): every element `1.0f32`.
  - **random** (otherwise): cheap xorshift PRNG seeded from `SEED + producer_index`,
    values in `[0,1)`. Keep RNG negligible vs transform.
- The **transformer** maps `&[f32] -> f64`:

```rust
fn transform(x: &[f32], big_i: u32) -> f64 {
    let mut total = 0.0f64;
    for i in 1..=big_i {
        let mut s = 0.0f64;
        for &v in x {
            s += (v.abs() as f64).powf(i as f64);   // runtime exponent
        }
        total += s.powf(1.0 / i as f64);
    }
    total
}
```

  Use `powf` with a **runtime** exponent (do not special-case small `i`; we want
  the real transcendental cost so `I` is a meaningful knob).

- The **consumer** keeps a running `f64` sum of transformer outputs.

**Oracle:** with all-ones input every message's output is exactly
`v = Σ_{i=1..I} K^(1/i)`. Summing `N` bit-identical `f64` values is order-
independent, so **every method prints the same `sum`** in `test` mode. That is the
correctness check.

## 4. Output format (exactly this)

Per run, one line to stdout:

```
RESULT <elapsed_ns> <sum>
```

- `<elapsed_ns>`: `u128` nanoseconds from an `Instant` duration.
- `<sum>`: consumer's final `f64`, formatted `{:.6}`.

`S == test`: run once, all-ones, single `RESULT` line.

`S == "<W>/<R>"`: `W` warmup runs, then a line with exactly `---`, then `R`
measured runs; each run prints a `RESULT`. Warmup+measured use **random** input.
Each run is fully independent (fresh structures/threads; `pool` preallocated fresh
per run in the main thread before timing). Only `RESULT`/`---` on stdout;
diagnostics → stderr.

## 5. Threading & timing protocol (all concurrent methods)

1. Main thread does all setup for the run (allocate coordination structures; if
   `ALLOC == pool`, preallocate the `N` buffers). **Untimed.**
2. Spawn the **consumer** (1) and **transformer** (`T`) threads; they begin
   spinning/blocking on empty input.
3. `let start = Instant::now();`
4. Spawn the **producer** (`P`) threads.
5. The consumer, after consuming exactly `N` outputs, computes
   `let end = Instant::now();` and hands `(end - start)` (or the raw `end`) plus
   `sum` back to main (e.g. via the `JoinHandle` return value).
6. Main joins all threads and prints `RESULT <ns> <sum>`.

Producer `p` owns id range `[p*N/P, (p+1)*N/P)` (integer math; exact partition of
`[0,N)`).

**Baseline `B`**: no threads. `start`, serial
`for i in 0..N { sum += transform(&produce(i)) }`, `end`, print. (`pool`
preallocation still before `start`.)

Spin loops call `std::hint::spin_loop()` in the wait body. Never `thread::sleep`.

Sharing structures across threads: wrap in `Arc`. For the hand-rolled `A`/`R`
storage you will need a small `unsafe impl Sync` wrapper (see below) — that is the
one place `unsafe` is justified; keep it tightly scoped and commented.

## 6. Methods

All non-baseline methods share the **same termination mechanism**: one shared
`AtomicUsize` **claim counter** `in_claim` (start 0), wrapped in
`crossbeam_utils::CachePadded` and shared via `Arc`. Each transformer:

```rust
let idx = in_claim.fetch_add(1, Ordering::Relaxed);
if idx >= N { break; }   // all work claimed → exit
// obtain exactly one input, transform, publish exactly one output
```

Exactly `N` items produced and `N` tickets claimed ⇒ every claiming transformer
eventually gets exactly one item. No check-then-pop race, no loss, no deadlock.

**All shared, contended counters must be `CachePadded`** (claim counter, ring
`enq_pos`/`deq_pos`).

### 6.A — `A`: full-size lock-free slot array

The payload store needs interior mutability with manual publication. Use:

```rust
struct Slots<Tp> { data: Vec<UnsafeCell<Option<Tp>>>, ready: Vec<AtomicU8> }
unsafe impl<Tp: Send> Sync for Slots<Tp> {}   // safety: each index has exactly
// one writer and one reader, ordered by `ready` (Release store / Acquire load)
```

Create, in main (untimed):

- `pslots: Slots<Vec<f32>>` of length `N` (`ready` init 0).
- `cval: Vec<AtomicU64>` of length `N` (stores `f64::to_bits`, no sentinel needed).
- `cready: Vec<AtomicU8>` of length `N`, init 0.

**Producer** `p` (writes by id — no shared write index, no contention):

```rust
for id in start..end {
    let buf: Vec<f32> = /* pool: take preallocated & fill in place; allocate: new */;
    unsafe { *pslots.data[id].get() = Some(buf); }   // plain write
    pslots.ready[id].store(1, Ordering::Release);     // publish
}
```

(For `pool` mode, keep the preallocated `Vec<f32>` inside the slot and fill it in
place, then still do the `Release` store; see §7.)

**Transformer:**

```rust
loop {
    let idx = in_claim.fetch_add(1, Ordering::Relaxed);
    if idx >= N { break; }
    while pslots.ready[idx].load(Ordering::Acquire) == 0 { spin_loop(); }
    let buf = unsafe { (*pslots.data[idx].get()).take().unwrap() };  // single reader
    let out = transform(&buf, I);
    cval[idx].store(out.to_bits(), Ordering::Relaxed);
    cready[idx].store(1, Ordering::Release);
}
```

**Consumer:**

```rust
let mut sum = 0.0f64;
for idx in 0..N {
    while cready[idx].load(Ordering::Acquire) == 0 { spin_loop(); }
    sum += f64::from_bits(cval[idx].load(Ordering::Relaxed));
}
let end = Instant::now();
```

Correctness: the `Release` store on `ready`/`cready` synchronizes-with the
`Acquire` load, so the preceding payload write is visible. Each slot is written by
exactly one thread and read by exactly one thread. **Never** use `read_volatile`/
`write_volatile` for synchronization — volatile in Rust is for MMIO and does
**not** establish happens-before; use atomics as shown.

### 6.R — `R`: Vyukov bounded MPMC ring buffer

Implement the canonical Vyukov bounded MPMC queue. Cell:

```rust
struct Cell<Tp> { seq: AtomicUsize, data: UnsafeCell<MaybeUninit<Tp>> }
struct Ring<Tp> { buf: Box<[Cell<Tp>]>, mask: usize,
                  enq: CachePadded<AtomicUsize>, deq: CachePadded<AtomicUsize> }
unsafe impl<Tp: Send> Sync for Ring<Tp> {}
```

`CAP = RING` (power of two, `mask = CAP-1`); cell `j.seq` initialized to `j`;
`enq = deq = 0`.

```rust
fn enqueue(&self, item: Tp) -> Result<(), Tp> {
    let mut pos = self.enq.load(Relaxed);
    loop {
        let cell = &self.buf[pos & self.mask];
        let seq = cell.seq.load(Acquire);
        let dif = seq as isize - pos as isize;
        if dif == 0 {
            if self.enq.compare_exchange_weak(pos, pos+1, Relaxed, Relaxed).is_ok() { 
                unsafe { (*cell.data.get()).write(item); }
                cell.seq.store(pos + 1, Release);
                return Ok(());
            }
        } else if dif < 0 { return Err(item); }      // full
        else { pos = self.enq.load(Relaxed); }
    }
}

fn dequeue(&self) -> Option<Tp> {
    let mut pos = self.deq.load(Relaxed);
    loop {
        let cell = &self.buf[pos & self.mask];
        let seq = cell.seq.load(Acquire);
        let dif = seq as isize - (pos + 1) as isize;
        if dif == 0 {
            if self.deq.compare_exchange_weak(pos, pos+1, Relaxed, Relaxed).is_ok() {
                let item = unsafe { (*cell.data.get()).assume_init_read() };
                cell.seq.store(pos + self.mask + 1, Release);   // pos + CAP
                return Some(item);
            }
        } else if dif < 0 { return None; }           // empty
        else { pos = self.deq.load(Relaxed); }
    }
}
```

Two rings: `in_ring: Ring<Vec<f32>>`, `out_ring: Ring<f64>`.

- **Producer:** `while in_ring.enqueue(buf).is_err() { spin_loop(); }` (rebind the
  returned buf on `Err`).
- **Transformer:** claim ticket; if `< N`, spin-`dequeue` one from `in_ring`,
  transform, spin-`enqueue` the `f64` to `out_ring`.
- **Consumer:** loop `N` times, spin-`dequeue` one `f64` from `out_ring`, add.

### 6.Q — queue-family variants

Two queues: `in` (producer→transformer, `Vec<f32>`) and `out`
(transformer→consumer, `f64`). Same claim-counter termination.

- **`Qbl`** (bounded, locking): hand-rolled `Mutex<VecDeque<Tp>> + Condvar`,
  capacity `RING`. `push` blocks on a `not_full` condvar when full; `pop` blocks on
  a `not_empty` condvar when empty. Implement carefully:
  - push: lock; `while q.len()==RING { not_full.wait(); }`; push_back; drop lock;
    `not_empty.notify_one()`.
  - pop: lock; `while q.is_empty() { not_empty.wait(); }`; pop_front; drop lock;
    `not_full.notify_one()`.
  Use `Condvar::wait_while` to avoid spurious-wakeup bugs.
- **`Qul`** (unbounded, locking): same, but no capacity limit and no `not_full`
  condvar (`push` never blocks).
- **`Qbf`** (bounded, lock-free): `crossbeam_channel::bounded(RING)`. Producer
  `send` (blocks when full → backpressure); transformer `recv`; consumer `recv`
  `N` times. `send`/`recv` block internally, so no manual spin needed.
- **`Quf`** (unbounded, lock-free): `crossbeam_channel::unbounded()`. Same
  `send`/`recv` usage.

For the locking variants, `Condvar::wait` blocks the thread; that is acceptable
(it is the nature of "classical heavyweight" concurrency and part of what we are
measuring). Do not convert them to spin.

Note: `std::sync::mpsc` is **single-consumer** and cannot serve the
producer→transformer stage (multiple transformers consume it), which is why the
lock-free channels use `crossbeam_channel` (MPMC).

## 7. Allocation modes (`ALLOC`)

- **`allocate`**: producers allocate each `Vec<f32>` (length `K`) inside the timed
  region.
- **`pool`**: main thread, before `start`, preallocates all `N` buffers. For `A`,
  put a `Vec<f32>` of length `K` into each `pslots` slot up front and have producers
  fill it in place (then `Release`-store the flag). For `R`/`Q`, keep a
  `Vec<Vec<f32>>` of length `N` indexed by id; producer fills `pool[id]` in place
  and moves/clones its handle onto the queue/ring as the message. (Because
  ownership must transfer through the ring/queue, `pool` mode for `R`/`Q` may need
  the buffer swapped out of the pool slot; simplest correct approach: pool holds
  `Option<Vec<f32>>`, producer `take`s it, fills, sends. The point is that the
  *allocation* happened before `start`.)

The publication handshake provides visibility for in-place writes exactly as in
`allocate` mode.

## 8. Timing details

- Use `std::time::Instant`. `start` in main immediately before spawning producers;
  `end` in the consumer immediately after the `N`-th output. Report
  `(end - start).as_nanos()`.
- Producer spawn cost is intentionally inside the measured region.
- No warmup logic beyond the `S` warmup runs.

## 9. Harness interface

The outer harness sets env vars and calls `rust/run.sh` once per configuration.
`run.sh` emits only `RESULT`/`---` on stdout and exits non-zero on error.

## 10. Implementation checklist (avoid these bugs)

- [ ] `in_claim.fetch_add`; transformers stop at `idx >= N`. No check-then-pop.
- [ ] Every contended counter is `CachePadded`.
- [ ] `A`/`R` payload published via `Release` store / `Acquire` load; each slot has
      exactly one writer and one reader. No `*_volatile` used for sync.
- [ ] `unsafe impl Sync` wrappers are the only `unsafe`, tightly scoped &
      commented; `MaybeUninit` read exactly once per cell occupancy.
- [ ] Condvar variants use `wait_while` / a predicate loop (no spurious-wakeup
      bug); channels use `crossbeam_channel` (MPMC), not `std::sync::mpsc`.
- [ ] Every spin loop calls `std::hint::spin_loop()`; no `thread::sleep`.
- [ ] `pool` preallocation + structure allocation before `start`.
- [ ] `RING` power of two for `R`.
- [ ] `test` mode: single run, all-ones, all methods print identical `sum`.
- [ ] Consumer captures `end`; main prints `RESULT <ns> <sum>` with `{:.6}`.
