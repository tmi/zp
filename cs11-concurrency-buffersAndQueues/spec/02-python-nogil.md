# Concurrency micro-benchmark — Python (free-threaded / nogil) implementation spec

## 0. What this is (read first)

This is an **artificial concurrency micro-benchmark**, not production code, and the
Python port is explicitly a **for-fun** experiment: the point is to see whether a
free-threaded (nogil) CPython build lets a `producer → transformer → consumer`
pipeline scale at all. "For fun" does **not** mean "sloppy about races" — the
coordination must be genuinely correct, which is why we use a real atomics
library rather than relying on interpreter internals.

Because it is artificial:

- Do **not** add logging, config frameworks, graceful shutdown, or abstractions.
  Keep it blunt.
- Do **not** try to "win" by exploiting CPython internals beyond what a competent
  engineer writes. Do not deliberately pessimize either.
- Busy-wait (spin) is expected and preferred over sleeping. Assume enough cores
  for `P + T + 1` threads.
- Use **manual threads** (`threading.Thread`), no pools/executors.

Target: a **free-threaded CPython 3.13+** interpreter (the `t`/`freethreaded`
build) with the GIL disabled. The program must assert this at startup:
`assert not sys._is_gil_enabled()` (fail loudly otherwise).

Dependencies: exactly one third-party library, an atomics library that provides
atomic integers with explicit memory ordering. Use **`atomics`** (PyPI package
`atomics`). Pin it to an exact version in `pyproject.toml`. `type`-check with `ty`
pinned to an exact version. Use `uv` for everything.

## 1. Folder layout

Create a folder `python-nogil/` as a sibling of this `spec/` folder. It is
**completely self-contained** — shares nothing with the other language folders.

```
python-nogil/
  run.sh
  pyproject.toml     # requires-python 3.13t; deps: atomics==<pin>; dev: ruff, ty==<pin>, pytest
  bench.py
```

`pyproject.toml` must select a free-threaded interpreter. With `uv` this is done
via a free-threaded Python (e.g. `uv python pin 3.13t` / `requires-python`), and
`run.sh` must launch that interpreter.

`run.sh` (thin wrapper for the harness):

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
exec uv run --python 3.13t python bench.py
```

(Adjust the `--python` selector to whatever pins the free-threaded build in this
environment. `run.sh` must print only the program's stdout to stdout.)

## 2. Parameters (all via environment variables)

| Var     | Meaning                                          | Example   |
|---------|--------------------------------------------------|-----------|
| `N`     | number of messages                               | `200000`  |
| `K`     | floats per message                               | `64`      |
| `I`     | transformer complexity (max norm order)          | `8`       |
| `P`     | producer thread count                            | `1`       |
| `T`     | transformer thread count                         | `4`       |
| `M`     | method: `B A R Qbl Qul` (see §6)                 | `A`       |
| `ALLOC` | `allocate` or `pool` (see §7)                    | `pool`    |
| `RING`  | ring / bounded queue capacity, power of two      | `1024`    |
| `S`     | suite: `test` or `<warmup>/<real>`               | `3/10`    |
| `SEED`  | RNG seed (optional, default `42`)                | `42`      |

Read with `os.environ`. Missing required var → print to `sys.stderr` and
`sys.exit(1)`. `RING` required for `R` and `Qbl`; power of two for `R`. Note
Python is slow, so keep `N` smaller than in the compiled ports.

## 3. Message, transform, and the correctness oracle

- A **message** is a `list[float]` of length `K` (a plain Python list; do not use
  `numpy` — that would vectorize away the very cost we want to measure).
- The **producer** for id `i` fills it:
  - **all-ones** (`S == test`): every element `1.0`.
  - **random** (otherwise): a cheap inline xorshift (integer state → float in
    `[0,1)`) seeded from `SEED + producer_index`. Do not use `random.Random` in
    the hot loop. Keep RNG negligible vs transform.
- The **transformer** maps `list[float] -> float`:

```python
def transform(x: list[float], big_i: int) -> float:
    total = 0.0
    for i in range(1, big_i + 1):
        s = 0.0
        for v in x:
            s += abs(v) ** i          # runtime exponent
        total += s ** (1.0 / i)
    return total
```

  Use `** i` with a **runtime** exponent (no special-casing small `i`).

- The **consumer** keeps a running `float` (Python float = C double) sum.

**Oracle:** with all-ones input every message output is exactly
`v = Σ_{i=1..I} K**(1/i)`. Summing `N` bit-identical floats is order-independent,
so **every method prints the same `sum`** in `test` mode.

## 4. Output format (exactly this)

Per run, one line to stdout:

```
RESULT <elapsed_ns> <sum>
```

- `<elapsed_ns>`: integer nanoseconds from `time.perf_counter_ns()` (see §8).
- `<sum>`: final float, formatted `f"{sum:.6f}"`.

`S == test`: one run, all-ones, single `RESULT` line.
`S == "<W>/<R>"`: `W` warmup runs, a line with exactly `---`, then `R` measured
runs; each run prints a `RESULT`. Warmup+measured use **random** input. Each run
fully independent (fresh structures/threads; `pool` preallocated fresh per run in
the main thread before timing). Only `RESULT`/`---` on stdout; diagnostics →
stderr.

## 5. Threading & timing protocol

1. Main thread does all setup for the run (allocate structures; if `ALLOC==pool`
   preallocate the `N` buffers). **Untimed.**
2. Create+start the **consumer** (1) and **transformer** (`T`) threads; they begin
   spinning/blocking on empty input.
3. `start = time.perf_counter_ns()`.
4. Create+start the **producer** (`P`) threads.
5. The consumer, after consuming exactly `N` outputs, records
   `end = time.perf_counter_ns()` into a shared holder.
6. Main `join()`s all threads and prints `RESULT (end - start) sum`.

Producer `p` owns id range `[p*N//P, (p+1)*N//P)` (exact partition).

**Baseline `B`**: no threads. `start`; serial `for i in range(N): sum +=
transform(produce(i))`; `end`; print. (`pool` preallocation before `start`.)

Spin-wait loops should be tight (`while flag.load() == 0: pass`). Do **not** call
`time.sleep`. (There is no CPython spin-hint intrinsic; a bare `pass` loop is the
intended busy-wait here.)

## 6. Methods

The hand-rolled methods (`A`, `R`) and the queue methods share the **same
termination mechanism**: one shared atomic **claim counter** `in_claim` (init 0),
an `atomics` atomic integer. Each transformer:

```python
idx = in_claim.fetch_inc()        # returns previous value; atomic
if idx >= N:
    break
# obtain exactly one input, transform, publish exactly one output
```

Exactly `N` items produced and `N` tickets claimed ⇒ each claiming transformer
gets exactly one item. No check-then-pop race, no loss, no deadlock.

Create atomics like `in_claim = atomics.atomic(width=8, atype=atomics.INT)` and use
`fetch_inc()`, `.load()`, `.store()`, `.cmpxchg_weak()`; pass `atomics.MemoryOrder`
where the algorithm needs acquire/release (defaults are sequentially consistent,
which is always safe here). Consult the `atomics` API for exact method names.

False sharing is not a meaningful concern at Python speed; you do not need manual
padding, but keep each shared atomic as its own object.

### 6.A — `A`: full-size lock-free slot array

Allocate (main, untimed) length-`N` structures:

- `pbuf: list[object]` — message lists, one slot per id (init `[None]*N`).
- `pready: list` — one `atomics.atomic(width=1, atype=INT)` per id, init 0.
- `cval: list[float]` — outputs per id (init `[0.0]*N`).
- `cready: list` — one atomic per id, init 0.

**Producer** `p` (writes by id — no shared write index):

```python
for id in range(start, end):
    b = pbuf[id] if alloc_pool else [0.0]*K
    fill(b, id)
    pbuf[id] = b                              # plain list store
    pready[id].store(1)                       # release/seq-cst publish
```

**Transformer:**

```python
while True:
    idx = in_claim.fetch_inc()
    if idx >= N:
        break
    while pready[idx].load() == 0:            # acquire/seq-cst
        pass
    out = transform(pbuf[idx], I)
    cval[idx] = out                           # plain list store
    cready[idx].store(1)                      # publish
```

**Consumer:**

```python
sum_ = 0.0
for idx in range(N):
    while cready[idx].load() == 0:
        pass
    sum_ += cval[idx]
end = time.perf_counter_ns()
```

Correctness note: the seq-cst store on `pready[idx]`/`cready[idx]` acts as a full
barrier and orders the preceding list-element store; the reader observes the flag
with an acquiring load before reading the payload. Each slot has exactly one
writer and one reader. Do **not** rely on "the GIL makes it safe" — there is no
GIL; correctness comes from the atomic flags. (CPython's free-threaded build
guarantees object-reference stores are themselves indivisible, so the payload
reference is never torn; the atomic flag provides the ordering.)

### 6.R — `R`: Vyukov bounded MPMC ring buffer

Implement the canonical Vyukov bounded MPMC queue with `atomics` per-cell sequence
numbers. Two rings: `in_ring` (payload = message list), `out_ring` (payload =
float). `CAP = RING` (power of two, `mask = CAP-1`).

Each cell = a payload slot in a `list` plus one `atomics.atomic(width=8,
atype=INT)` `seq`, cell `j` init to `j`. Padded `enq_pos`, `deq_pos` atomics init 0.

```python
def enqueue(item) -> bool:
    pos = enq_pos.load()
    while True:
        j = pos & mask
        dif = seq[j].load() - pos                 # acquire
        if dif == 0:
            if enq_pos.cmpxchg_weak(pos, pos + 1): # returns (ok, expected)
                data[j] = item
                seq[j].store(pos + 1)              # release
                return True
            # on failure cmpxchg updates pos to current
        elif dif < 0:
            return False                           # full
        else:
            pos = enq_pos.load()

def dequeue():   # returns (True, item) or (False, None)
    pos = deq_pos.load()
    while True:
        j = pos & mask
        dif = seq[j].load() - (pos + 1)
        if dif == 0:
            if deq_pos.cmpxchg_weak(pos, pos + 1):
                item = data[j]
                seq[j].store(pos + CAP)            # release
                return True, item
        elif dif < 0:
            return False, None                     # empty
        else:
            pos = deq_pos.load()
```

(Match the exact `cmpxchg` signature of the `atomics` library; the essential logic
is the Vyukov `dif`-based state machine above.)

- **Producer:** `while not in_ring.enqueue(b): pass`
- **Transformer:** claim ticket; if `< N`, spin-`dequeue` one from `in_ring`,
  transform, spin-`enqueue` the float to `out_ring`.
- **Consumer:** loop `N` times, spin-`dequeue` a float from `out_ring`, add.

### 6.Q — queue-family variants (stdlib `queue.Queue`)

Two `queue.Queue` instances: `qin` (producer→transformer), `qout`
(transformer→consumer). `queue.Queue` is thread-safe (lock+condition based) and
correct under nogil. Same claim-counter termination.

- **`Qbl`** (bounded, locking): `queue.Queue(maxsize=RING)` for both. Producer
  `qin.put(b)` (blocks when full → backpressure); transformer after claiming does
  `qin.get()`, then `qout.put(out)`; consumer `qout.get()` `N` times.
- **`Qul`** (unbounded, locking): `queue.Queue()` (no maxsize) for both; same usage.

**Skip the lock-free queue variants (`Qbf`, `Quf`) explicitly** — the Python
stdlib has no lock-free queue, and `A`/`R` already cover the lock-free case for
this for-fun port. If `M` is `Qbf`/`Quf`, print an error to stderr and exit
non-zero.

`queue.Queue.get()`/`put()` block, so no manual spin is needed for the Q variants.

## 7. Allocation modes (`ALLOC`)

- **`allocate`**: producers create each `list[float]` of length `K` inside the
  timed region.
- **`pool`**: main thread, before `start`, preallocates all `N` lists. For `A`,
  fill `pbuf` with `[0.0]*K` lists up front and have producers overwrite elements
  in place. For `R`/`Q`, keep a `pool: list[list[float]]` of length `N` indexed by
  id; producer fills `pool[id]` in place and passes it on. No list creation in the
  timed region.

## 8. Timing details

Use `time.perf_counter_ns()` only. `start` in main immediately before creating
producer threads; `end` in the consumer immediately after the `N`-th output.
Report `end - start`. Producer thread-creation cost is intentionally measured. No
warmup logic beyond the `S` warmup runs.

## 9. Harness interface

The outer harness sets env vars and calls `python-nogil/run.sh` once per
configuration. `run.sh` emits only `RESULT`/`---` on stdout and exits non-zero on
error (including the free-threaded assertion and unsupported `M`).

## 10. Implementation checklist (avoid these bugs)

- [ ] Startup asserts `not sys._is_gil_enabled()`.
- [ ] `in_claim.fetch_inc()`; transformers stop at `idx >= N`. No check-then-pop.
- [ ] `A`/`R` publish via atomic store / observe via atomic load; each slot has
      exactly one writer and one reader. No reliance on the GIL for correctness.
- [ ] Vyukov `seq` init `= j`; dequeue stores `pos + CAP`; `RING` power of two.
- [ ] `Q` variants use `queue.Queue`; `Qbf`/`Quf` refused.
- [ ] Spin loops are bare `while ...: pass`; no `time.sleep`; no `numpy`.
- [ ] `pool` preallocation before `start`.
- [ ] `atomics` pinned exactly; `ty` pinned exactly; type annotations everywhere.
- [ ] `test` mode: single run, all-ones, all methods print identical `sum`.
- [ ] Consumer captures `end`; main prints `RESULT (end-start) sum` with `.6f`.
