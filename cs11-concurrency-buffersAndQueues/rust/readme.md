# CS11 Concurrency Micro-Benchmark - Rust Implementation

This folder contains the Rust implementation of the `producer -> transformer -> consumer` concurrency micro-benchmark.

## Build and Run

To run the benchmark directly via `run.sh`:

```bash
N=100000 K=64 I=8 P=1 T=4 M=A ALLOC=pool RING=1024 S=test ./run.sh
```

Supported methods (`M`):
- `B`: Baseline (serial execution, single thread)
- `A`: Full-size lock-free slot array (`UnsafeCell` + `AtomicU8` flags)
- `R`: Vyukov bounded MPMC ring buffer
- `Qbl`: Bounded locking queue (`Mutex<VecDeque>` + `Condvar`)
- `Qul`: Unbounded locking queue (`Mutex<VecDeque>` + `Condvar`)
- `Qbf`: Bounded lock-free queue (`crossbeam_channel::bounded`)
- `Quf`: Unbounded lock-free queue (`crossbeam_channel::unbounded`)

Supported allocation modes (`ALLOC`):
- `allocate`: Messages allocated per producer turn inside the timed region
- `pool`: Message buffers preallocated prior to timing
