# CS11 Concurrency Micro-Benchmark - Java Implementation

This folder contains the Java implementation of the `producer -> transformer -> consumer` concurrency micro-benchmark.

## Build and Run

To run the benchmark directly via `run.sh`:

```bash
N=100000 K=64 I=8 P=1 T=4 M=A ALLOC=pool RING=1024 S=test ./run.sh
```

Supported methods (`M`):
- `B`: Baseline (single-threaded / serial execution)
- `A`: Full-size lock-free slot array with AtomicIntegerArray publication flags
- `R`: Vyukov bounded MPMC ring buffer
- `Qbl`: Bounded locking queue (`ArrayBlockingQueue`)
- `Qul`: Unbounded locking queue (`LinkedBlockingQueue`)
- `Quf`: Unbounded lock-free queue (`ConcurrentLinkedQueue`)

Supported allocation modes (`ALLOC`):
- `allocate`: Messages allocated per producer turn
- `pool`: Message buffers preallocated prior to timing
