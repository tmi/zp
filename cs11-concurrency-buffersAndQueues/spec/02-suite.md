# Benchmark suite spec — `suite/`

## 0. Purpose

`suite/` drives the per-language benchmark binaries, sweeps parameters, collects
their stdout into a results file, and runs a small Python script to summarize and
plot. It contains no benchmark logic itself — it only sets environment variables
and invokes each language's uniform entry point.

This is tooling for an **artificial** benchmark: keep it simple and hackable. All
tunable knobs live at the **top** of `suite.sh` so they are trivial to change.

## 1. Folder layout

```
suite/
  suite.sh          # the driver (this spec)
  analyze.py        # parsing, summary, plotting
  pyproject.toml    # uv project; deps: matplotlib; dev: ruff, ty==<pin>, pytest
  results/          # generated: <lang>-<scenario>.txt and *.png (gitignored)
```

`suite/` is a sibling of `spec/`, `java/`, `rust/`, `c/`, `python-nogil/`.

## 2. Uniform per-language interface (assumed)

Each language folder exposes `./<lang>/run.sh`, a thin wrapper that reads all
parameters from environment variables, builds if necessary (build noise → stderr),
runs exactly one process, and prints **only** these lines to stdout:

- one `RESULT <elapsed_ns> <sum>` line per run, and
- a single `---` line separating warmup runs from measured runs (only when
  `S = "<W>/<R>"`; absent when `S = test`).

`run.sh` exits non-zero on any error (bad params, unsupported method, etc.).

Environment variables consumed by every `run.sh`: `N K I P T M ALLOC RING S
SEED`. Language `<lang>` is one of `java rust c python-nogil`.

## 3. Invocation

```
./suite.sh <lang> <scenario>
```

- `<lang>` ∈ `java | rust | c | python-nogil`
- `<scenario>` ∈ `test | spst | spmt | mpmt`

Example: `./suite.sh java test`, `./suite.sh rust spmt`.

The script writes/overwrites `results/<lang>-<scenario>.txt`, then calls
`uv run python analyze.py results/<lang>-<scenario>.txt <scenario>`.

## 4. Configuration block (top of `suite.sh`)

All of the following are variables at the very top of the file for easy editing.

```bash
# ---- static params (not swept) ----
WARMUP=3
REAL=10
SUITE="${WARMUP}/${REAL}"       # value of S for measured scenarios
SEED=42
RING=1024                       # ring / bounded-queue capacity (power of two)

# ---- sweep ranges (edit here) ----
K_SWEEP=(16 64 256)             # message length sweep (spmt, mpmt)
I_SWEEP=(2 8 32)                # transformer complexity sweep (spmt, mpmt)
ALLOC_SWEEP=(allocate pool)

# ---- per-scenario fixed params ----
# test : correctness only, all-ones, one run each
TEST_N=100000; TEST_K=64; TEST_I=4; TEST_P=2; TEST_T=4

# spst : baseline vs concurrency, 1 producer / 1 transformer, single K/I point
SPST_N=2000000; SPST_K=64; SPST_I=8; SPST_P=1; SPST_T=1

# spmt : a few transformers, K/I swept
SPMT_N=2000000; SPMT_P=1; SPMT_T_SWEEP=(2 4)

# mpmt : "bigger machine", K/I swept
MPMT_N=8000000; MPMT_P=4; MPMT_T=20

# ---- methods supported per language (edit if a port adds/drops a variant) ----
IMPLS_java=(B A R Qbl Qul Quf)
IMPLS_rust=(B A R Qbl Qbf Qul Quf)
IMPLS_c=(B A R)
IMPLS_python_nogil=(B A R Qbl Qul)
```

Notes:
- The baseline `B` ignores `P`, `T`, `RING`, `ALLOC=pool/allocate` differences
  except that `pool` still preallocates; include `B` once per (K,I) with
  `ALLOC=allocate` only (no need to sweep `ALLOC` or `T` for baseline — see §5).
- Select the impl list for the chosen language via indirect expansion, e.g.
  `key="IMPLS_${lang//-/_}"; impls=$(eval echo \${$key[@]})` (map `python-nogil`
  → `IMPLS_python_nogil`).

## 5. Scenario definitions (what `suite.sh` loops over)

For each configuration the script:
1. exports the env vars,
2. echoes a machine-parseable header line to the results file:
   `# CFG lang=<l> scenario=<s> M=<m> ALLOC=<a> N=<n> K=<k> I=<i> P=<p> T=<t> RING=<r> S=<S>`
3. appends the stdout of `./<lang>/run.sh` to the results file.

Truncate the results file at the start of the run.

### `test` (correctness)
- `S=test` (single all-ones run per config), `N=TEST_N`, `K=TEST_K`, `I=TEST_I`,
  `P=TEST_P`, `T=TEST_T`.
- Sweep: every supported `M` × `ALLOC_SWEEP`.
- Goal: `analyze.py` verifies every printed `sum` is identical.

### `spst` (how bad vs baseline)
- `S=SUITE`, `N=SPST_N`, `K=SPST_K`, `I=SPST_I`, `P=1`, `T=1`.
- Sweep: every supported `M` × `ALLOC_SWEEP`. (No K/I sweep.)
- `B` runs once (`ALLOC=allocate`).

### `spmt` (does a few transformers beat baseline)
- `S=SUITE`, `N=SPMT_N`, `P=1`.
- Sweep: `T ∈ SPMT_T_SWEEP` × `K ∈ K_SWEEP` × `I ∈ I_SWEEP` × supported `M`
  (excluding `B`) × `ALLOC_SWEEP`.
- Plus `B` once per (K,I) with `ALLOC=allocate` (baseline reference for each work
  point).

### `mpmt` ("bigger machine")
- `S=SUITE`, `N=MPMT_N`, `P=MPMT_P`, `T=MPMT_T`.
- Sweep: `K ∈ K_SWEEP` × `I ∈ I_SWEEP` × supported `M` (excluding `B`) ×
  `ALLOC_SWEEP`.
- Plus `B` once per (K,I) with `ALLOC=allocate`.

Keep the nested loops readable; emit a progress line per config to stderr so long
sweeps show life.

## 6. Results file format

Plain text, append-only. A block per config:

```
# CFG lang=rust scenario=spmt M=A ALLOC=pool N=2000000 K=64 I=8 P=1 T=4 RING=1024 S=3/10
RESULT 123456789 640.000000
RESULT 120001111 640.000000
RESULT 119880000 640.000000
---
RESULT 118500000 640.000000
RESULT 118700000 640.000000
...
```

For `test` scenario blocks there is no `---` and a single `RESULT` line.

## 7. `analyze.py`

Invoked as `python analyze.py <results_file> <scenario>`. Behavior:

- **Parse** the file into records: each `# CFG ...` header starts a new record;
  collect the `RESULT` lines; treat lines after `---` as *measured* (before `---`
  as *warmup*, discarded). For `test`, the single `RESULT` is the measured value.
- Store per record: the parsed CFG dict, list of measured `elapsed_ns`, and the
  `sum` (they must all be equal within a record; assert so).

### If `scenario == test`
- Collect the `sum` of every record. Group by `(K,I,N)` (they are constant here,
  so effectively one group). Assert **all sums are exactly equal**; print
  `PASS: all <count> methods agree, sum=<v>` or `FAIL` listing each differing
  `(M, ALLOC, sum)`. Exit non-zero on FAIL. No plots.

### If `scenario ∈ {spst, spmt, mpmt}`
- For each record compute `median_ns` of the measured runs and
  `throughput = N / (median_ns / 1e9)` (messages/sec).
- **Summary table** to stdout: columns `M ALLOC K I P T median_ms throughput`,
  sorted by (K, I, throughput desc). Also print, per (K,I) group, the speedup of
  each method vs the `B` baseline at that (K,I): `throughput(M) /
  throughput(B)` — this is the headline number (>1 means beats baseline).
- **Plots** (matplotlib, save PNGs into `results/`):
  - `spst`: a single bar chart, x = `M×ALLOC`, y = throughput, with a horizontal
    line at the baseline `B` throughput. File `results/<lang>-spst.png`.
  - `spmt` / `mpmt`: for each `T` value (spmt) / overall (mpmt), a grid of small
    line charts, one per method family, x = work = `K*I` (log scale), y =
    throughput, one line per method, plus a baseline line. Or, simpler and
    acceptable: one figure per (K,I) with grouped bars over methods and a baseline
    line. File(s) `results/<lang>-<scenario>[-T<t>].png`.
- Keep plotting code short; do not over-engineer styling.

Use only `matplotlib` + stdlib. Type-annotate; `ty`-clean; `ruff`-clean.

## 8. `justfile` (project convention)

Provide `suite/justfile` (and each language folder may have its own) with at least:
- `val`: `uv run ruff check . && uv run ty check . && uv run pytest -q` for
  `analyze.py`; plus `shellcheck suite.sh` if available.
- `clean`: remove `results/` PNGs/txt and Python caches.

## 9. Suite implementation checklist

- [ ] All static params and sweep ranges are variables at the top of `suite.sh`.
- [ ] Two positional args: `<lang> <scenario>`; validate both, usage on error.
- [ ] Impl list chosen by language; `B` included appropriately (once per work
      point, `ALLOC=allocate`).
- [ ] Results file truncated at start; `# CFG` header before each block; run.sh
      stdout appended verbatim.
- [ ] Progress to stderr, never polluting the results file.
- [ ] `analyze.py` run at the end; `test` asserts equal sums, others summarize +
      plot.
- [ ] Nothing here depends on any single language's internals — only `run.sh` +
      the `RESULT`/`---` contract.
