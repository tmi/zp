#!/usr/bin/env bash
set -euo pipefail

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
# shellcheck disable=SC2034
IMPLS_java=(B A R Qbl Qul Quf)
# shellcheck disable=SC2034
IMPLS_rust=(B A R Qbl Qbf Qul Quf)
# shellcheck disable=SC2034
IMPLS_c=(B A R)
# shellcheck disable=SC2034
IMPLS_python_nogil=(B A R Qbl Qul)

# ---- usage / validation ----
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <lang> <scenario>" >&2
  echo "  <lang>: java | rust | c | python-nogil" >&2
  echo "  <scenario>: test | spst | spmt | mpmt" >&2
  exit 1
fi

lang="$1"
scenario="$2"

case "$lang" in
  java|rust|c|python-nogil) ;;
  *)
    echo "Error: unsupported language '$lang'" >&2
    echo "Usage: $0 <lang> <scenario>" >&2
    exit 1
    ;;
esac

case "$scenario" in
  test|spst|spmt|mpmt) ;;
  *)
    echo "Error: unsupported scenario '$scenario'" >&2
    echo "Usage: $0 <lang> <scenario>" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RUN_SH="$PROJECT_ROOT/$lang/run.sh"

if [ ! -x "$RUN_SH" ]; then
  echo "Error: runner script '$RUN_SH' not found or not executable" >&2
  exit 1
fi

# Determine impl array for chosen language
key="IMPLS_${lang//-/_}"
impls=()
# shellcheck disable=SC2154
eval "impls=(\"\${${key}[@]}\")"

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/${lang}-${scenario}.txt"
: > "$RESULTS_FILE"

run_cfg() {
  local m="$1" alloc="$2" n="$3" k="$4" i="$5" p="$6" t="$7" s="$8"
  echo "Running $lang $scenario: M=$m ALLOC=$alloc N=$n K=$k I=$i P=$p T=$t RING=$RING S=$s" >&2
  echo "# CFG lang=$lang scenario=$scenario M=$m ALLOC=$alloc N=$n K=$k I=$i P=$p T=$t RING=$RING S=$s" >> "$RESULTS_FILE"

  N="$n" K="$k" I="$i" P="$p" T="$t" M="$m" ALLOC="$alloc" RING="$RING" S="$s" SEED="$SEED" "$RUN_SH" >> "$RESULTS_FILE"
}

case "$scenario" in
  test)
    for m in "${impls[@]}"; do
      for alloc in "${ALLOC_SWEEP[@]}"; do
        run_cfg "$m" "$alloc" "$TEST_N" "$TEST_K" "$TEST_I" "$TEST_P" "$TEST_T" "test"
      done
    done
    ;;

  spst)
    run_cfg "B" "allocate" "$SPST_N" "$SPST_K" "$SPST_I" "$SPST_P" "$SPST_T" "$SUITE"
    for m in "${impls[@]}"; do
      if [ "$m" = "B" ]; then continue; fi
      for alloc in "${ALLOC_SWEEP[@]}"; do
        run_cfg "$m" "$alloc" "$SPST_N" "$SPST_K" "$SPST_I" "$SPST_P" "$SPST_T" "$SUITE"
      done
    done
    ;;

  spmt)
    for k in "${K_SWEEP[@]}"; do
      for i in "${I_SWEEP[@]}"; do
        run_cfg "B" "allocate" "$SPMT_N" "$k" "$i" "$SPMT_P" "1" "$SUITE"
        for t in "${SPMT_T_SWEEP[@]}"; do
          for m in "${impls[@]}"; do
            if [ "$m" = "B" ]; then continue; fi
            for alloc in "${ALLOC_SWEEP[@]}"; do
              run_cfg "$m" "$alloc" "$SPMT_N" "$k" "$i" "$SPMT_P" "$t" "$SUITE"
            done
          done
        done
      done
    done
    ;;

  mpmt)
    for k in "${K_SWEEP[@]}"; do
      for i in "${I_SWEEP[@]}"; do
        run_cfg "B" "allocate" "$MPMT_N" "$k" "$i" "$MPMT_P" "$MPMT_T" "$SUITE"
        for m in "${impls[@]}"; do
          if [ "$m" = "B" ]; then continue; fi
          for alloc in "${ALLOC_SWEEP[@]}"; do
            run_cfg "$m" "$alloc" "$MPMT_N" "$k" "$i" "$MPMT_P" "$MPMT_T" "$SUITE"
          done
        done
      done
    done
    ;;
esac

cd "$SCRIPT_DIR"
exec uv run python analyze.py "$RESULTS_FILE" "$scenario"
