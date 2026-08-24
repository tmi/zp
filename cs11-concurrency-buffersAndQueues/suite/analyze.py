#!/usr/bin/env python3
import math
import os
import statistics
import sys
from dataclasses import dataclass
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


@dataclass
class Record:
    cfg: dict[str, Any]
    measured_elapsed_ns: list[float]
    sum_val: float


def parse_cfg_line(line: str) -> dict[str, Any]:
    parts = line.strip().split()[2:]
    cfg: dict[str, Any] = {}
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            if k in ("N", "K", "I", "P", "T", "RING"):
                cfg[k] = int(v)
            else:
                cfg[k] = v
    return cfg


def parse_results(filepath: str, scenario: str) -> list[Record]:
    records: list[Record] = []
    current_cfg: dict[str, Any] | None = None
    measured_elapsed_ns: list[float] = []
    current_sum: float | None = None
    saw_sep = False

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("# CFG "):
                if current_cfg is not None and measured_elapsed_ns:
                    assert current_sum is not None
                    records.append(Record(current_cfg, measured_elapsed_ns, current_sum))
                current_cfg = parse_cfg_line(line)
                measured_elapsed_ns = []
                current_sum = None
                saw_sep = False

            elif line == "---":
                saw_sep = True

            elif line.startswith("RESULT "):
                parts = line.split()
                if len(parts) >= 3:
                    elapsed_ns = float(parts[1])
                    sum_val = float(parts[2])

                    if scenario == "test" or saw_sep:
                        measured_elapsed_ns.append(elapsed_ns)
                        if current_sum is None:
                            current_sum = sum_val
                        else:
                            assert math.isclose(current_sum, sum_val, rel_tol=1e-5), (
                                f"Sum mismatch within record: {current_sum} vs {sum_val}"
                            )

    if current_cfg is not None and measured_elapsed_ns:
        assert current_sum is not None
        records.append(Record(current_cfg, measured_elapsed_ns, current_sum))

    return records


def analyze_test(records: list[Record]) -> int:
    if not records:
        print("FAIL: No records found")
        return 1

    first_sum = records[0].sum_val
    all_agree = True
    differing: list[tuple[str, str, float]] = []

    for r in records:
        if not math.isclose(r.sum_val, first_sum, rel_tol=1e-5):
            all_agree = False
            differing.append((str(r.cfg.get("M")), str(r.cfg.get("ALLOC")), r.sum_val))

    if all_agree:
        print(f"PASS: all {len(records)} methods agree, sum={first_sum:.6f}")
        return 0
    else:
        print("FAIL: differing methods found:")
        for m, alloc, s in differing:
            print(f"  M={m} ALLOC={alloc} sum={s:.6f}")
        return 1


def plot_spst(records: list[Record], lang: str, results_dir: str) -> None:
    non_b = [r for r in records if r.cfg.get("M") != "B"]
    b_rec = next((r for r in records if r.cfg.get("M") == "B"), None)

    if not non_b:
        return

    labels = [f"{r.cfg['M']}/{r.cfg['ALLOC']}" for r in non_b]
    throughputs = [r.cfg["N"] / (statistics.median(r.measured_elapsed_ns) / 1e9) for r in non_b]

    plt.figure(figsize=(10, 6))
    plt.bar(labels, throughputs, color="skyblue", edgecolor="navy")

    if b_rec is not None:
        b_tp = b_rec.cfg["N"] / (statistics.median(b_rec.measured_elapsed_ns) / 1e9)
        plt.axhline(y=b_tp, color="red", linestyle="--", label=f"Baseline B ({b_tp/1e6:.1f}M msg/s)")
        plt.legend()

    plt.title(f"{lang.upper()} - spst (Single Producer / Single Transformer)")
    plt.xlabel("Method / Allocation")
    plt.ylabel("Throughput (msg/s)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = os.path.join(results_dir, f"{lang}-spst.png")
    plt.savefig(out_path)
    plt.close()


def plot_grid(records: list[Record], lang: str, scenario: str, t_val: int | None, results_dir: str) -> None:
    work_points = sorted({(r.cfg["K"], r.cfg["I"]) for r in records})
    if not work_points:
        return

    num_points = len(work_points)
    ncols = min(3, num_points)
    nrows = math.ceil(num_points / ncols)

    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), squeeze=False)

    for idx, (k, i) in enumerate(work_points):
        r_idx = idx // ncols
        c_idx = idx % ncols
        ax = axes[r_idx][c_idx]

        point_recs = [r for r in records if r.cfg["K"] == k and r.cfg["I"] == i]
        b_rec = next((r for r in point_recs if r.cfg.get("M") == "B"), None)
        non_b = [r for r in point_recs if r.cfg.get("M") != "B"]

        if not non_b:
            continue

        labels = [f"{r.cfg['M']}/{r.cfg['ALLOC']}" for r in non_b]
        throughputs = [r.cfg["N"] / (statistics.median(r.measured_elapsed_ns) / 1e9) for r in non_b]

        ax.bar(labels, throughputs, color="skyblue", edgecolor="navy")

        if b_rec is not None:
            b_tp = b_rec.cfg["N"] / (statistics.median(b_rec.measured_elapsed_ns) / 1e9)
            ax.axhline(y=b_tp, color="red", linestyle="--", label="Baseline B")

        ax.set_title(f"K={k}, I={i}")
        ax.set_ylabel("Throughput (msg/s)")
        ax.tick_params(axis="x", rotation=45)

    # Hide unused subplots
    for idx in range(num_points, nrows * ncols):
        r_idx = idx // ncols
        c_idx = idx % ncols
        axes[r_idx][c_idx].set_visible(False)

    title_str = f"{lang.upper()} - {scenario}" + (f" (T={t_val})" if t_val is not None else "")
    fig.suptitle(title_str, fontsize=14)
    fig.tight_layout()

    filename = f"{lang}-{scenario}-T{t_val}.png" if t_val is not None else f"{lang}-{scenario}.png"
    out_path = os.path.join(results_dir, filename)
    plt.savefig(out_path)
    plt.close()


def analyze_measured(records: list[Record], scenario: str, results_dir: str) -> None:
    if not records:
        print("No records to analyze.")
        return

    lang = records[0].cfg.get("lang", "unknown")

    # Find baseline throughputs per (K, I)
    baselines: dict[tuple[int, int], float] = {}
    for r in records:
        if r.cfg.get("M") == "B":
            k, i = r.cfg["K"], r.cfg["I"]
            median_ns = statistics.median(r.measured_elapsed_ns)
            baselines[(k, i)] = r.cfg["N"] / (median_ns / 1e9)

    # Prepare summary data
    table_rows: list[dict[str, Any]] = []
    for r in records:
        median_ns = statistics.median(r.measured_elapsed_ns)
        median_ms = median_ns / 1_000_000.0
        tp = r.cfg["N"] / (median_ns / 1e9)
        k, i = r.cfg["K"], r.cfg["I"]
        b_tp = baselines.get((k, i))
        speedup = (tp / b_tp) if b_tp else 1.0

        table_rows.append({
            "M": r.cfg.get("M"),
            "ALLOC": r.cfg.get("ALLOC"),
            "K": k,
            "I": i,
            "P": r.cfg.get("P"),
            "T": r.cfg.get("T"),
            "median_ms": median_ms,
            "throughput": tp,
            "speedup": speedup,
        })

    # Sort table by (K, I, throughput desc)
    table_rows.sort(key=lambda row: (row["K"], row["I"], -row["throughput"]))

    print("=" * 90)
    print(f"SUMMARY TABLE ({lang.upper()} - {scenario})")
    print("=" * 90)
    header = f"{'M':<6} {'ALLOC':<10} {'K':<6} {'I':<6} {'P':<4} {'T':<4} {'median_ms':<12} {'throughput (msg/s)':<20} {'speedup vs B':<12}"
    print(header)
    print("-" * 90)

    for row in table_rows:
        print(
            f"{row['M']:<6} {row['ALLOC']:<10} {row['K']:<6} {row['I']:<6} {row['P']:<4} {row['T']:<4} "
            f"{row['median_ms']:<12.2f} {row['throughput']:<20,.0f} {row['speedup']:<12.2f}x"
        )
    print("=" * 90)

    # Generate plots
    if scenario == "spst":
        plot_spst(records, lang, results_dir)
    elif scenario == "spmt":
        ts = sorted({r.cfg["T"] for r in records if r.cfg.get("M") != "B"})
        for t_val in ts:
            t_records = [r for r in records if r.cfg.get("M") == "B" or r.cfg.get("T") == t_val]
            plot_grid(t_records, lang, scenario, t_val=t_val, results_dir=results_dir)
    elif scenario == "mpmt":
        plot_grid(records, lang, scenario, t_val=None, results_dir=results_dir)


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: analyze.py <results_file> <scenario>")
        sys.exit(1)

    results_file = sys.argv[1]
    scenario = sys.argv[2]

    if not os.path.exists(results_file):
        print(f"Error: results file '{results_file}' does not exist.")
        sys.exit(1)

    results_dir = os.path.dirname(os.path.abspath(results_file))
    records = parse_results(results_file, scenario)

    if scenario == "test":
        sys.exit(analyze_test(records))
    else:
        analyze_measured(records, scenario, results_dir)


if __name__ == "__main__":
    main()
