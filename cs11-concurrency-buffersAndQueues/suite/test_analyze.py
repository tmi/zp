import tempfile
from pathlib import Path

from analyze import analyze_measured, analyze_test, parse_results


def test_parse_results_test_scenario() -> None:
    content = """# CFG lang=rust scenario=test M=B ALLOC=allocate N=100000 K=64 I=4 P=2 T=4 RING=1024 S=test
RESULT 10000000 12345.0
# CFG lang=rust scenario=test M=A ALLOC=allocate N=100000 K=64 I=4 P=2 T=4 RING=1024 S=test
RESULT 12000000 12345.0
"""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        records = parse_results(tmp_path, "test")
        assert len(records) == 2
        assert records[0].cfg["M"] == "B"
        assert records[0].cfg["ALLOC"] == "allocate"
        assert records[0].sum_val == 12345.0
        assert records[0].measured_elapsed_ns == [10000000.0]

        res = analyze_test(records)
        assert res == 0
    finally:
        Path(tmp_path).unlink()


def test_parse_results_differing_sums_in_test() -> None:
    content = """# CFG lang=java scenario=test M=B ALLOC=allocate N=100000 K=64 I=4 P=2 T=4 RING=1024 S=test
RESULT 10000000 12345.0
# CFG lang=java scenario=test M=A ALLOC=allocate N=100000 K=64 I=4 P=2 T=4 RING=1024 S=test
RESULT 12000000 99999.0
"""
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        records = parse_results(tmp_path, "test")
        assert len(records) == 2
        res = analyze_test(records)
        assert res == 1
    finally:
        Path(tmp_path).unlink()


def test_parse_results_varying_sums_in_non_test_scenario() -> None:
    content = """# CFG lang=rust scenario=spst M=B ALLOC=allocate N=2000000 K=64 I=8 P=1 T=1 RING=1024 S=3/2
RESULT 15000000 100.0
RESULT 14000000 200.0
RESULT 13000000 300.0
---
RESULT 10000000 400.0
RESULT 11000000 500.0
# CFG lang=rust scenario=spst M=A ALLOC=pool N=2000000 K=64 I=8 P=1 T=1 RING=1024 S=3/2
RESULT 18000000 600.0
RESULT 17000000 700.0
RESULT 16000000 800.0
---
RESULT 8000000 900.0
RESULT 9000000 1000.0
"""
    with tempfile.TemporaryDirectory() as tmpdir:
        results_file = Path(tmpdir) / "rust-spst.txt"
        results_file.write_text(content)

        records = parse_results(str(results_file), "spst")
        assert len(records) == 2
        assert records[0].measured_elapsed_ns == [10000000.0, 11000000.0]
        assert records[1].measured_elapsed_ns == [8000000.0, 9000000.0]

        analyze_measured(records, "spst", tmpdir)

        png_path = Path(tmpdir) / "rust-spst.png"
        assert png_path.exists()
