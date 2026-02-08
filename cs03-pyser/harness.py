import time
import importlib
from multiprocessing.pool import ThreadPool
from generate import generate_small, generate_medium

def unit_test(method: str):
    sd = importlib.import_module(f"sd_{method}")

    small = generate_small(1)[0]
    data_small = sd.ser_small(small)
    res_small = sd.des_small(data_small)
    assert res_small == small, f"Small failed for {method}: {res_small} != {small}"

    medium = generate_medium(1, 2, 2, 2, 2)[0]
    data_medium = sd.ser_medium(medium)
    res_medium = sd.des_medium(data_medium)
    assert res_medium == medium, f"Medium failed for {method}: {res_medium} != {medium}"
    print(f"Unit test passed for {method}")

def perf_test(method: str):
    sd = importlib.import_module(f"sd_{method}")

    n_small = 1000
    n_medium = 100

    smalls = generate_small(n_small)
    mediums = generate_medium(n_medium, 10, 5, 5, 5)

    # Measure Small Ser
    out_small = [None] * n_small
    # Warmup
    for i in range(n_small):
        out_small[i] = sd.ser_small(smalls[i])

    start = time.perf_counter_ns()
    for _ in range(5):
        for i in range(n_small):
            out_small[i] = sd.ser_small(smalls[i])
    ser_small_time = (time.perf_counter_ns() - start) / 5 / n_small

    pool = ThreadPool()
    start = time.perf_counter_ns()
    for _ in range(5):
        pool.map(sd.ser_small, smalls)
    ser_small_tp_time = (time.perf_counter_ns() - start) / 5 / n_small

    # Measure Small Des
    res_small = [None] * n_small
    # Warmup
    for i in range(n_small):
        res_small[i] = sd.des_small(out_small[i])

    start = time.perf_counter_ns()
    for _ in range(5):
        for i in range(n_small):
            res_small[i] = sd.des_small(out_small[i])
    des_small_time = (time.perf_counter_ns() - start) / 5 / n_small

    start = time.perf_counter_ns()
    for _ in range(5):
        pool.map(sd.des_small, out_small)
    des_small_tp_time = (time.perf_counter_ns() - start) / 5 / n_small

    # Measure Medium Ser
    out_medium = [None] * n_medium
    # Warmup
    for i in range(n_medium):
        out_medium[i] = sd.ser_medium(mediums[i])

    start = time.perf_counter_ns()
    for _ in range(5):
        for i in range(n_medium):
            out_medium[i] = sd.ser_medium(mediums[i])
    ser_medium_time = (time.perf_counter_ns() - start) / 5 / n_medium

    start = time.perf_counter_ns()
    for _ in range(5):
        pool.map(sd.ser_medium, mediums)
    ser_medium_tp_time = (time.perf_counter_ns() - start) / 5 / n_medium

    # Measure Medium Des
    res_medium = [None] * n_medium
    # Warmup
    for i in range(n_medium):
        res_medium[i] = sd.des_medium(out_medium[i])

    start = time.perf_counter_ns()
    for _ in range(5):
        for i in range(n_medium):
            res_medium[i] = sd.des_medium(out_medium[i])
    des_medium_time = (time.perf_counter_ns() - start) / 5 / n_medium

    start = time.perf_counter_ns()
    for _ in range(5):
        pool.map(sd.des_medium, out_medium)
    des_medium_tp_time = (time.perf_counter_ns() - start) / 5 / n_medium

    pool.close()
    pool.join()

    out_small_0 = out_small[0]
    out_medium_0 = out_medium[0]
    if out_small_0 is None or out_medium_0 is None:
        raise ValueError("No data generated")
    size_small = len(out_small_0)
    size_medium = len(out_medium_0)

    return {
        "size_small": size_small,
        "size_medium": size_medium,
        "ser_small_mean": ser_small_time,
        "ser_small_tp_mean": ser_small_tp_time,
        "des_small_mean": des_small_time,
        "des_small_tp_mean": des_small_tp_time,
        "ser_medium_mean": ser_medium_time,
        "ser_medium_tp_mean": ser_medium_tp_time,
        "des_medium_mean": des_medium_time,
        "des_medium_tp_mean": des_medium_tp_time,
    }

if __name__ == "__main__":
    methods = ["struct", "pickle", "cloudpickle", "pydantic", "orjson", "protobuf", "avro", "fory"]
    results = {}
    for m in methods:
        try:
            unit_test(m)
            results[m] = perf_test(m)
        except Exception as e:
            print(f"Failed for {m}: {e}")
            import traceback
            traceback.print_exc()

    print("\n| method | size (small-medium) | ser-mean | ser-tp-mean | des-mean | des-tp-mean |")
    print("| --- | --- | --- | --- | --- | --- |")
    for m in methods:
        if m not in results:
            continue
        r = results[m]
        def fmt(s, m_val):
            return f"{s/1000:.2f}µs / {m_val/1000:.2f}µs"
        print(f"| {m} | {r['size_small']}/{r['size_medium']} | {fmt(r['ser_small_mean'], r['ser_medium_mean'])} | {fmt(r['ser_small_tp_mean'], r['ser_medium_tp_mean'])} | {fmt(r['des_small_mean'], r['des_medium_mean'])} | {fmt(r['des_small_tp_mean'], r['des_medium_tp_mean'])} |")
