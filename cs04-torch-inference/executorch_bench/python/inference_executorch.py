import torch
import numpy as np
import argparse
import time
import os
from typing import List
try:
    from executorch.extension.pybindings._portable_lib import _load_for_executorch_from_buffer # type: ignore
except ImportError:
    _load_for_executorch_from_buffer = None

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model_type", choices=["simple", "rnn", "transformer"])
    parser.add_argument("--backend", default="portable", choices=["portable", "xnnpack", "mps", "cuda", "coreml"])
    args = parser.parse_args()

    if _load_for_executorch_from_buffer is None:
        print("ExecuTorch python bindings not found. Make sure executorch is installed correctly.")
        return

    # Single inference
    pte_single_path = f"data/{args.model_type}_single.pte"
    if not os.path.exists(pte_single_path):
        print(f"ExecuTorch single model not found at {pte_single_path}.")
        return

    with open(pte_single_path, "rb") as f:
        buffer_single = f.read()
    module_single = _load_for_executorch_from_buffer(buffer_single)

    single_input_data = np.load(f"data/example_input_single_{args.model_type}.npz")["data"].astype(np.float32)
    single_input = torch.from_numpy(single_input_data)

    # Warmup
    module_single.forward([single_input])

    latencies: List[float] = []
    for _ in range(20):
        start = time.perf_counter_ns()
        module_single.forward([single_input])
        latencies.append(time.perf_counter_ns() - start)

    p95 = np.percentile(latencies, 95)
    print(f"Single inference P95 (ExecuTorch): {p95 / 1e6:.4f} ms")

    # Batch inference
    pte_batch_path = f"data/{args.model_type}_batch.pte"
    if not os.path.exists(pte_batch_path):
        print(f"ExecuTorch batch model not found at {pte_batch_path}.")
        return

    with open(pte_batch_path, "rb") as f:
        buffer_batch = f.read()
    module_batch = _load_for_executorch_from_buffer(buffer_batch)

    batch_input_data = np.load(f"data/example_input_batch_{args.model_type}.npz")["data"].astype(np.float32)
    batch_input = torch.from_numpy(batch_input_data)

    # Warmup
    module_batch.forward([batch_input])

    batch_latencies: List[float] = []
    for _ in range(3):
        start = time.perf_counter_ns()
        module_batch.forward([batch_input])
        batch_latencies.append(time.perf_counter_ns() - start)

    mean_batch = np.mean(batch_latencies)
    print(f"Batch inference mean (ExecuTorch): {mean_batch / 1e6:.4f} ms")

if __name__ == "__main__":
    main()
