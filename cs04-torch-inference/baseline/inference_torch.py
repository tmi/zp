import torch
import numpy as np
import argparse
import time
import importlib
from typing import Any, List

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model_type", choices=["simple", "rnn", "transformer"])
    parser.add_argument("--device", default="cpu", help="torch device (cpu, cuda, mps)")
    args = parser.parse_args()

    device = torch.device(args.device)

    module_name = f"baseline.model_{args.model_type}"
    module: Any = importlib.import_module(module_name)
    model = module.get_model().to(device)
    ckpt_path = f"data/{args.model_type}.ckpt"
    model.load_state_dict(torch.load(ckpt_path, map_location=device))
    model.eval()

    # Load data
    single_input_data = np.load(f"data/example_input_single_{args.model_type}.npz")["data"]
    batch_input_data = np.load(f"data/example_input_batch_{args.model_type}.npz")["data"]

    single_input = torch.from_numpy(single_input_data).to(device)
    batch_input = torch.from_numpy(batch_input_data).to(device)

    # Single inference
    # Warmup
    with torch.no_grad():
        model(single_input)

    latencies: List[float] = []
    for _ in range(20):
        start = time.perf_counter_ns()
        with torch.no_grad():
            model(single_input)
        latencies.append(time.perf_counter_ns() - start)

    p95 = np.percentile(latencies, 95)
    print(f"Single inference P95: {p95 / 1e6:.4f} ms")

    # Batch inference
    # Warmup
    with torch.no_grad():
        model(batch_input)

    batch_latencies: List[float] = []
    for _ in range(3):
        start = time.perf_counter_ns()
        with torch.no_grad():
            model(batch_input)
        batch_latencies.append(time.perf_counter_ns() - start)

    mean_batch = np.mean(batch_latencies)
    print(f"Batch inference mean: {mean_batch / 1e6:.4f} ms")

if __name__ == "__main__":
    main()
