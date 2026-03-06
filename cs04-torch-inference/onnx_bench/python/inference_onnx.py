import onnxruntime as ort
import numpy as np
import argparse
import time
import os
from typing import List

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model_type", choices=["simple", "rnn", "transformer"])
    parser.add_argument("--provider", default="cpu", choices=["cpu", "cuda", "coreml", "xnnpack"])
    args = parser.parse_args()

    providers = {
        "cpu": ["CPUExecutionProvider"],
        "cuda": ["CUDAExecutionProvider"],
        "coreml": ["CoreMLExecutionProvider"],
        "xnnpack": ["XnnpackExecutionProvider"]
    }[args.provider]

    onnx_path = f"data/{args.model_type}.onnx"
    if not os.path.exists(onnx_path):
        print(f"ONNX model not found at {onnx_path}. Run initialize_model.py with --onnx first.")
        return

    session = ort.InferenceSession(onnx_path, providers=providers)
    input_name = session.get_inputs()[0].name

    # Load data
    single_input_data = np.load(f"data/example_input_single_{args.model_type}.npz")["data"].astype(np.float32)
    batch_input_data = np.load(f"data/example_input_batch_{args.model_type}.npz")["data"].astype(np.float32)

    # Single inference
    # Warmup
    session.run(None, {input_name: single_input_data})

    latencies: List[float] = []
    for _ in range(20):
        start = time.perf_counter_ns()
        session.run(None, {input_name: single_input_data})
        latencies.append(time.perf_counter_ns() - start)

    p95 = np.percentile(latencies, 95)
    print(f"Single inference P95 (ONNX): {p95 / 1e6:.4f} ms")

    # Batch inference
    # Warmup
    session.run(None, {input_name: batch_input_data})

    batch_latencies: List[float] = []
    for _ in range(3):
        start = time.perf_counter_ns()
        session.run(None, {input_name: batch_input_data})
        batch_latencies.append(time.perf_counter_ns() - start)

    mean_batch = np.mean(batch_latencies)
    print(f"Batch inference mean (ONNX): {mean_batch / 1e6:.4f} ms")

if __name__ == "__main__":
    main()
