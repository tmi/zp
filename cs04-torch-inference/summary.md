*Status*: Incubating

# Summary

Implemented baseline PyTorch models (Simple, RNN, Transformer), ONNX export and inference, and ExecuTorch compilation and inference.
Cross-language benchmarks were implemented for Python, C++, Rust, and Go.

## Benchmark Results (Python)

All results are in milliseconds (ms).

### Linux (**unknown device**)
| Model | Framework | Single (P95) | Batch (Mean) |
|-------|-----------|--------------|--------------|
| Simple | Torch | 0.1163 | 0.0941 |
| Simple | ONNX | 0.0286 | 0.0476 |
| Simple | ExecuTorch | 0.1961 | 0.4628 |
| RNN | Torch | 0.4541 | 0.7164 |
| RNN | ONNX | 0.2696 | 0.3785 |
| RNN | ExecuTorch | 1.0631 | 4.6720 |
| Transformer | Torch | 1.0217 | 4.2968 |
| Transformer | ONNX | 0.4817 | 3.7966 |
| Transformer | ExecuTorch | 19.9789 | 295.4877 |

**TODO** have this generated as a `just` action

### MacOS
**Missing**

## Benchmark Results (Compiled)
**Missing**

NOTE -- see `justfile` for implementation issues.

## Key Takeaways
- ONNX Runtime (Python) consistently showed the lowest latency for single inferences.
- ExecuTorch (Portable backend) showed higher latency, especially for the Transformer model, which is expected for the unoptimized portable runtime.
- Batch inference in ExecuTorch required separate model exports for fixed shapes (or dynamic shapes which had some constraints in this environment).
- Cross-language support (C++, Rust, Go) was implemented for ONNX and ExecuTorch to demonstrate multi-language deployment capabilities.
