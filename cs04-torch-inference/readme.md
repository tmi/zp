# cs04-torch-inference

Demonstrate compilation of PyTorch models to ExecuTorch and ONNX, with inference benchmarks in multiple languages.

## Setup
1. Install `uv`
2. Run `uv sync` in `cs04-torch-inference`
3. Use `just val` for validation

## Usage
Refer to:
- `baseline/`: PyTorch baseline and model definitions
- `onnx_bench/`: ONNX inference benchmarks (Python, C++, Rust, Go)
- `executorch_bench/`: ExecuTorch inference benchmarks (Python, C++, Rust)

## Data Generation
Run `uv run python -m baseline.initialize_model <model_type> --onnx --executorch` to generate models and sample data in `data/`.
Supported models: `simple`, `rnn`, `transformer`.
