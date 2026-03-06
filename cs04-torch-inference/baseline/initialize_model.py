import torch
import numpy as np
import argparse
import os
import importlib
from typing import Any

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model_type", choices=["simple", "rnn", "transformer"])
    parser.add_argument("--onnx", action="store_true", help="Also export to ONNX")
    parser.add_argument("--executorch", action="store_true", help="Also export to ExecuTorch")
    args = parser.parse_args()

    module_name = f"baseline.model_{args.model_type}"
    module: Any = importlib.import_module(module_name)
    model = module.get_model()
    input_shape = module.get_input_shape()

    os.makedirs("data", exist_ok=True)
    ckpt_path = f"data/{args.model_type}.ckpt"
    torch.save(model.state_dict(), ckpt_path)
    print(f"Saved checkpoint to {ckpt_path}")

    # Example inputs
    single_input = torch.randn(1, *input_shape)
    batch_input = torch.randn(20, *input_shape)

    single_input_path = f"data/example_input_single_{args.model_type}.npz"
    batch_input_path = f"data/example_input_batch_{args.model_type}.npz"
    np.savez(single_input_path, data=single_input.numpy())
    np.savez(batch_input_path, data=batch_input.numpy())

    # Save as .bin for cross-language benchmarks
    single_input.numpy().tofile(f"data/example_input_single_{args.model_type}.bin")
    batch_input.numpy().tofile(f"data/example_input_batch_{args.model_type}.bin")

    print(f"Saved example inputs to {single_input_path}, {batch_input_path}, and .bin files")

    model.eval()

    if args.onnx:
        os.makedirs("onnx_bench/python", exist_ok=True)
        onnx_path = f"data/{args.model_type}.onnx"
        torch.onnx.export(
            model,
            (batch_input,), # Use batch_input as example for better tracing of batch dim
            onnx_path,
            export_params=True,
            opset_version=14,
            do_constant_folding=True,
            input_names=['x'],
            output_names=['output'],
            dynamic_axes={'x': {0: 'batch_size'}, 'output': {0: 'batch_size'}}
        )
        print(f"Exported ONNX model to {onnx_path}")

    if args.executorch:
        import executorch.exir as exir
        os.makedirs("executorch_bench/python", exist_ok=True)

        # Single pte
        pte_single_path = f"data/{args.model_type}_single.pte"
        with torch.no_grad():
            exported_program = torch.export.export(model, (single_input,))
            edge_program = exir.to_edge(exported_program)
            executorch_program = edge_program.to_executorch()
            with open(pte_single_path, "wb") as f:
                f.write(executorch_program.buffer)
        print(f"Exported ExecuTorch single model to {pte_single_path}")

        # Batch pte
        pte_batch_path = f"data/{args.model_type}_batch.pte"
        with torch.no_grad():
            exported_program = torch.export.export(model, (batch_input,))
            edge_program = exir.to_edge(exported_program)
            executorch_program = edge_program.to_executorch()
            with open(pte_batch_path, "wb") as f:
                f.write(executorch_program.buffer)
        print(f"Exported ExecuTorch batch model to {pte_batch_path}")

if __name__ == "__main__":
    main()
