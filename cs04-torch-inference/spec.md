**Goal**: demonstrate compilation to executorch

Create a bunch of python scripts:
 - model_simple.py -- defines a torch with like 3 linear layers
 - model_rnn.py -- defines a torch with an rnn
 - model_transformer.py -- defines a torch transformer
Neither needs to be overly complex, just a default showcase of architecture like this

More scripts:
 - initialize_model.py -- a runnable script that is given a model name ("simple", "rnn", "transformer"), sets the weights to some random values ("training"), and saves as a ckpt
   - additionally, also save example_input_single_<model>.npz (one random input tensor with the correct shape) and example_input_batch_<model>.npz (random input tensor with shape [20, * input shape])
 - inference_torch.py -- given a checkpoint name, reads the checkpoint, inspects the input shape, read example input data, and invokes inference, with runtime measured by time.perf_counter_ns
   - dont measure data reading, first run inference warmup, then run 20 point inferences and calculate .95 percentile
   - then run a batch inference with a batch size like 20; run 1 warmup and 3 times and report mean

That is the baseline. Save the scripts into `baseline/`, and data into `data/`. Don't commit the content of data folder (put it to .gitignore), but make sure the functions to populate it are part of the model_<name>.py files

Then do the following:
1. ONNX -- Expand initialize_model to allow for onnx export, and implement inference_onnx.py, which would be exactly like inference_torch but with onnx runtime. Save that to onnx/python folder
2. ONNX but without python -- try c++, rust, and golang, putting each into onnx/<lang> folder. The logic for perf measurement should be identical, ie, read the respective data/ and onnx weights file, run warmups and calc percentile etc
3. Executorch -- like with onnx, but use the executorch. Try c++, rust, and golang as well


Note about backends/runtimes -- make sure this is configurable. We want to support torch + cpu, cuda, mps; onnx + coreml, cuda, xnnpack; executorh + xnnpack, cuda, coreml, mps
Presumably you don't have all backends accessible -- try to implement the code for it based on best guess / linting, but don't worry about testing.

Note about weights and quantization -- ignore this for now, that is, keep all weights f32/f64.
