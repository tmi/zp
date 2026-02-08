# cs03-pyser

Comparison of various serialization options in Python.

## Methods Compared
- struct
- pickle
- cloudpickle
- pydantic (json mode)
- orjson
- google protobuf
- apache avro
- apache fory (fury)

## Running
Use `just val` to run lints, type checks, and tests.
Run `uv run python harness.py` to run benchmarks.
