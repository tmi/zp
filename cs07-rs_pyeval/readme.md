# rs_pyeval

A Rust library with Python bindings for evaluating expressions.

## Features
- Expression evaluation with support for:
  - Arithmetic operators (`+`, `-`, `*`, `/`, `**`, `//`, `%`)
  - String concatenation (`+`)
  - Function calls (`timedelta`, `floor_day`, `floor_hour`, `upper`, `lower`)
  - Method calls (`split`)
  - Subscripting for lists
  - Variable interpolation via `${...}` syntax
- Automated datetime parsing for variables
- Glyph extraction to identify referenced variables

## Development
This project uses `maturin` for Python bindings and `uv` for dependency management.

### Prerequisites
- Rust and Cargo
- `uv`

### Commands
- Run all validations: `just val`
- Run Rust tests: `cd pyeval-core && cargo test`
- Build and install locally: `maturin develop`
- Run Python tests: `uv run pytest`
