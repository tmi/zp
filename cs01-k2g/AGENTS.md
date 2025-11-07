Read `readme.md` first for the project description.

# Tooling and Environment
Use `uv` for all python related work (uv init, uv run, etc).
Each time you introduce a new dependency D, run `uv add D`
Whenever you make any code changes, run `uv run ty check` and fix all type errors. Feel free to use `: Any` if no good type is available, or `# ty: ignore` if the error is hard to fix.

Kafka is already running at `kafka:9092`, `kafka:9093` -- use it. There is topic `t1` created already with one partition, as well as `t1-json`.

Postgres is already running at `postgres` host, with password `pgt`.

# Coding style
Use comments sparringly, only for non-intuitive or non-usual code patterns.
In python, always use type annotations, even Any if no better type is available.
Indent by four spaces.

Do not use language keywords or builtins as field members or variables. For example, do not use `min` but instead use `minimum` or `minor` or `minimal` or `min_`, etc.

# Libraries
## Python
For making http requests, use httx library. Use async or sync depending on context.
For json handling, use orjson.
