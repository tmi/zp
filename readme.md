# Project

## CS01 K2G
* Status: Complete
* Summary: This project benchmarks architectures for visualizing real-time Kafka data in Grafana, comparing direct ingestion against a PostgreSQL/TimescaleDB pipeline. Findings show that PostgreSQL significantly outperforms direct Kafka queries in fetch speed, especially at high message volumes, and provides better query flexibility for dashboards.

## CS02 K2P
* Status: Complete
* Summary: This case study evaluates PostgreSQL performance across five schema designs for nested data, including JSONB, denormalization, and native arrays. Using a Rust-based tool for benchmarking, it demonstrates how schema choices significantly impact ingestion and query performance, with native arrays and denormalization offering the best query efficiency.

## CS03 Python Serialization Methods Comparison 
* Status: Complete
* Summary: This project compares various Python serialization methods, such as orjson, Pydantic, Protobuf, and Fury, using different dataclass sizes. It identifies `orjson` as a top general-purpose serializer and `pyfory` (Fury) as the highest performer for speed, while Protobuf and Avro remain ideal for schema-dependent cross-service communication.

## CS04 Torch Inference Showcase
* Status: Incubating
* Summary: This showcase implements baseline PyTorch models (Simple, RNN, Transformer) and explores their deployment via ONNX and ExecuTorch. It includes cross-language benchmarks for Python, C++, Rust, and Go, highlighting performance trade-offs between different runtimes and the challenges of batch inference in optimized environments.

## CS05 Rust-Python library+binary Showcase
* Status: Incubating
* Summary: This project demonstrates a hybrid Rust-Python architecture, featuring a Rust library and CLI integrated with a Python interface via `maturin` and `pyo3`. It showcases modern project management with `uv`, task automation with `just`, and cross-language testing to ensure seamless functionality between Rust and Python components.

## CS06 Coding Agent Project
* Status: Incubating
* Summary: This project implements a basic autonomous coding agent in Rust, featuring a terminal user interface (TUI) and integration with Ollama for local LLM support. It serves as a foundation for exploring agentic workflows and tool-calling capabilities within a memory-efficient and performant Rust environment.

## CS07 Rust Expression Evaluator
* Status: Incubating
* Summary: The `cs07-rs_pyeval` project provides a high-performance expression evaluation library implemented in Rust with seamless Python integration. It is designed to handle string interpolation with embedded logic, particularly useful for dynamic configuration and data processing pipelines that involve datetime manipulation and string operations.

## CS08 LLM Serving Benchmark
* Status: Incubating
* Summary: This project implements a CLI tool in Rust designed to benchmark LLM serving platforms, specifically Ollama and vLLM. It measures key performance indicators such as Time to First Token (TTFT) and Tokens Per Second (TPS) using streaming responses, providing developers with metrics to evaluate and optimize LLM inference performance.

## CS09 Simple Dungeon Game
* Status: Incubating
* Summary: This project implements a retro-style dungeon crawling game using Python and `curses`. It provides a clean, extensible framework for level-based gameplay with treasure collection and trap mechanics. The design prioritizes ease of content creation through ASCII-based level files and centralized asset management.

## CS10 LSP CLI
* Status: Incubating
* Summary: This project provides a Rust-based command-line interface for interacting with Language Server Protocol (LSP) servers. It supports spawning servers, checking status, and performing code navigation tasks like finding definitions and callers. The tool uses named pipes for asynchronous communication and includes fallbacks for servers with limited capability support.

## CS11 Concurrency Buffers and Queues
* Status: Incubating
* Summary: This subproject implements a Java concurrency micro-benchmark evaluating message-passing performance across producer, transformer, and consumer pipelines using strategies such as lock-free slot arrays, Vyukov MPMC ring buffers, and JDK standard queue variants (`ArrayBlockingQueue`, `LinkedBlockingQueue`, `ConcurrentLinkedQueue`).
