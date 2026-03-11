# Coding Agent

A basic Rust-based autonomous coding agent with a TUI.

## Features
- Minimalist TUI using `ratatui`
- Ollama integration for LLM support
- File reading tool
- Session logging in `/tmp/agenticSessions/` (logs user inputs, assistant outputs, tool calls, and MCP interchange)

## Usage
Run with:
```bash
cargo run -- --model ollama:llama3
```

## Shortcuts
- `Enter`: Submit input
- `Esc`, `Ctrl-D`: Exit
- `Backspace`: Delete character
