Implement a basic CLI tool in Rust that is supposed to benchmark LLM serving platforms.

For start, focus on Ollama and vLLM. Assume both are configured and running.

Ideate benchmarking options, like time to first token and tokens per second.

The exposed command should allow for providing a prompt, url of the server, and parameters to the server (like which model, what reasoning effort, etc).
