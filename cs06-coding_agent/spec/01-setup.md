Implement a basic agent in Rust that will eventually focus on autonomous coding.

It will be a CLI application, with a minimalistic interface:
 1. A text input field, occupying bottom of the screen, like three lines
 2. Agent's output, occupying the top screen

Structure the code as follows:
 - models: here will go code for interacting with eg ollama or cloud APIs, ie, calling of the LLM itself
   - for start, implement only ollama integration. Assume there will always be ollama running at the machine, ie, no need to set it up yourself
 - tools: here we will put tools that the agent will have at its disposal
   - for start, put there only a 'read' tool, that will, given a file name, return its complete contents
 - agents: here we will put prompts and configurations
   - agent will be a trait I guess?
   - implement one agent, 'main', which has a system prompt something like "You are a smart assistant. You have tools at your disposal. Be very brief in general"
 - logging: here we will handle all logging
   - for now, at the start, generate a session id, and create a file like /tmp/agenticSessions/<sessionId.agentId.log>, and make sure the whole conversation (inputs and outputs) gets logged, and log any tool call there as well

Use `clap` crate for CLI arguments. The only supported for now will be `-m/--model` which will give the string we pass to ollama (expect it to be in the format 'ollama:<model>', and extract that model, ie, have a trait Model with OllamaModel implementation)

Write only basic unit tests, no integration or anything.

For inspiration, you can inspect the https://github.com/openai/codex project -- also rust, also coding oriented.
But that is a huge project, you implement only very basics.
