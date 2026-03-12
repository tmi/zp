Add MCP support to the agent. 
Support configuring both as a json as a cli param.
The json would follow the standard, for example
```
{ "mcpServers": { "myServer": { "command": "uv", "args"  } } }
```
And the json would be given as `--json <path to json>`.

The CLI param would be `--mcp <name>:"command args"`, for example `--mcp myServer:"uv run myServer"`.

If both are given, merge top level keys, and in case of collision prefer the `--mcp` param value.
The `--mcp` param can be given at most once. Neither is mandatory.

Put the mcp related code to a new `mcp` module (though you'll need to import from that module in main or agents models etc)

No need to include any example or default MCPs.
