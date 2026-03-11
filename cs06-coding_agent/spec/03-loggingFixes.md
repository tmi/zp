In the main module, we are logging every user input and response

We would like to add tool calls and responses too -- both for the tools module and for the mcp module. Make sure you log both the name of the tool / method of the mcp request sent, and the params.

Make sure that these log lines and the already existing input/response lines are distinguishable, like by logger name, line prefix, something.

Edit README.md with a note about logging -- we want every major change to respect logging, ie, if there is any interchange thing happening, it should be logged
