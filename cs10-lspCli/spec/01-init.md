This project should be a rust CLI to interact with LSP servers

It should support the following commands:
lspCli server status
lspCli server spawn "name" "command"
lspCli definition --server serverName "symbol"
lspCli callers --server serverName "symbol"

With example session:
```
$ lspCli server status
<none>
$ lspCli server spawn s1 "uvx ty server"
spawned PID 123
$ lspCli server status
* (123) uvx ty server
$ lspCli definition --server s1 myFunc
'def myFunc(a: str) -> None:'
L24@myProj/myMod/__init__.py
$ lspCli callers myFunc
'myFunc("hello")'
L12@myProj/myMod/util.py
'myFunc("world")'
L24@myProj/otherMod/func.py
```

The spawned process would ideally communicate via named pipes, created in eg /tmp/lspCli/<serverName>.stdin, .stdout

When there would be a single server only, then the `--server` can be omitted, as in the last example call.
