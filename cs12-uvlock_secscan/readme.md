# CS12 uv.lock security scan

`uvlock_secscan.sh` checks packages in `uv.lock` files for (1) age and (2) known vulnerabilities via [OSV](https://osv.dev).

Requires `bash`, `git`, `curl`, `jq`, GNU `date`.

```
./uvlock_secscan.sh [--lock <file>] [--exclude-newer "<timedelta>"]
```

* Without `--lock`, all `uv.lock` files tracked (or untracked-unignored) in the current git repo are checked.
* On `main`/`master` all registry packages are checked; on other branches only packages whose `name`+`version` differ from the lock on `main`/`master` (local, else `origin/`).
* Age: the earliest `upload-time` of a package's files must be older than the min age: `--exclude-newer`, else `[options] exclude-newer-span` in the lock, else `1 day`. Formats: `1 day`, `36 hours`, `1w`, `P1DT12H`.
* Vulns: `POST` to OSV `/v1/query`; `{}` is good. HTTP 404 = not found, other content = vulnerabilities (ids listed). Per-package issues are collected; comms/parse failures abort immediately.
* Exit 0 on all passed, 1 otherwise. `OSV_URL` env var overrides the endpoint (useful for local testing with a fake server).
* Non-registry packages (virtual/editable/git) are skipped.

Validate with `just val` (shellcheck + syntax).
