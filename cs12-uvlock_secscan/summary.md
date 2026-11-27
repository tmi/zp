*Status*: Incubating

Bash script `uvlock_secscan.sh` that checks every package in a repo's `uv.lock` files for being too new (configurable min age) and for known vulnerabilities via the OSV API; on non-main branches only changed packages are checked. Exits 0/1.
