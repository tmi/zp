Write a bash script for checking age and vulnerability of a given uv.lock file.

We want the following:
1/ It finds all uv.lock files in the current git repo (assume `git` is installed, if cwd not in a git repo fail intelligently) and handles them. If `--lock <file>` is given, only handle one
2/ It checks whether its on `main`/`master` branch -- in which case it analyzes all packages in the lock, otherwise only those whose versions have changed compared to `main`/`master`.
3/ The first check is whether any package is not too new -- if `--exclude-newer "<timedelta>"` respect that, otherwise if the uv.lock itself has an `[options] exclude-newer-span = "<timedelta>"` respect that, otherwise default to 1 day.
4/ The second check is to invoke basically `curl -XPOST --data '{"version": "7.0.0", "package": {"name": "forecast-in-a-box", "ecosystem": "PyPI"}}' https://api.osv.dev/v1/query` and see if it returns `{}` (good) or basically anything else (bad -- but distingiuish in the resulting message between comms failure, the package not being found, and the response containing some content).

Do it for all locks & packages, then report if all package-locks that had an issue and what issue, or whether everything passed, and exit with 0 or 1.
In case of comms failure or lock parsing failure etc, you can fail early, but finding vulns for a single package is not a stopper.
