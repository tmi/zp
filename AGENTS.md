# Git & Commits
* When making a commit, use the format `[<subfolderId>] message` -- for example, commits to `cs00-something/` should be like `[cs00] improve tests`.
* Before making a commit, *always* check you are not in the `main` branch. Refuse to commit and stop all work if you are.
* Before making a commit, make sure all tests and lints in the affected project pass -- for rust projects, `cargo check` and `cargo test`, for python, `uv run ruff` and `uv run pytest`.
* When retrieving comments to a PR, use `gh api /repos/:owner/:repo/pulls/<number>/comments`. Assume `gh` is authenticated.

# Projects
* Each subfolder (cs00-something, cs01-else, ...) is a project on its own. Never work or change multiple projects at once.
* Each project contains a readme.md (which is intended as instructions for contributors, including you)  and a summary.md (the results and takeaway, intended for downstream users). When you edit a project, consider updating both. Readme can be detailed but not overly verbose, Summary only very brief.
* Each project contains `spec/` directory which is a history of individual change requests -- never modify these, only read them to understand project history if needed.
* Each project contains a justfile (for https://github.com/casey/just) with at least `val` recipe, which runs linting and type checking and tests.

# Python
* To manage projects and dependencies, use `uv`, for example, `uv init` or `uv run pytest`. Use `dev` dependency group for tools like pytest, ruff and ty.
* Always use type annotations, even when nothing better than `Any` is possible. Use `ty` for checking. Make sure `ty` is pinned to an exact version in the pyproject (for example, `ty==0.0.15`).
