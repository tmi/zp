## Git & Commits
* When making a commit, use the format `[<subfolder>] message` -- for example, commits to `research/` should be like `[research] added TFT model`.
* Before making a commit, *always* check you are not in the `main` branch. Refuse to commit and stop all work if you are.
* Before making a commit, make sure all tests and lints pass -- for rust projects, `cargo check` and `cargo test`, for python, `uv run ruff` and `uv run pytest`.
* When retrieving comments to a PR, use `gh api /repos/:owner/:repo/pulls/<number>/comments`. Assume `gh` is authenticated.
