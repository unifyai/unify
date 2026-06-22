#!/usr/bin/env bash
# Install the pre-commit framework hooks for this checkout.
#
# Git hooks are a per-clone, untracked artifact: a fresh clone, worktree, or
# cloud checkout has no hooks until this runs. Installing via the pre-commit
# framework wires up every hook in .pre-commit-config.yaml in one place —
# formatting (black/autoflake) AND the cursoragent identity guard
# (block-cursoragent-identity) — so they coexist instead of one overwriting
# .git/hooks/pre-commit.

set -e

if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "[install-git-hooks] Not a git repository, skipping"
    exit 0
fi

# Resolve a pre-commit runner: prefer the project venv, then PATH, then uv.
if [ -x ".venv/bin/pre-commit" ]; then
    PRE_COMMIT=(".venv/bin/pre-commit")
elif command -v pre-commit > /dev/null 2>&1; then
    PRE_COMMIT=("pre-commit")
elif command -v uv > /dev/null 2>&1; then
    PRE_COMMIT=("uv" "run" "pre-commit")
else
    echo "[install-git-hooks] pre-commit is not available. Install it (e.g. 'uv sync --all-groups' or 'pip install pre-commit') and re-run." >&2
    exit 1
fi

"${PRE_COMMIT[@]}" install
echo "[install-git-hooks] Installed pre-commit hooks (formatting + cursoragent identity guard)"
