#!/usr/bin/env bash
# Reject commits authored/committed under a cursoragent identity.
#
# Runs as a pre-commit hook (see .pre-commit-config.yaml). Keeping the check
# inside the pre-commit framework means it coexists with the formatting hooks
# instead of overwriting .git/hooks/pre-commit.

fail() {
    echo "ERROR: Commit blocked - $1"
    echo ""
    echo "Please configure your git identity:"
    echo "  git config --global user.name 'Your Name'"
    echo "  git config --global user.email 'you@example.com'"
    echo ""
    echo "Or run: ./scripts/configure-git-identity.sh"
    exit 1
}

AUTHOR_NAME=$(git config user.name 2>/dev/null || echo "")
AUTHOR_EMAIL=$(git config user.email 2>/dev/null || echo "")

case "$AUTHOR_NAME" in *cursoragent*|*"Cursor Agent"*) fail "author name contains 'cursoragent'";; esac
case "$AUTHOR_EMAIL" in *cursoragent*) fail "author email contains 'cursoragent'";; esac
case "${GIT_AUTHOR_NAME:-}" in *cursoragent*|*"Cursor Agent"*) fail "GIT_AUTHOR_NAME contains 'cursoragent'";; esac
case "${GIT_AUTHOR_EMAIL:-}" in *cursoragent*) fail "GIT_AUTHOR_EMAIL contains 'cursoragent'";; esac
case "${GIT_COMMITTER_NAME:-}" in *cursoragent*|*"Cursor Agent"*) fail "GIT_COMMITTER_NAME contains 'cursoragent'";; esac
case "${GIT_COMMITTER_EMAIL:-}" in *cursoragent*) fail "GIT_COMMITTER_EMAIL contains 'cursoragent'";; esac

exit 0
