#!/usr/bin/env bash
# Pull the shared LLM cache down from the newest llm-cache-ndjson artifact.
#
# The Actions cache is branch-scoped. A pull request into main runs with
# base=main and cannot restore an entry saved on staging, so the read-only
# suite there would miss every key and fail closed on the first prompt.
# Artifacts are repo-scoped, so they reach across branches and are what makes
# a promotion PR a passable gate.
#
# Callers invoke this only when the Actions cache missed. The store is large
# and the Actions cache is co-located with the runner, so it stays the hot
# path; this is the fallback that survives eviction and branch scoping.
set -euo pipefail

repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
target="${1:-.cache.ndjson}"

# Expired artifacts are still listed, with expired=true, so they have to be
# filtered out rather than assumed absent. Any workflow may publish this name,
# so this selects on the name across the repo rather than walking one workflow.
run_id="$(
  gh api "/repos/${repo}/actions/artifacts?name=llm-cache-ndjson&per_page=100" \
    --jq '[.artifacts[] | select(.expired == false)]
          | sort_by(.created_at) | reverse | .[0].workflow_run.id // empty'
)"

if [ -z "$run_id" ]; then
  echo "No unexpired llm-cache-ndjson artifact found; leaving ${target} as-is."
  exit 0
fi

echo "Hydrating ${target} from run ${run_id}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# gh run download refuses to overwrite an existing file, and a restore may
# already have written a stale branch-scoped copy, so stage and move over it.
if gh run download "$run_id" --repo "$repo" -n llm-cache-ndjson -D "$tmpdir"; then
  mv -f "${tmpdir}/.cache.ndjson" "$target"
  echo "Hydrated $(grep -c . "$target" || true) entries from run ${run_id}"
else
  echo "Artifact absent on run ${run_id}; leaving ${target} as-is."
fi
