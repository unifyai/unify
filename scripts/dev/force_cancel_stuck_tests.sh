#!/usr/bin/env bash
# Force-cancel Tests workflow runs that ignore ordinary cancel.
#
# Symptom: gh run cancel "succeeds" but cancel_requested_at stays null and
# jobs remain in_progress on "Run tests" for tens of minutes, holding the
# tests concurrency group so new staging matrices stay pending forever.
#
# GitHub's force-cancel bypasses that stuck state:
#   https://docs.github.com/en/rest/actions/workflow-runs#force-cancel-a-workflow-run
#
# Usage:
#   bash scripts/dev/force_cancel_stuck_tests.sh           # staging in_progress
#   bash scripts/dev/force_cancel_stuck_tests.sh main
#   bash scripts/dev/force_cancel_stuck_tests.sh staging 29326830812
set -euo pipefail

BRANCH="${1:-staging}"
RUN_ID="${2:-}"

if [[ -n "$RUN_ID" ]]; then
  echo "Force-cancelling run ${RUN_ID}..."
  gh api --method POST "repos/unifyai/unify/actions/runs/${RUN_ID}/force-cancel"
  gh run view "$RUN_ID" --json status,conclusion,url
  exit 0
fi

echo "Looking for in_progress Tests runs on ${BRANCH}..."
mapfile -t RUNS < <(gh run list --workflow=tests.yml --branch="$BRANCH" --status in_progress --limit 20 --json databaseId,url,displayTitle --jq '.[] | "\(.databaseId)\t\(.url)\t\(.displayTitle)"')

if (( ${#RUNS[@]} == 0 )); then
  echo "No in_progress Tests runs on ${BRANCH}."
  exit 0
fi

for row in "${RUNS[@]}"; do
  rid="${row%%$'\t'*}"
  rest="${row#*$'\t'}"
  echo "Force-cancelling ${rid}  ${rest}"
  gh api --method POST "repos/unifyai/unify/actions/runs/${rid}/force-cancel"
done

echo "Done. Current Tests runs on ${BRANCH}:"
gh run list --workflow=tests.yml --branch="$BRANCH" --limit 5
