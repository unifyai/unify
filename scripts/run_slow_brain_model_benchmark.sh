#!/usr/bin/env bash
# Run live slow-brain latency benchmarks on real ConversationManager tests.
#
# Exercises:
#   tests/conversation_manager/core/test_slow_brain_model_latency.py
#
# Matrix: 4 scenarios × 2 models × 3 repeats = 24 timed runs.
#
# Requires a running Orchestra (local self-host stack or hosted ORCHESTRA_URL).
#
# Usage:
#   cd unity
#   ./scripts/run_slow_brain_model_benchmark.sh
#   ./scripts/run_slow_brain_model_benchmark.sh --json-out /tmp/slow_brain_bench.json

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

JSON_OUT=""
EXTRA_ARGS=()
while (($# > 0)); do
  case "$1" in
    --json-out)
      JSON_OUT="${2:-}"
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

export UNILLM_CACHE=false
export LITELLM_LOG="${LITELLM_LOG:-ERROR}"
if [[ -n "$JSON_OUT" ]]; then
  export SLOW_BRAIN_BENCHMARK_JSON="$JSON_OUT"
fi

echo "Running slow-brain benchmark matrix (4 scenarios × 2 models × 3 repeats)..."
uv run pytest \
  tests/conversation_manager/core/test_slow_brain_model_latency.py \
  -v \
  -m slow_brain_benchmark \
  "${EXTRA_ARGS[@]}"

echo
echo "Done. Summary printed above by pytest_sessionfinish."
if [[ -n "$JSON_OUT" ]]; then
  echo "Raw timings: $JSON_OUT"
fi
