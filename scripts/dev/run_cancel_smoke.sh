#!/usr/bin/env bash
# Drive cancel-latency smoke via Flow Smoke (exists on main; concurrency
# independent of Tests). Dispatch with confirm_llm_spend=CANCEL_SMOKE_OK.
# Usage: bash scripts/dev/run_cancel_smoke.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

REF="${1:-staging}"
echo "Dispatching Flow Smoke cancel diagnostics on ${REF}..."
gh workflow run flow-smoke.yml --ref "$REF" -f confirm_llm_spend=CANCEL_SMOKE_OK

sleep 4
RUN_ID="$(gh run list --workflow=flow-smoke.yml --branch="$REF" --limit 1 --json databaseId,displayTitle,status --jq '
  [.[] | select(.displayTitle|test("Flow smoke|CANCEL|cancel"; "i") or .status=="queued" or .status=="pending" or .status=="in_progress")][0].databaseId
')"
# Fallback: newest run
if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
  RUN_ID="$(gh run list --workflow=flow-smoke.yml --branch="$REF" --limit 1 --json databaseId --jq '.[0].databaseId')"
fi
echo "Run: https://github.com/unifyai/unify/actions/runs/${RUN_ID}"

echo "Waiting until all Hang steps are in_progress..."
ready=0
for _ in $(seq 1 90); do
  payload="$(gh run view "$RUN_ID" --json status,jobs,displayTitle)"
  ready="$(python3 -c "
import json,sys
d=json.load(sys.stdin)
jobs=[j for j in d['jobs'] if j['name'].startswith(('A:','B:','C:','D:','E:'))]
if len(jobs) < 5:
    print(0); sys.exit(0)
n=sum(1 for j in jobs for s in (j.get('steps') or []) if s.get('name')=='Hang' and s.get('status')=='in_progress')
print(n)
" <<<"$payload")"
  echo "  hang_in_progress=${ready}/5  run=$(echo "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "$ready" == "5" ]]; then
    break
  fi
  sleep 5
done

if [[ "$ready" != "5" ]]; then
  echo "ERROR: timed out waiting for Hang steps" >&2
  gh run view "$RUN_ID" --json jobs --jq '.jobs[] | {name,status,conclusion}'
  exit 1
fi

# Let heartbeats land in logs
sleep 15

CANCEL_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Cancelling at ${CANCEL_AT}..."
gh run cancel "$RUN_ID"
echo "$CANCEL_AT" > /tmp/cancel_smoke_cancel_at.txt
echo "$RUN_ID" > /tmp/cancel_smoke_run_id.txt

echo "Polling until run completes (E has always() sleep 120s)..."
for _ in $(seq 1 180); do
  st="$(gh run view "$RUN_ID" --json status,conclusion --jq '.status + "/" + (.conclusion // "-")')"
  echo "  ${st}"
  case "$st" in
    in_progress/*|pending/*|queued/*) sleep 5 ;;
    *) break ;;
  esac
done

python3 - <<'PY'
import json, subprocess
from datetime import datetime

run_id = open("/tmp/cancel_smoke_run_id.txt").read().strip()
cancel_at = datetime.fromisoformat(open("/tmp/cancel_smoke_cancel_at.txt").read().strip().replace("Z","+00:00"))
raw = subprocess.check_output(["gh","run","view",run_id,"--json","status,conclusion,jobs,url"], text=True)
d = json.loads(raw)
print(f"\nRun {run_id}: {d['status']}/{d.get('conclusion')}  {d['url']}")
print(f"Cancel requested at: {cancel_at.isoformat()}\n")
print(f"{'job':40} {'conclusion':12} {'hang_lag':>10} {'job_lag':>10} {'always':>8} {'cancel_step':>11}")
print("-" * 95)

def parse(ts):
    if not ts or ts.startswith("0001"):
        return None
    return datetime.fromisoformat(ts.replace("Z","+00:00"))

for j in sorted(d["jobs"], key=lambda x: x["name"]):
    if not j["name"].startswith(("A:","B:","C:","D:","E:")):
        continue
    hang_end = always = cancel_cleanup = None
    always = False
    cancel_cleanup = False
    for s in j.get("steps") or []:
        if s.get("name") == "Hang":
            hang_end = parse(s.get("completedAt"))
        if s.get("name") == "Slow always cleanup" and s.get("conclusion") not in (None, "skipped", ""):
            always = True
        if s.get("name") == "Cancel-only cleanup" and s.get("conclusion") == "success":
            cancel_cleanup = True
    job_end = parse(j.get("completedAt"))
    hang_lag = f"{(hang_end-cancel_at).total_seconds():.0f}s" if hang_end else "—"
    job_lag = f"{(job_end-cancel_at).total_seconds():.0f}s" if job_end else "—"
    print(f"{j['name'][:40]:40} {str(j.get('conclusion') or j['status']):12} {hang_lag:>10} {job_lag:>10} {str(always):>8} {str(cancel_cleanup):>11}")
PY

# Pull Hang logs for signal evidence
echo ""
echo "=== Signal / heartbeat evidence from logs ==="
for name in "A: baseline sleep" "B: tmux wait, no trap" "C: tmux + kill-server trap" "D: parallel_run-style trap" "E: always() 120s after hang"; do
  jid="$(gh run view "$RUN_ID" --json jobs --jq --arg n "$name" '.jobs[] | select(.name==$n) | .databaseId')"
  echo "--- $name ---"
  gh run view --job "$jid" --log 2>/dev/null | rg -n "SMOKE_|caught signal|always-|cancelled-cleanup|Error: The operation was canceled" | tail -20 || true
done
