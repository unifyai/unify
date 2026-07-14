#!/usr/bin/env bash
# Drive the Cancel Smoke workflow: dispatch → wait → cancel → report timings.
# Usage: bash scripts/dev/run_cancel_smoke.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "Dispatching Cancel Smoke on staging..."
gh workflow run cancel-smoke.yml --ref staging

# Resolve the new run id
sleep 3
RUN_ID="$(gh run list --workflow=cancel-smoke.yml --branch=staging --limit 1 --json databaseId,status --jq '.[0].databaseId')"
echo "Run: https://github.com/unifyai/unify/actions/runs/${RUN_ID}"

echo "Waiting until all Hang steps are in_progress..."
for _ in $(seq 1 60); do
  payload="$(gh run view "$RUN_ID" --json status,jobs)"
  ready="$(python3 -c "
import json,sys
d=json.load(sys.stdin)
jobs=[j for j in d['jobs'] if j['name'].startswith(('A:','B:','C:','D:','E:'))]
if len(jobs) < 5:
    print('0'); sys.exit(0)
n=0
for j in jobs:
    for s in j.get('steps') or []:
        if s.get('name') in ('Hang',) and s.get('status')=='in_progress':
            n+=1
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
  exit 1
fi

CANCEL_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Cancelling at ${CANCEL_AT}..."
gh run cancel "$RUN_ID"
echo "$CANCEL_AT" > /tmp/cancel_smoke_cancel_at.txt
echo "$RUN_ID" > /tmp/cancel_smoke_run_id.txt

echo "Polling until run completes..."
for _ in $(seq 1 120); do
  st="$(gh run view "$RUN_ID" --json status,conclusion --jq '.status + "/" + (.conclusion // "-")')"
  echo "  ${st}"
  if [[ "$st" != in_progress/* && "$st" != pending/* && "$st" != queued/* ]]; then
    break
  fi
  sleep 5
done

DONE_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Done at ${DONE_AT}. Analysing..."

python3 - <<PY
import json, subprocess
from datetime import datetime, timezone

run_id = open("/tmp/cancel_smoke_run_id.txt").read().strip()
cancel_at = datetime.fromisoformat(open("/tmp/cancel_smoke_cancel_at.txt").read().strip().replace("Z","+00:00"))

raw = subprocess.check_output(["gh","run","view",run_id,"--json","status,conclusion,jobs,url"], text=True)
d = json.loads(raw)
print(f"\\nRun {run_id}: {d['status']}/{d.get('conclusion')}  {d['url']}")
print(f"Cancel requested at: {cancel_at.isoformat()}")
print()
print(f"{'job':40} {'conclusion':12} {'hang_end_lag':>12} {'job_end_lag':>12} {'always_ran':>10} {'cancel_step':>11}")
print("-"*100)

def parse(ts):
    if not ts or ts.startswith("0001"):
        return None
    return datetime.fromisoformat(ts.replace("Z","+00:00"))

for j in sorted(d["jobs"], key=lambda x: x["name"]):
    hang_end = None
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
    print(f"{j['name'][:40]:40} {str(j.get('conclusion') or j['status']):12} {hang_lag:>12} {job_lag:>12} {str(always):>10} {str(cancel_cleanup):>11}")
PY
