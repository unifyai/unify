"""Recurring weekly report benchmark: hermes-agent comparison arm.

Identical protocol to the unify driver (harness.py), applied to hermes-agent:

  - The literally identical natural-language utterance is given to the hermes
    agent as one headless chat message (``cli.py -q ...``). No manual cron
    setup, no skill authoring — the agent self-organizes, exactly as the
    unify actor did.
  - Whatever recurring automation the agent created is then fired N times
    via hermes's own manual trigger (``hermes cron run <id>``), which
    executes the job in-process exactly like a scheduler tick would.
  - The same seeded fixture serves the data and receives the reports, and
    the same ground-truth scorer grades every delivered report.

Metering is neutral: hermes's OpenAI-compatible ``base_url`` points at a
local recording proxy (openrouter_proxy.py) that forwards to OpenRouter
unchanged and records provider-reported usage per call — the same source of
truth the unify arm's in-process hook read. Model is pinned to the same
``openai/gpt-5.6-sol`` via OpenRouter.

Isolation: a throwaway ``HERMES_HOME`` under the results directory, so no
real hermes profile is touched; the agent's shell cwd is a scratch
workspace.

Launch via run_hermes.sh.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EXPERIMENT_DIR = Path(__file__).resolve().parent

from benchmarks.recurring_weekly_report.fixture import (  # noqa: E402
    DEFAULT_PORT,
    DEFAULT_SEED,
    FixtureServer,
    expected_report,
    score_report,
)
from benchmarks.recurring_weekly_report.harness import UTTERANCE_TEMPLATE  # noqa: E402
from benchmarks.recurring_weekly_report.openrouter_proxy import (  # noqa: E402
    RecordingProxy,
)

HERMES_REPO = Path(
    os.environ.get("RWR_HERMES_REPO", str(Path.home() / "hermes-agent")),
)
BENCH_MODEL = os.environ.get("RWR_MODEL", "openai/gpt-5.6-sol")

# base_url deliberately lives in OPENROUTER_BASE_URL (set per subprocess):
# hermes only trusts config-file base_url for auto/custom providers, while
# the env var is its first-class "OpenRouter mirror/proxy" override that
# keeps OPENROUTER_API_KEY selection intact (hermes_cli/runtime_provider.py).
CONFIG_TEMPLATE = """\
model:
  default: "{model}"
  provider: "openrouter"
"""


class PhaseLedger:
    """Phase windows over the proxy ledger file (counts + aggregation)."""

    def __init__(self, ledger_path: Path) -> None:
        self.ledger_path = ledger_path
        self.marks: list[tuple[str, int, int, float]] = []

    def _lines(self) -> list[dict[str, Any]]:
        if not self.ledger_path.exists():
            return []
        rows = []
        for line in self.ledger_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def count(self) -> int:
        return len(self._lines())

    def mark(self, name: str, start: int, end: int, wall: float) -> None:
        self.marks.append((name, start, end, wall))

    def summarize(self) -> list[dict[str, Any]]:
        rows = self._lines()
        phases = []
        covered: set[int] = set()
        for name, start, end, wall in self.marks:
            stats = {
                "name": name,
                "wall_seconds": round(wall, 2),
                "llm_calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "provider_cost_usd": None,
                "usage_missing_calls": 0,
                "other_http_calls": 0,
                "models": {},
            }
            for idx in range(start, min(end, len(rows))):
                row = rows[idx]
                covered.add(idx)
                # Only completion requests are LLM calls; catalog/model GETs
                # are free metadata traffic and would inflate the count.
                if "/chat/completions" not in str(row.get("path", "")):
                    stats["other_http_calls"] += 1
                    continue
                stats["llm_calls"] += 1
                stats["prompt_tokens"] += int(row.get("prompt_tokens") or 0)
                stats["completion_tokens"] += int(row.get("completion_tokens") or 0)
                stats["total_tokens"] += int(row.get("total_tokens") or 0)
                if row.get("usage_missing"):
                    stats["usage_missing_calls"] += 1
                model = row.get("response_model") or row.get("request_model") or "?"
                stats["models"][model] = stats["models"].get(model, 0) + 1
            phases.append(stats)
        background = {
            "name": "background",
            "wall_seconds": 0.0,
            "llm_calls": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "provider_cost_usd": None,
            "usage_missing_calls": 0,
            "models": {},
        }
        for idx, row in enumerate(rows):
            if idx in covered:
                continue
            if "/chat/completions" not in str(row.get("path", "")):
                background["other_http_calls"] = (
                    background.get("other_http_calls", 0) + 1
                )
                continue
            background["llm_calls"] += 1
            background["prompt_tokens"] += int(row.get("prompt_tokens") or 0)
            background["completion_tokens"] += int(row.get("completion_tokens") or 0)
            background["total_tokens"] += int(row.get("total_tokens") or 0)
        if background["llm_calls"] or background.get("other_http_calls"):
            phases.append(background)
        return phases


def _hermes_env(
    hermes_home: Path,
    workdir: Path,
    proxy_base_url: str,
) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "HERMES_HOME": str(hermes_home),
        "OPENROUTER_API_KEY": os.environ["OPENROUTER_API_KEY"],
        "OPENROUTER_BASE_URL": proxy_base_url,
        "TERMINAL_CWD": str(workdir),
        "PYTHONUNBUFFERED": "1",
        "TERM": "dumb",
        "NO_COLOR": "1",
    }
    return env


def _run_hermes(
    args: list[str],
    *,
    hermes_home: Path,
    workdir: Path,
    proxy_base_url: str,
    log_path: Path,
    timeout_s: float,
) -> tuple[int, str]:
    """Run a hermes CLI invocation headless; returns (returncode, tail)."""
    cmd = [str(HERMES_REPO / ".venv" / "bin" / "hermes"), *args]
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(f"\n===== {datetime.now(timezone.utc).isoformat()} {args!r}\n")
        log.flush()
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(HERMES_REPO),
                env=_hermes_env(hermes_home, workdir, proxy_base_url),
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                timeout=timeout_s,
            )
            code = proc.returncode
        except subprocess.TimeoutExpired:
            log.write(f"\n===== TIMEOUT after {timeout_s}s\n")
            code = -1
    tail = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
    return code, tail


def _load_cron_jobs(hermes_home: Path) -> list[dict[str, Any]]:
    jobs_file = hermes_home / "cron" / "jobs.json"
    if not jobs_file.exists():
        return []
    data = json.loads(jobs_file.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        jobs = data.get("jobs")
        return jobs if isinstance(jobs, list) else []
    return data if isinstance(data, list) else []


def defuse_hermes_artifacts(hermes_home: Path) -> list[str]:
    """Neutralize live machinery the hermes agent may have left behind.

    The agent can legitimately create recurring cron jobs, spawn a gateway
    process to tick them, and (observed in the drift_recovery operator-fix
    session) even install a persistent launchd service — all pointed at the
    throwaway HERMES_HOME. Results directories must be inert artifacts, so
    after a run: disable every cron job in the profile, kill any gateway
    process whose environment binds this home, and remove launchd agents
    that reference it. Returns a log of actions taken.
    """
    import plistlib
    import signal
    import subprocess

    actions: list[str] = []
    jobs_file = hermes_home / "cron" / "jobs.json"
    if jobs_file.exists():
        data = json.loads(jobs_file.read_text(encoding="utf-8"))
        jobs = data.get("jobs") if isinstance(data, dict) else data
        for job in jobs or []:
            if job.get("enabled"):
                job["enabled"] = False
                job["state"] = "paused"
                job["paused_reason"] = "benchmark artifact - defused post-run"
                actions.append(f"disabled cron job {job.get('id')}")
        jobs_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    home_str = str(hermes_home)
    try:
        pids = subprocess.run(
            ["pgrep", "-f", "hermes_cli.main gateway"],
            capture_output=True,
            text=True,
        ).stdout.split()
    except Exception:
        pids = []
    for pid in pids:
        try:
            env_dump = subprocess.run(
                ["ps", "eww", pid],
                capture_output=True,
                text=True,
            ).stdout
            if home_str in env_dump:
                os.kill(int(pid), signal.SIGTERM)
                actions.append(f"killed gateway pid {pid}")
        except Exception:
            continue

    launch_agents = Path.home() / "Library" / "LaunchAgents"
    if launch_agents.is_dir():
        for plist_path in launch_agents.glob("*hermes*.plist"):
            try:
                if home_str not in plist_path.read_text(errors="replace"):
                    continue
                label = plistlib.loads(plist_path.read_bytes()).get("Label")
                if label:
                    subprocess.run(
                        ["launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
                        capture_output=True,
                    )
                plist_path.unlink()
                actions.append(f"removed launch agent {plist_path.name}")
            except Exception:
                continue
    return actions


def _snapshot_profile_artifacts(hermes_home: Path) -> dict[str, Any]:
    """Record what the agent persisted: cron jobs, skills, scripts."""
    artifacts: dict[str, Any] = {"cron_jobs": _load_cron_jobs(hermes_home)}
    skills_dir = hermes_home / "skills"
    artifacts["skills"] = (
        sorted(
            str(p.relative_to(skills_dir)) for p in skills_dir.rglob("*") if p.is_file()
        )
        if skills_dir.exists()
        else []
    )
    scripts_dir = hermes_home / "scripts"
    artifacts["scripts"] = (
        sorted(
            str(p.relative_to(scripts_dir))
            for p in scripts_dir.rglob("*")
            if p.is_file()
        )
        if scripts_dir.exists()
        else []
    )
    return artifacts


def main() -> int:
    if not os.environ.get("OPENROUTER_API_KEY"):
        raise SystemExit("OPENROUTER_API_KEY is required (use run_hermes.sh)")
    if not (HERMES_REPO / ".venv" / "bin" / "hermes").exists():
        raise SystemExit(f"hermes binary missing — run `uv sync` in {HERMES_REPO}")

    seed = int(os.environ.get("RWR_SEED", DEFAULT_SEED))
    fixture_port = int(os.environ.get("RWR_PORT", DEFAULT_PORT))
    proxy_port = int(os.environ.get("RWR_PROXY_PORT", "8124"))
    n_runs = int(os.environ.get("RWR_RUNS", "4"))
    phase_timeout_s = float(os.environ.get("RWR_PHASE_TIMEOUT_S", "1800"))
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ") + "-hermes"

    results_dir = EXPERIMENT_DIR / "results" / run_id
    results_dir.mkdir(parents=True, exist_ok=True)
    hermes_home = results_dir / "hermes_home"
    hermes_home.mkdir(parents=True, exist_ok=True)
    workdir = results_dir / "workspace"
    workdir.mkdir(parents=True, exist_ok=True)
    log_path = results_dir / "hermes_cli.log"

    proxy = RecordingProxy(
        port=proxy_port,
        ledger_path=results_dir / "proxy_ledger.jsonl",
    ).start()
    fixture = FixtureServer(seed=seed, port=fixture_port).start()
    print(f"[fixture] {fixture.base_url} (seed={seed})")
    print(f"[proxy] {proxy.base_url} -> openrouter.ai")

    (hermes_home / "config.yaml").write_text(
        CONFIG_TEMPLATE.format(model=BENCH_MODEL),
        encoding="utf-8",
    )
    ledger = PhaseLedger(results_dir / "proxy_ledger.jsonl")

    utterance = UTTERANCE_TEMPLATE.format(base_url=fixture.base_url)
    results: dict[str, Any] = {
        "experiment": "recurring_weekly_report",
        "system": "hermes-agent",
        "run_id": run_id,
        "hermes_repo": str(HERMES_REPO),
        "model": BENCH_MODEL,
        "seed": seed,
        "n_runs": n_runs,
        "utterance": utterance,
        "runs": [],
    }

    # ── Phase: setup (identical utterance, one headless chat message) ──────
    print("[setup] issuing utterance to hermes ...")
    start = ledger.count()
    t0 = time.monotonic()
    code, tail = _run_hermes(
        ["chat", "-q", utterance],
        hermes_home=hermes_home,
        workdir=workdir,
        proxy_base_url=proxy.base_url,
        log_path=log_path,
        timeout_s=phase_timeout_s,
    )
    ledger.mark("setup", start, ledger.count(), time.monotonic() - t0)
    results["setup"] = {"exit_code": code, "log_tail": tail}
    print(f"[setup] exit={code}")

    jobs = _load_cron_jobs(hermes_home)
    results["profile_after_setup"] = _snapshot_profile_artifacts(hermes_home)
    if len(jobs) != 1:
        _finalize(results, ledger, results_dir, fixture, proxy)
        print(f"[abort] expected exactly one cron job after setup, found {len(jobs)}")
        return 1
    job = jobs[0]
    job_id = str(job.get("id"))
    print(f"[setup] cron job created: {job_id} ({job.get('name')})")

    # ── Phases: N manual fires of the agent-created job ────────────────────
    reports_seen = 0
    for i in range(1, n_runs + 1):
        run_date = datetime.now(timezone.utc).date()
        print(f"[run_{i}] firing cron job {job_id} ...")
        start = ledger.count()
        t0 = time.monotonic()
        code, tail = _run_hermes(
            ["cron", "run", job_id],
            hermes_home=hermes_home,
            workdir=workdir,
            proxy_base_url=proxy.base_url,
            log_path=log_path,
            timeout_s=phase_timeout_s,
        )
        ledger.mark(f"run_{i}", start, ledger.count(), time.monotonic() - t0)

        delivered = fixture.sink.snapshot()[reports_seen:]
        reports_seen += len(delivered)
        expected = expected_report(seed, run_date)
        scores = [score_report(r["body"], expected) for r in delivered]
        run_row = {
            "run": i,
            "run_date": run_date.isoformat(),
            "exit_code": code,
            "reports_delivered": len(delivered),
            "reports": [r["body"] for r in delivered],
            "expected_report": expected,
            "scores": scores,
            "correct": (
                len(delivered) == 1 and scores[0]["correct"] if scores else False
            ),
        }
        results["runs"].append(run_row)
        print(
            f"[run_{i}] exit={code} reports={len(delivered)} correct={run_row['correct']}",
        )

    results["profile_final"] = _snapshot_profile_artifacts(hermes_home)
    results["defuse_actions"] = defuse_hermes_artifacts(hermes_home)
    _finalize(results, ledger, results_dir, fixture, proxy)
    return 0


def _finalize(
    results: dict[str, Any],
    ledger: PhaseLedger,
    results_dir: Path,
    fixture: Any,
    proxy: Any,
) -> None:
    results["phases"] = ledger.summarize()
    results["finished_at"] = datetime.now(timezone.utc).isoformat()
    with open(results_dir / "results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    lines = [
        f"# recurring_weekly_report (hermes-agent arm) — {results['run_id']}",
        "",
        f"- model: `{results['model']}` via local recording proxy -> OpenRouter",
        f"- hermes repo: `{results['hermes_repo']}`",
        "",
        "| phase | LLM calls | prompt tok | completion tok | usage-missing | wall (s) |",
        "|---|---|---|---|---|---|",
    ]
    for p in results["phases"]:
        lines.append(
            f"| {p['name']} | {p['llm_calls']} | {p['prompt_tokens']} | "
            f"{p['completion_tokens']} | {p['usage_missing_calls']} | {p['wall_seconds']} |",
        )
    lines += [
        "",
        "| run | exit | reports | correct |",
        "|---|---|---|---|",
    ]
    for r in results.get("runs", []):
        lines.append(
            f"| {r['run']} | {r['exit_code']} | {r['reports_delivered']} | {r['correct']} |",
        )
    summary = "\n".join(lines) + "\n"
    (results_dir / "summary.md").write_text(summary, encoding="utf-8")

    fixture.stop()
    proxy.stop()
    print(f"\n{summary}")
    print(f"[done] results in {results_dir}")


if __name__ == "__main__":
    sys.exit(main())
