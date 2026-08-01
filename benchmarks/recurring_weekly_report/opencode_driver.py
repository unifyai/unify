"""Recurring weekly report benchmark: OpenCode comparison arm.

Same protocol as the unify / hermes / openclaw drivers, with one
structural difference that has to be stated up front because it changes
what "firing" means.

**OpenCode ships no scheduler.** There is no cron surface, so the agent
cannot register a recurring job the way the other three arms do; something
outside it has to supply the wake. The harness therefore plays the
scheduler, executing whatever the agent itself declared, in this
precedence:

  1. If the agent wrote a **crontab spec** into the workspace, run the
     command that spec names. This is the agent stating outright what
     should run on a schedule, and is the direct analogue of reading a job
     row out of hermes's or openclaw's cron store.
  2. Else, if it declared a **custom command** (`.opencode/command*/*.md`
     — OpenCode's named-invocable-prompt mechanism), fire it with
     ``opencode run --command <name>``.
  3. Else, if it left exactly one **runnable script** (`*.py` / `*.sh` at
     the workspace root, under `scripts/`, or under `.opencode/`), execute
     that directly — the zero-token path, matching how the hermes arm's
     ``no_agent`` script is fired.
  4. Else, fire a fixed neutral wake prompt (``WAKE_PROMPT``), which is
     what a scheduler with nothing declared would have to do.

Rules 2-4 were fixed before any run. Rule 1 was added after the first
triage runs showed the agent declaring its automation in a `.cron` file
that rules 2-3 could not see — a gap in the harness, not a property of
the system under test. The revision moves strictly toward executing the
agent's own declaration rather than a harness guess, and every experiment
in this arm is run under it.

Which rule fired is recorded per fire in ``results.json`` as
``fire_mode``, so the report can never quietly depend on the choice.
Fires use a fresh session (no ``--continue``), because a scheduler wake
carries no conversation — the same way the other arms' isolated cron
sessions start cold. Whatever the agent persisted into the workspace is
therefore the only thing carried between fires, which is exactly the
property under test.

Metering is neutral: the OpenRouter provider's ``baseURL`` is repointed at
the local recording proxy. Both ``model`` and ``small_model`` are pinned to
``openai/gpt-5.6-sol`` — OpenCode otherwise picks a cheaper model for
title generation, which would leave part of its real cost on a different
provider and off the comparison.

Isolation: per-run XDG dirs (``XDG_{DATA,CACHE,CONFIG,STATE}_HOME``) plus a
fresh git-initialised workspace, so the real ``~/.local/share/opencode``
profile is never read or written.

This module doubles as the shared OpenCode toolkit for the other
experiments' drivers.

Launch via run_opencode.sh.
"""

from __future__ import annotations

import json
import os
import shutil
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
from benchmarks.recurring_weekly_report.hermes_driver import PhaseLedger  # noqa: E402
from benchmarks.recurring_weekly_report.openrouter_proxy import (  # noqa: E402
    RecordingProxy,
)

OPENCODE_REPO = Path(os.environ.get("OCODE_REPO", str(Path.home() / "opencode")))
BENCH_MODEL = os.environ.get("OCODE_MODEL", "openai/gpt-5.6-sol")

WAKE_PROMPT = "Run the recurring automation you set up, now."

# Baseline for the crontab guard. The agent reaches for the host scheduler,
# so an installed entry is removed after *every* agent turn rather than at
# run end: cron fires on minute 0, and a multi-minute run can otherwise
# cross that boundary with a live job pointed at the live fixture.
_CRONTAB_BASELINE: str | None = None
_CRONTAB_GUARD_ARMED = False


def write_opencode_config(
    config_path: Path,
    *,
    proxy_base_url: str,
    model: str = BENCH_MODEL,
) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                "$schema": "https://opencode.ai/config.json",
                "model": f"openrouter/{model}",
                "small_model": f"openrouter/{model}",
                "provider": {
                    "openrouter": {
                        "options": {
                            "baseURL": proxy_base_url,
                            # Reference, never the literal: this file is
                            # archived into the committed results tree.
                            "apiKey": "{env:OPENROUTER_API_KEY}",
                        },
                    },
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def install_cli_shim(state_root: Path) -> Path:
    """Put a real `opencode` on PATH for the agent's own use.

    The harness drives OpenCode from a source checkout via `bun`, so
    without this the agent's own CLI is absent: an observed setup ran
    `opencode --help`, got `command not found`, and abandoned the task.
    A normal install has the binary on PATH, and the agent needs it to
    discover and invoke its own surface (notably `opencode run --command`
    when scheduling itself). The shim inherits the caller's isolated
    environment, so anything the agent launches stays in the sandbox.
    """
    bin_dir = state_root / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    shim = bin_dir / "opencode"
    entry = OPENCODE_REPO / "packages" / "opencode" / "src" / "index.ts"
    shim.write_text(
        "#!/usr/bin/env bash\n" f'exec bun --conditions=browser "{entry}" "$@"\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)
    return bin_dir


def opencode_env(state_root: Path, config_path: Path) -> dict[str, str]:
    bin_dir = install_cli_shim(state_root)
    return {
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
        "HOME": os.environ.get("HOME", ""),
        "XDG_DATA_HOME": str(state_root / "data"),
        "XDG_CACHE_HOME": str(state_root / "cache"),
        "XDG_CONFIG_HOME": str(state_root / "config"),
        "XDG_STATE_HOME": str(state_root / "state"),
        "OPENCODE_CONFIG": str(config_path),
        "OPENCODE_DISABLE_AUTOUPDATE": "1",
        "OPENROUTER_API_KEY": os.environ["OPENROUTER_API_KEY"],
        "NO_COLOR": "1",
        "TERM": "dumb",
    }


def run_opencode(
    args: list[str],
    *,
    workspace: Path,
    state_root: Path,
    config_path: Path,
    log_path: Path,
    timeout_s: float,
) -> tuple[int, str]:
    """Run one headless OpenCode invocation from the workspace."""
    cmd = [
        "bun",
        "--conditions=browser",
        str(OPENCODE_REPO / "packages" / "opencode" / "src" / "index.ts"),
        *args,
    ]
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(f"\n===== {datetime.now(timezone.utc).isoformat()} {args!r}\n")
        log.flush()
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(workspace),
                env=opencode_env(state_root, config_path),
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=timeout_s,
            )
            code, out = proc.returncode, proc.stdout
            log.write(proc.stdout)
            log.write(proc.stderr)
        except subprocess.TimeoutExpired as exc:
            log.write(f"\n===== TIMEOUT after {timeout_s}s\n")
            code = -1
            out = (
                (exc.stdout or b"").decode("utf-8", errors="replace")
                if isinstance(
                    exc.stdout,
                    bytes,
                )
                else (exc.stdout or "")
            )
        if _CRONTAB_GUARD_ARMED:
            for line in restore_crontab(_CRONTAB_BASELINE):
                log.write(f"[crontab-guard] {line}\n")
    return code, out


def workspace_files(workspace: Path) -> list[str]:
    if not workspace.exists():
        return []
    return sorted(
        str(p.relative_to(workspace))
        for p in workspace.rglob("*")
        if p.is_file() and ".git" not in p.parts
    )


def discover_commands(workspace: Path) -> list[str]:
    """Custom command names the agent declared (OpenCode's job analogue)."""
    names: list[str] = []
    for sub in ("command", "commands"):
        d = workspace / ".opencode" / sub
        if d.is_dir():
            names.extend(sorted(p.stem for p in d.glob("*.md")))
    return names


def discover_scripts(workspace: Path) -> list[Path]:
    """Runnable automation scripts the agent left in the workspace."""
    out: list[Path] = []
    for pattern in (
        "*.py",
        "*.sh",
        "scripts/*.py",
        "scripts/*.sh",
        ".opencode/*.py",
        ".opencode/*.sh",
    ):
        out.extend(sorted(workspace.glob(pattern)))
    return out


def discover_cron_command(workspace: Path) -> str | None:
    """The command the agent itself declared should run on a schedule.

    When the agent writes a crontab spec into the workspace, that file
    names the automation explicitly — the most faithful possible answer to
    "what did this system register?", and the direct analogue of reading a
    job row out of hermes's or openclaw's cron store. Returns the command
    with the schedule fields and any output redirect stripped.
    """
    candidates = sorted(workspace.rglob("*.cron")) + sorted(
        workspace.rglob("crontab*"),
    )
    for path in candidates:
        if not path.is_file() or ".git" in path.parts:
            continue
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split(None, 5)
            if len(fields) < 6:
                continue
            command = fields[5]
            for redirect in (">>", "2>&1", ">"):
                idx = command.find(redirect)
                if idx > 0:
                    command = command[:idx]
            command = command.strip()
            if command:
                return command
    return None


def fire_automation(
    *,
    workspace: Path,
    state_root: Path,
    config_path: Path,
    log_path: Path,
    timeout_s: float,
) -> dict[str, Any]:
    """Execute the automation per the firing rule."""
    cron_command = discover_cron_command(workspace)
    if cron_command:
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"\n===== cron-spec fire: {cron_command}\n")
            proc = subprocess.run(
                cron_command,
                shell=True,
                cwd=str(workspace),
                env=opencode_env(state_root, config_path),
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=timeout_s,
            )
            log.write(proc.stdout)
            log.write(proc.stderr)
        return {
            "fire_mode": "cron_spec",
            "exit_code": proc.returncode,
            "output_tail": proc.stdout[-1200:],
        }

    commands = discover_commands(workspace)
    if commands:
        name = commands[0]
        code, out = run_opencode(
            ["run", "--command", name],
            workspace=workspace,
            state_root=state_root,
            config_path=config_path,
            log_path=log_path,
            timeout_s=timeout_s,
        )
        return {
            "fire_mode": f"command:{name}",
            "exit_code": code,
            "output_tail": out[-1200:],
        }

    scripts = discover_scripts(workspace)
    if len(scripts) == 1:
        script = scripts[0]
        runner = (
            ["python3", str(script)]
            if script.suffix == ".py"
            else [
                "bash",
                str(script),
            ]
        )
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"\n===== script fire {script.name}\n")
            proc = subprocess.run(
                runner,
                cwd=str(workspace),
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=timeout_s,
            )
            log.write(proc.stdout)
            log.write(proc.stderr)
        return {
            "fire_mode": f"script:{script.name}",
            "exit_code": proc.returncode,
            "output_tail": proc.stdout[-1200:],
        }

    code, out = run_opencode(
        ["run", WAKE_PROMPT],
        workspace=workspace,
        state_root=state_root,
        config_path=config_path,
        log_path=log_path,
        timeout_s=timeout_s,
    )
    return {
        "fire_mode": "wake_prompt",
        "exit_code": code,
        "output_tail": out[-1200:],
    }


def arm_crontab_guard(results_dir: Path, before: str | None) -> None:
    """Restore the crontab even if the run is killed.

    The defuse in each driver's ``finally`` does not run on SIGTERM/SIGINT,
    and an interrupted run has still had its agent install host jobs — so
    the same restore is wired to those signals and to interpreter exit.
    """
    import atexit
    import signal

    done = {"v": False}

    def _restore(*_args: Any) -> None:
        if done["v"]:
            return
        done["v"] = True
        for line in defuse_host_artifacts(results_dir, before):
            print(f"[defuse] {line}")

    global _CRONTAB_BASELINE, _CRONTAB_GUARD_ARMED
    _CRONTAB_BASELINE = before
    _CRONTAB_GUARD_ARMED = True

    atexit.register(_restore)
    for sig in (signal.SIGTERM, signal.SIGINT):
        previous = signal.getsignal(sig)

        def _handler(signum: int, frame: Any, _prev: Any = previous) -> None:
            _restore()
            if callable(_prev):
                _prev(signum, frame)
            else:
                raise SystemExit(128 + signum)

        signal.signal(sig, _handler)


def snapshot_crontab() -> str | None:
    """The user crontab before the run, or None when none is installed.

    OpenCode has no scheduler of its own, so its agent reaches for the
    host's: observed installing real hourly `crontab` entries pointing at
    the benchmark workspace. Those are live side effects on the machine
    running the suite and must not outlive a run.
    """
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else None


def restore_crontab(before: str | None) -> list[str]:
    """Put the user crontab back exactly as it was; report what changed."""
    actions: list[str] = []
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    after = proc.stdout if proc.returncode == 0 else None
    if after == before:
        return actions
    added = [
        line
        for line in (after or "").splitlines()
        if line.strip() and line not in (before or "").splitlines()
    ]
    actions.append(f"crontab modified by agent; removed {len(added)} entry/entries")
    actions.extend(f"  removed: {line}" for line in added)
    if before is None or not before.strip():
        subprocess.run(["crontab", "-r"], capture_output=True)
    else:
        subprocess.run(["crontab", "-"], input=before, text=True, capture_output=True)
    return actions


def defuse_host_artifacts(results_dir: Path, crontab_before: str | None) -> list[str]:
    """Remove anything the agent installed outside its workspace."""
    import plistlib

    actions = restore_crontab(crontab_before)
    launch_agents = Path.home() / "Library" / "LaunchAgents"
    if launch_agents.is_dir():
        for plist_path in launch_agents.glob("*.plist"):
            try:
                if str(results_dir) not in plist_path.read_text(errors="replace"):
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


def scrub_state_archive(state_root: Path, workspace: Path) -> None:
    """Keep the evidentiary artifacts; drop machine state and caches."""
    for name in ("cache", "state"):
        target = state_root / name
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
    shim_bin = state_root / "bin"
    if shim_bin.is_dir():
        shutil.rmtree(shim_bin, ignore_errors=True)
    data = state_root / "data" / "opencode"
    for name in ("log", "bin", "repos", "snapshot"):
        target = data / name
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
    for auth in data.rglob("auth.json"):
        auth.unlink()
    config_copy = state_root / "config" / "opencode"
    if config_copy.is_dir():
        shutil.rmtree(config_copy, ignore_errors=True)
    ws_git = workspace / ".git"
    if ws_git.is_dir():
        shutil.rmtree(ws_git, ignore_errors=True)


def prepare_workspace(workspace: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init", "-q"],
        cwd=str(workspace),
        capture_output=True,
    )


def require_opencode() -> None:
    if not os.environ.get("OPENROUTER_API_KEY"):
        raise SystemExit("OPENROUTER_API_KEY is required (use run_opencode.sh)")
    entry = OPENCODE_REPO / "packages" / "opencode" / "src" / "index.ts"
    if not entry.exists():
        raise SystemExit(
            f"OpenCode checkout missing at {OPENCODE_REPO} — clone sst/opencode "
            "and run `bun install`",
        )


def main() -> int:
    require_opencode()

    seed = int(os.environ.get("RWR_SEED", DEFAULT_SEED))
    fixture_port = int(os.environ.get("RWR_PORT", DEFAULT_PORT))
    proxy_port = int(os.environ.get("RWR_PROXY_PORT", "8174"))
    n_runs = int(os.environ.get("RWR_RUNS", "4"))
    phase_timeout_s = float(os.environ.get("RWR_PHASE_TIMEOUT_S", "1800"))
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ") + "-opencode"

    results_dir = EXPERIMENT_DIR / "results" / run_id
    results_dir.mkdir(parents=True, exist_ok=True)
    state_root = results_dir / "opencode_state"
    workspace = results_dir / "workspace"
    config_path = results_dir / "opencode.json"
    log_path = results_dir / "opencode_cli.log"
    prepare_workspace(workspace)
    crontab_before = snapshot_crontab()
    arm_crontab_guard(results_dir, crontab_before)

    proxy = RecordingProxy(
        port=proxy_port,
        ledger_path=results_dir / "proxy_ledger.jsonl",
    ).start()
    fixture = FixtureServer(seed=seed, port=fixture_port).start()
    print(f"[fixture] {fixture.base_url} (seed={seed})")
    print(f"[proxy] {proxy.base_url} -> openrouter.ai")

    write_opencode_config(config_path, proxy_base_url=proxy.base_url)
    ledger = PhaseLedger(results_dir / "proxy_ledger.jsonl")

    utterance = UTTERANCE_TEMPLATE.format(base_url=fixture.base_url)
    results: dict[str, Any] = {
        "experiment": "recurring_weekly_report",
        "system": "opencode",
        "run_id": run_id,
        "opencode_repo": str(OPENCODE_REPO),
        "model": BENCH_MODEL,
        "seed": seed,
        "n_runs": n_runs,
        "utterance": utterance,
        "wake_prompt": WAKE_PROMPT,
        "runs": [],
    }

    try:
        print("[setup] issuing utterance to opencode ...")
        start = ledger.count()
        t0 = time.monotonic()
        code, out = run_opencode(
            ["run", utterance],
            workspace=workspace,
            state_root=state_root,
            config_path=config_path,
            log_path=log_path,
            timeout_s=phase_timeout_s,
        )
        ledger.mark("setup", start, ledger.count(), time.monotonic() - t0)
        results["setup"] = {"exit_code": code, "output_tail": out[-2000:]}
        print(f"[setup] exit={code}")

        results["profile_after_setup"] = {
            "workspace_files": workspace_files(workspace),
            "commands": discover_commands(workspace),
            "scripts": [p.name for p in discover_scripts(workspace)],
        }
        print(f"[setup] persisted: {results['profile_after_setup']}")

        reports_seen = 0
        for i in range(1, n_runs + 1):
            run_date = datetime.now(timezone.utc).date()
            print(f"[run_{i}] firing ...")
            start = ledger.count()
            t0 = time.monotonic()
            fire = fire_automation(
                workspace=workspace,
                state_root=state_root,
                config_path=config_path,
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
                "fire_mode": fire["fire_mode"],
                "exit_code": fire["exit_code"],
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
                f"[run_{i}] mode={fire['fire_mode']} reports={len(delivered)} "
                f"correct={run_row['correct']}",
            )

        results["profile_final"] = {
            "workspace_files": workspace_files(workspace),
            "commands": discover_commands(workspace),
            "scripts": [p.name for p in discover_scripts(workspace)],
        }
    finally:
        results["defuse_actions"] = defuse_host_artifacts(
            results_dir,
            crontab_before,
        )
        if results["defuse_actions"]:
            print(f"[defuse] {results['defuse_actions']}")
        scrub_state_archive(state_root, workspace)
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
        f"# recurring_weekly_report (opencode arm) — {results['run_id']}",
        "",
        f"- model: `{results['model']}` via local recording proxy -> OpenRouter",
        f"- opencode repo: `{results['opencode_repo']}`",
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
        "| run | fire mode | reports | correct |",
        "|---|---|---|---|",
    ]
    for r in results.get("runs", []):
        lines.append(
            f"| {r['run']} | {r['fire_mode']} | {r['reports_delivered']} | "
            f"{r['correct']} |",
        )
    summary = "\n".join(lines) + "\n"
    (results_dir / "summary.md").write_text(summary, encoding="utf-8")

    fixture.stop()
    proxy.stop()
    print(f"\n{summary}")
    print(f"[done] results in {results_dir}")


if __name__ == "__main__":
    sys.exit(main())
