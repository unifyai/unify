#!/usr/bin/env python3
"""Seed the global builtins catalogues (primitives, guidance, integrations).

Creates (or converges) the public-read ``Builtins`` Unify project holding
one platform-wide copy of every manager's static primitive rows and the
builtin guidance library imported from the Agent Skills ecosystem, plus the
vector columns required for read-only ranked search.

Run with the API key of the account that should OWN the catalogue (the
platform admin account on hosted deployments; the shared key on self-host):

    UNIFY_KEY=<admin-key> ORCHESTRA_URL=<api-url> \
        .venv/bin/python scripts/seed_builtins_catalog.py

The run is idempotent and hash-guarded (per manager for primitives, per
skill for guidance), so it is safe (and cheap) to invoke on every deploy.

Self-host and local provider-backed integration catalogs can be bootstrapped
from the same manifest used to configure Orchestra provider backends. Select
exactly one executor; the script never falls back to another executor after
failure:

    UNIFY_KEY=<admin-key> ORCHESTRA_ADMIN_KEY=<admin-key> ORCHESTRA_URL=<api-url> \
        UNITY_INTEGRATION_BOOTSTRAP_EXECUTOR=direct_worker \
        .venv/bin/python scripts/seed_builtins_catalog.py \
        --integration-bootstrap-manifest deploy/selfhost/integration-bootstrap.selfhost.toml
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shlex
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path
from typing import Any, NamedTuple
from urllib import error, parse, request

_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

SYNC_PASSTHROUGH_FIELDS = {
    "tool_limit_per_app",
    "component_limit_per_app",
    "include_all_managed_apps",
    "include_all_apps",
    "create_auth_configs",
    "sync_tools",
    "prune_unlisted_apps",
}
DEFAULT_COMPOSIO_BATCH_SIZE = 25
BOOTSTRAP_STATUS_SUCCESS = "success"
BUILTINS_BOOTSTRAP_SEED_OWNER = "public-builtins"
INTEGRATION_BOOTSTRAP_EXECUTORS = {"direct_worker", "api", "none"}


class ProviderBootstrapPlan(NamedTuple):
    backend_id: str
    environment: str
    desired_hash: str
    desired_config: dict[str, Any]
    backend_payload: dict[str, Any]
    sync_payload: dict[str, Any] | None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--integration-bootstrap-manifest",
        default=os.environ.get("UNITY_INTEGRATION_BOOTSTRAP_MANIFEST", ""),
        help=(
            "Optional provider bootstrap manifest. When provided, this script "
            "syncs Orchestra provider catalog rows and immediately seeds the "
            "returned app-scoped rows into Builtins."
        ),
    )
    parser.add_argument(
        "--admin-key",
        default=os.environ.get("ORCHESTRA_ADMIN_KEY", ""),
        help="Orchestra admin key. Defaults to ORCHESTRA_ADMIN_KEY.",
    )
    parser.add_argument(
        "--skip-integrations",
        action="store_true",
        default=os.environ.get("UNITY_SKIP_BUILTINS_INTEGRATIONS", "").lower()
        in {"1", "true", "yes"},
        help="Seed only primitives and guidance; integration bootstrap is handled elsewhere.",
    )
    return parser.parse_args(argv)


def _integration_bootstrap_executor(environment: str) -> str:
    executor = os.environ.get("UNITY_INTEGRATION_BOOTSTRAP_EXECUTOR", "").strip()
    if not executor:
        raise ValueError(
            "UNITY_INTEGRATION_BOOTSTRAP_EXECUTOR is required when an integration "
            "bootstrap manifest is provided. Select exactly one of: "
            f"{', '.join(sorted(INTEGRATION_BOOTSTRAP_EXECUTORS))}",
        )
    if executor not in INTEGRATION_BOOTSTRAP_EXECUTORS:
        raise ValueError(
            f"Invalid UNITY_INTEGRATION_BOOTSTRAP_EXECUTOR={executor!r}; expected one "
            f"of {', '.join(sorted(INTEGRATION_BOOTSTRAP_EXECUTORS))}",
        )
    return executor


def _load_manifest(path: str) -> dict[str, Any]:
    with open(path, "rb") as file:
        if path.endswith(".json"):
            import json

            manifest = json.loads(file.read().decode("utf-8"))
        else:
            manifest = tomllib.load(file)
    if not isinstance(manifest, dict):
        raise ValueError("Integration bootstrap manifest must be an object")
    if not isinstance(manifest.get("providers"), dict):
        raise ValueError("Integration bootstrap manifest providers must be an object")
    return manifest


def _backend_payload(
    *,
    backend_id: str,
    environment: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    status = config.get("status", "disabled")
    if status not in {"enabled", "disabled"}:
        raise ValueError(f"{backend_id}: status must be enabled or disabled")
    return {
        "backend_id": backend_id,
        "kind": config.get("kind") or backend_id,
        "environment": environment,
        "display_name": config.get("display_name") or backend_id.title(),
        "status": status,
        "allowed_orgs_or_tenants": config.get("allowed_orgs_or_tenants") or [],
        "default_priority": int(config.get("default_priority", 100)),
        "config_json": config.get("config_json") or {},
    }


def _sync_payload(*, backend_id: str, config: dict[str, Any]) -> dict[str, Any] | None:
    if config.get("status") != "enabled":
        return None
    sync = config.get("sync")
    if not sync:
        return None
    mode = sync.get("mode", "partial")
    if mode not in {"partial", "full"}:
        raise ValueError(f"{backend_id}: sync.mode must be partial or full")
    payload: dict[str, Any] = {
        "backend_id": backend_id,
        "app_slugs": [] if mode == "full" else list(sync.get("app_slugs") or []),
        "sync_mode": mode,
    }
    for field in SYNC_PASSTHROUGH_FIELDS:
        if field in sync:
            payload[field] = sync[field]
    if mode == "full":
        if "include_all_managed_apps" in payload:
            payload["include_all_managed_apps"] = True
        if "include_all_apps" in payload:
            payload["include_all_apps"] = True
    return payload


def _provider_plan(
    *,
    manifest: dict[str, Any],
    backend_id: str,
    config: dict[str, Any],
) -> ProviderBootstrapPlan:
    environment = str(manifest.get("environment") or "selfhost")
    backend_payload = _backend_payload(
        backend_id=backend_id,
        environment=environment,
        config=config,
    )
    sync_payload = _sync_payload(backend_id=backend_id, config=config)
    desired_sync_config = None
    if sync_payload:
        desired_sync_config = {
            **sync_payload,
            "mode": sync_payload.get("sync_mode", "partial"),
        }
    desired_config = {
        "schema_version": manifest.get("schema_version", 1),
        "environment": environment,
        "seed_owner": BUILTINS_BOOTSTRAP_SEED_OWNER,
        "backend": backend_payload,
        "sync": desired_sync_config,
    }
    desired_hash = hashlib.sha256(_json_dumps(desired_config).encode()).hexdigest()
    if sync_payload:
        sync_payload = {
            **sync_payload,
            "cache_version": (
                f"{BUILTINS_BOOTSTRAP_SEED_OWNER}-{environment}-"
                f"{backend_id}-{desired_hash[:12]}"
            ),
        }
    return ProviderBootstrapPlan(
        backend_id=backend_id,
        environment=environment,
        desired_hash=desired_hash,
        desired_config=desired_config,
        backend_payload=backend_payload,
        sync_payload=sync_payload,
    )


def _admin_request(
    *,
    base_url: str,
    admin_key: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 120,
) -> dict[str, Any]:
    import json

    data = None if payload is None else json.dumps(payload).encode("utf-8")
    started_at = time.perf_counter()
    req = request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {admin_key}",
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            logging.info(
                "Orchestra admin request complete method=%s path=%s status=%s elapsed=%.1fs",
                method,
                path,
                response.status,
                time.perf_counter() - started_at,
            )
            return json.loads(body) if body else {}
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {path} failed with HTTP {exc.code}: {detail}",
        ) from exc


def _bootstrap_state(
    *,
    base_url: str,
    admin_key: str,
    environment: str,
    backend_id: str,
) -> dict[str, Any] | None:
    query = parse.urlencode({"environment": environment, "backend_id": backend_id})
    try:
        return _admin_request(
            base_url=base_url,
            admin_key=admin_key,
            method="GET",
            path=f"/admin/integrations/bootstrap-state?{query}",
        )
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return None
        raise


def _bootstrap_state_matches(
    *,
    state: dict[str, Any] | None,
    plan: ProviderBootstrapPlan,
) -> bool:
    diagnostics = (state or {}).get("last_sync_diagnostics") or {}
    return (
        bool(state)
        and state.get("desired_hash") == plan.desired_hash
        and state.get("last_status") in {BOOTSTRAP_STATUS_SUCCESS, "skipped"}
        and diagnostics.get("seed_owner") == BUILTINS_BOOTSTRAP_SEED_OWNER
        and diagnostics.get("builtins_seeded") is True
    )


def _sync_diagnostics(
    *,
    plan: ProviderBootstrapPlan,
    result: dict[str, Any] | None,
    builtins_changed: bool,
) -> dict[str, Any]:
    sync_config = plan.desired_config.get("sync") or {}
    sync_mode = sync_config.get("mode") or sync_config.get("sync_mode")
    diagnostics: dict[str, Any] = {
        "seed_owner": BUILTINS_BOOTSTRAP_SEED_OWNER,
        "builtins_seeded": True,
        "builtins_changed": builtins_changed,
        "sync_mode": sync_mode,
        "requested_app_slugs": list(sync_config.get("app_slugs") or []),
        "prune_unlisted_apps": bool(sync_config.get("prune_unlisted_apps", False)),
    }
    if result:
        for field in (
            "status",
            "error",
            "warning",
            "auth_configs_created",
            "auth_configs_reused",
            "cache_version",
            "prune_unlisted_apps",
            "apps_pruned",
            "tools_pruned",
            "apps_upserted",
            "tools_upserted",
            "apps_inserted",
            "apps_updated",
            "tools_inserted",
            "tools_updated",
            "skipped_batches",
            "completed_batches",
            "executor",
            "run_id",
            "desired_hash",
            "elapsed_seconds",
            "exit_code",
        ):
            if field in result:
                diagnostics[field] = result[field]
        for field in ("skipped_apps", "matched_app_slugs", "diagnostics"):
            if field in result:
                diagnostics[field] = result[field]
    return diagnostics


def _put_bootstrap_state(
    *,
    base_url: str,
    admin_key: str,
    plan: ProviderBootstrapPlan,
    status: str,
    result: dict[str, Any] | None = None,
    builtins_changed: bool = False,
    error_message: str | None = None,
) -> None:
    logging.info(
        "Writing integration bootstrap state backend=%s environment=%s status=%s "
        "builtins_changed=%s error=%s",
        plan.backend_id,
        plan.environment,
        status,
        builtins_changed,
        bool(error_message),
    )
    diagnostics = _sync_diagnostics(
        plan=plan,
        result=result,
        builtins_changed=builtins_changed,
    )
    if error_message:
        diagnostics["error"] = error_message[:1000]
        diagnostics["builtins_seeded"] = False
    payload = {
        "environment": plan.environment,
        "backend_id": plan.backend_id,
        "desired_hash": plan.desired_hash,
        "desired_config": plan.desired_config,
        "last_status": status,
        "last_error": error_message[:1000] if error_message else None,
        "apps_upserted": int((result or {}).get("apps_upserted", 0) or 0),
        "tools_upserted": int((result or {}).get("tools_upserted", 0) or 0),
        "last_sync_diagnostics": diagnostics,
    }
    _admin_request(
        base_url=base_url,
        admin_key=admin_key,
        method="PUT",
        path="/admin/integrations/bootstrap-state",
        payload=payload,
    )


def _effective_prune(payload: dict[str, Any]) -> bool:
    return (
        bool(payload.get("prune_unlisted_apps")) or payload.get("sync_mode") == "full"
    )


def _seed_sync_result(
    *,
    result: dict[str, Any],
    sync_payload: dict[str, Any],
) -> bool:
    from unity.integrations.builtins_catalog import seed_builtin_integrations

    if result.get("status") == "failed":
        raise RuntimeError(
            result.get("error")
            or f"{sync_payload['backend_id']}: integration catalog sync failed",
        )
    apps = (result.get("apps") or []) if sync_payload.get("_seed_apps", True) else None
    tools = (
        (result.get("tools") or []) if sync_payload.get("_seed_tools", True) else None
    )
    app_slugs = result.get("matched_app_slugs") or sync_payload.get("app_slugs") or []
    phase = str(sync_payload.get("_seed_phase") or "sync")
    started_at = time.perf_counter()
    logging.info(
        "Seeding integration Builtins start phase=%s backend=%s apps=%s tools=%s "
        "app_slugs=%s prune=%s",
        phase,
        sync_payload["backend_id"],
        "omitted" if apps is None else len(apps),
        "omitted" if tools is None else len(tools),
        len(app_slugs),
        _effective_prune(sync_payload),
    )
    changed = seed_builtin_integrations(
        apps=apps,
        tools=tools,
        backend_id=str(sync_payload["backend_id"]),
        app_slugs=[str(slug) for slug in app_slugs],
        prune_unlisted_apps=_effective_prune(sync_payload),
    )
    logging.info(
        "Seeding integration Builtins complete phase=%s backend=%s apps=%s tools=%s "
        "changed=%s prune=%s elapsed=%.1fs",
        phase,
        sync_payload["backend_id"],
        "omitted" if apps is None else len(apps),
        "omitted" if tools is None else len(tools),
        changed,
        _effective_prune(sync_payload),
        time.perf_counter() - started_at,
    )
    return changed


def _merge_sync_results(
    *,
    base: dict[str, Any] | None,
    batch: dict[str, Any],
    app_count: int | None = None,
) -> dict[str, Any]:
    merged = dict(base or {})
    merged["status"] = batch.get("status") or merged.get("status") or "success"
    if app_count is not None:
        merged["apps_upserted"] = app_count
    else:
        merged["apps_upserted"] = int(merged.get("apps_upserted", 0) or 0) + int(
            batch.get("apps_upserted", 0) or 0,
        )
    for field in ("tools_upserted", "apps_pruned", "tools_pruned"):
        merged[field] = int(merged.get(field, 0) or 0) + int(batch.get(field, 0) or 0)
    merged["skipped_apps"] = [
        *(merged.get("skipped_apps") or []),
        *(batch.get("skipped_apps") or []),
    ]
    merged["apps"] = [
        *(merged.get("apps") or []),
        *(batch.get("apps") or []),
    ]
    merged["tools"] = [
        *(merged.get("tools") or []),
        *(batch.get("tools") or []),
    ]
    matched = {
        str(slug)
        for slug in [
            *(merged.get("matched_app_slugs") or []),
            *(batch.get("matched_app_slugs") or []),
        ]
        if slug
    }
    merged["matched_app_slugs"] = sorted(matched)
    for field in ("cache_version", "warning", "error"):
        if batch.get(field) is not None:
            merged[field] = batch[field]
    merged["auth_configs_created"] = int(
        merged.get("auth_configs_created", 0) or 0,
    ) + int(batch.get("auth_configs_created", 0) or 0)
    merged["auth_configs_reused"] = int(
        merged.get("auth_configs_reused", 0) or 0,
    ) + int(batch.get("auth_configs_reused", 0) or 0)
    return merged


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _should_batch_composio_full_sync(sync_payload: dict[str, Any]) -> bool:
    return (
        sync_payload.get("backend_id") == "composio"
        and sync_payload.get("sync_mode") == "full"
        and bool(sync_payload.get("include_all_managed_apps"))
        and bool(sync_payload.get("sync_tools", True))
    )


def _builtins_sync_request_payload(
    *,
    plan: ProviderBootstrapPlan,
    sync_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "backend_id": plan.backend_id,
        "environment": plan.environment,
        "desired_hash": plan.desired_hash,
        "desired_config": plan.desired_config,
        "cache_version": sync_payload.get("cache_version"),
        "mode": "all",
        "app_slugs": list(sync_payload.get("app_slugs") or []),
        "prune_unlisted_apps": _effective_prune(sync_payload),
        "sync_payload": sync_payload,
        "batch_size": int(
            os.environ.get(
                "UNITY_INTEGRATION_BOOTSTRAP_BATCH_SIZE",
                DEFAULT_COMPOSIO_BATCH_SIZE,
            ),
        ),
        "workers": int(os.environ.get("UNITY_INTEGRATION_BOOTSTRAP_WORKERS", "4")),
    }


def _parse_final_json(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("executor did not emit final JSON status")


def _run_json_command(
    command: list[str],
    *,
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        suffix=".json",
    ) as request_file:
        json.dump(request_payload, request_file)
        request_file.flush()
        completed = subprocess.run(
            [*command, "--request-file", request_file.name],
            check=False,
            capture_output=True,
            encoding="utf-8",
        )
    if completed.returncode != 0:
        try:
            result = _parse_final_json(completed.stdout)
        except Exception:
            result = {}
        error = (
            result.get("error") or completed.stderr.strip() or completed.stdout.strip()
        )
        raise RuntimeError(
            error or f"executor failed with exit code {completed.returncode}",
        )
    return _parse_final_json(completed.stdout)


def _run_direct_worker_executor(payload: dict[str, Any]) -> dict[str, Any]:
    command = os.environ.get(
        "UNITY_INTEGRATION_BOOTSTRAP_DIRECT_WORKER_CMD",
        f"{sys.executable} -m orchestra.workers.builtins_artifacts_seed_job",
    )
    return _run_json_command(shlex.split(command), request_payload=payload)


def _run_api_executor(
    *,
    base_url: str,
    admin_key: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    result = _admin_request(
        base_url=base_url,
        admin_key=admin_key,
        method="POST",
        path="/admin/integrations/builtins-sync/start",
        payload=payload,
        timeout=float(os.environ.get("UNITY_INTEGRATION_BOOTSTRAP_TIMEOUT", "3600")),
    )
    return result


def _sync_and_seed_provider(
    *,
    base_url: str,
    admin_key: str,
    plan: ProviderBootstrapPlan,
    sync_payload: dict[str, Any],
    executor: str = "api",
) -> tuple[bool, dict[str, Any]]:
    logging.info(
        "Starting Builtins integrations artifact seed backend=%s environment=%s mode=%s executor=%s",
        plan.backend_id,
        plan.environment,
        sync_payload.get("sync_mode"),
        executor,
    )
    payload = _builtins_sync_request_payload(plan=plan, sync_payload=sync_payload)
    if executor == "api":
        result = _run_api_executor(
            base_url=base_url,
            admin_key=admin_key,
            payload=payload,
        )
    elif executor == "direct_worker":
        result = _run_direct_worker_executor(payload)
    elif executor == "none":
        result = {
            "status": BOOTSTRAP_STATUS_SUCCESS,
            "executor": "none",
            "apps_upserted": 0,
            "tools_upserted": 0,
            "skipped_batches": 0,
            "completed_batches": 0,
        }
    else:
        raise ValueError(f"Unsupported integration bootstrap executor: {executor}")
    logging.info(
        "Completed Builtins integrations artifact seed backend=%s status=%s apps=%s tools=%s "
        "skipped_batches=%s completed_batches=%s",
        plan.backend_id,
        result.get("status"),
        result.get("apps_upserted", 0),
        result.get("tools_upserted", 0),
        result.get("skipped_batches", 0),
        result.get("completed_batches", 0),
    )
    if result.get("status") != BOOTSTRAP_STATUS_SUCCESS:
        raise RuntimeError(
            result.get("error") or "Builtins integrations artifact seed failed",
        )
    changed = bool(
        int(result.get("apps_upserted", 0) or 0)
        or int(result.get("tools_upserted", 0) or 0),
    )
    return changed, result


def _sync_integration_bootstrap_manifest(
    *,
    manifest_path: str,
    base_url: str,
    admin_key: str,
) -> bool:
    manifest = _load_manifest(manifest_path)
    logging.info(
        "Starting integration bootstrap manifest path=%s providers=%s",
        manifest_path,
        len(manifest["providers"]),
    )
    changed = False
    for backend_id, config in sorted(manifest["providers"].items()):
        if not isinstance(config, dict):
            raise ValueError(f"{backend_id}: provider config must be an object")
        plan = _provider_plan(
            manifest=manifest,
            backend_id=str(backend_id),
            config=config,
        )
        executor = _integration_bootstrap_executor(plan.environment)
        logging.info(
            "Checking integration bootstrap state backend=%s environment=%s desired_hash=%s executor=%s",
            plan.backend_id,
            plan.environment,
            plan.desired_hash,
            executor,
        )
        state = _bootstrap_state(
            base_url=base_url,
            admin_key=admin_key,
            environment=plan.environment,
            backend_id=plan.backend_id,
        )
        if _bootstrap_state_matches(state=state, plan=plan):
            logging.info(
                "Skipping integration bootstrap backend=%s; manifest hash already seeded",
                backend_id,
            )
            continue
        logging.info(
            "Registering integration backend backend=%s environment=%s",
            plan.backend_id,
            plan.environment,
        )
        _admin_request(
            base_url=base_url,
            admin_key=admin_key,
            method="POST",
            path="/admin/integrations/backends",
            payload=plan.backend_payload,
        )
        if plan.sync_payload is None:
            logging.info("Skipping integration sync for backend=%s", backend_id)
            _put_bootstrap_state(
                base_url=base_url,
                admin_key=admin_key,
                plan=plan,
                status=BOOTSTRAP_STATUS_SUCCESS,
                result={"status": BOOTSTRAP_STATUS_SUCCESS},
                builtins_changed=False,
            )
            continue
        try:
            logging.info(
                "Starting integration bootstrap sync backend=%s environment=%s",
                plan.backend_id,
                plan.environment,
            )
            provider_changed, result = _sync_and_seed_provider(
                base_url=base_url,
                admin_key=admin_key,
                plan=plan,
                sync_payload=plan.sync_payload,
                executor=executor,
            )
            logging.info(
                "Completed integration bootstrap sync backend=%s status=%s "
                "apps=%s tools=%s changed=%s",
                plan.backend_id,
                result.get("status"),
                result.get("apps_upserted", 0),
                result.get("tools_upserted", 0),
                provider_changed,
            )
            _put_bootstrap_state(
                base_url=base_url,
                admin_key=admin_key,
                plan=plan,
                status=result.get("status") or BOOTSTRAP_STATUS_SUCCESS,
                result=result,
                builtins_changed=provider_changed,
            )
            changed = provider_changed or changed
        except Exception as exc:
            logging.info(
                "Integration bootstrap sync failed backend=%s error=%s",
                plan.backend_id,
                str(exc)[:500],
            )
            _put_bootstrap_state(
                base_url=base_url,
                admin_key=admin_key,
                plan=plan,
                status="failed",
                error_message=str(exc),
            )
            raise
    return changed


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    from unity.common.builtins import builtins_project
    from unity.function_manager.builtins_catalog import seed_builtin_primitives
    from unity.guidance_manager.builtins_catalog import seed_builtin_guidance
    from unity.integrations.builtins_catalog import seed_builtin_integrations

    project = builtins_project()
    logging.info(
        "Starting Builtins catalogue seed project=%s integration_manifest=%s",
        project,
        bool(args.integration_bootstrap_manifest),
    )
    primitives_changed = seed_builtin_primitives()
    guidance_changed = seed_builtin_guidance()
    if args.skip_integrations:
        integrations_changed = False
    elif args.integration_bootstrap_manifest:
        base_url = os.environ.get("ORCHESTRA_URL", "").rstrip("/")
        if not base_url:
            raise ValueError("ORCHESTRA_URL is required for integration bootstrap")
        if not args.admin_key:
            raise ValueError(
                "ORCHESTRA_ADMIN_KEY or --admin-key is required for integration bootstrap",
            )
        integrations_changed = _sync_integration_bootstrap_manifest(
            manifest_path=args.integration_bootstrap_manifest,
            base_url=base_url,
            admin_key=args.admin_key,
        )
    else:
        integrations_changed = seed_builtin_integrations()
    for name, changed in (
        ("primitives", primitives_changed),
        ("guidance", guidance_changed),
        ("integrations", integrations_changed),
    ):
        state = "updated" if changed else "already up to date"
        print(f"Builtins {name} catalogue ({project}): {state}")
    if args.integration_bootstrap_manifest:
        print(
            "Integration manifest bootstrap "
            f"path={args.integration_bootstrap_manifest}",
        )
    if not (primitives_changed or guidance_changed or integrations_changed):
        logging.info(
            "Builtins catalogue seed found no changes project=%s; exiting successfully",
            project,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
