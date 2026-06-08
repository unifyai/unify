#!/usr/bin/env bash
# =============================================================================
# self_host_env.sh — Load self-host runtime env from unity/.env
# =============================================================================
#
# Sources BYOK keys and workspace OAuth credentials for Orchestra and Adapters.
# Microsoft workspace connect uses MICROSOFT_BYOD_CLIENT_ID in Orchestra and
# MS365_BYOD_* in Adapters — this helper mirrors the Orchestra name when needed.
#
set -euo pipefail

# Matches get_local_root() in unity/file_manager/settings.py (~/Unity/Local).
SELF_HOST_DEFAULT_WORKSPACE="${SELF_HOST_DEFAULT_WORKSPACE:-$HOME/Unity/Local}"

default_self_host_workspace() {
  printf '%s' "${UNITY_LOCAL_ROOT:-$SELF_HOST_DEFAULT_WORKSPACE}"
}

ensure_self_host_workspace_dir() {
  local workspace
  workspace="$(default_self_host_workspace)"
  mkdir -p "$workspace"
}

load_self_host_env_file() {
  local env_file="${1:-}"
  if [[ -z "$env_file" || ! -f "$env_file" ]]; then
    return 0
  fi
  # Parse KEY=VALUE lines only — never `source` the whole file, which breaks on
  # orphan values or duplicate keys that produce multiline upserts.
  local exports
  exports="$(python3 - "$env_file" <<'PYEOF'
import re
import shlex
import sys
from pathlib import Path

path = Path(sys.argv[1])
skip = {
    "UNIFY_KEY",
    "SHARED_UNIFY_KEY",
    "unify_key",
    "UNITY_COMMS_URL",
    "UNITY_ADAPTERS_URL",
}
key_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
seen: set[str] = set()
for raw in path.read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    if "=" not in line:
        continue
    key, _, val = line.partition("=")
    key = key.strip()
    if key in skip or not key_re.match(key) or key in seen:
        continue
    seen.add(key)
    val = val.strip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
        val = val[1:-1]
    print(f"export {shlex.quote(key)}={shlex.quote(val)}")
PYEOF
)"
  if [[ -n "$exports" ]]; then
    # shellcheck disable=SC1090
    eval "$exports"
  fi
}

export_workspace_oauth_env() {
  local env_file="${1:-${SELF_HOST_ENV_FILE:-${UNITY_ENV_FILE:-${UNITY_REPO:-}/.env}}}"
  load_self_host_env_file "$env_file"

  if [[ -n "${MICROSOFT_BYOD_CLIENT_ID:-}" && -z "${MS365_BYOD_CLIENT_ID:-}" ]]; then
    export MS365_BYOD_CLIENT_ID="$MICROSOFT_BYOD_CLIENT_ID"
  fi

  local key val
  for key in \
    GOOGLE_OAUTH_CLIENT_ID \
    GOOGLE_OAUTH_CLIENT_SECRET \
    OAUTH_STATE_SIGNING_KEY \
    MICROSOFT_BYOD_CLIENT_ID \
    MS365_BYOD_CLIENT_ID \
    MS365_BYOD_CLIENT_SECRET; do
    val="${!key:-}"
    if [[ -n "$val" ]]; then
      export "$key=$val"
    fi
  done
}

workspace_oauth_configured() {
  [[ -n "${GOOGLE_OAUTH_CLIENT_ID:-}" && -n "${OAUTH_STATE_SIGNING_KEY:-}" ]] \
    || [[ -n "${MICROSOFT_BYOD_CLIENT_ID:-}" && -n "${OAUTH_STATE_SIGNING_KEY:-}" ]] \
    || [[ -n "${MS365_BYOD_CLIENT_ID:-}" && -n "${OAUTH_STATE_SIGNING_KEY:-}" ]]
}

append_self_host_unity_runtime_env() {
  local -n _target_array="$1"
  local env_file="${2:-${SELF_HOST_ENV_FILE:-${UNITY_ENV_FILE:-${UNITY_REPO:-}/.env}}}"
  load_self_host_env_file "$env_file"

  local workspace
  workspace="$(default_self_host_workspace)"
  ensure_self_host_workspace_dir
  _target_array+=("UNITY_LOCAL_ROOT=$workspace")

  local key val
  for key in \
    UNITY_WEB_TAVILY_API_KEY \
    UNITY_WEB_ENABLED \
    UNITY_ACTOR_ANTICAPTCHA_KEY \
    ANTICAPTCHA_KEY \
    UNIFY_MODEL \
    OPENAI_API_KEY \
    ANTHROPIC_API_KEY \
    DEEPSEEK_API_KEY \
    DEEPGRAM_API_KEY \
    CARTESIA_API_KEY; do
    val="${!key:-}"
    if [[ -n "$val" ]]; then
      _target_array+=("$key=$val")
    fi
  done
}
