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

load_self_host_env_file() {
  local env_file="${1:-}"
  if [[ -z "$env_file" ]]; then
    return 0
  fi
  if [[ ! -f "$env_file" ]]; then
    return 0
  fi
  # shellcheck disable=SC1090
  set -a
  source "$env_file"
  set +a
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
