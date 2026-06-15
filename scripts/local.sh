#!/usr/bin/env bash
# =============================================================================
# local.sh — Local Unity for chat testing
# =============================================================================
#
# Starts either the full ConversationManager with Unity-owned local comms ingress
# (if LLM keys are available) or a lightweight echo responder for simple
# Pub/Sub plumbing checks.
#
# Designed to be called by Console's local.sh --chat, but can also be used
# standalone.
#
# Usage:
#   ./scripts/local.sh start              # Auto-detect mode (CM or echo)
#   ./scripts/local.sh start --echo       # Force echo responder
#   ./scripts/local.sh start --full       # Force full CM (fails without keys)
#   ./scripts/local.sh stop               # Stop Unity process
#   ./scripts/local.sh status             # Show status
#   ./scripts/local.sh gateway-doctor     # Run gateway config checks
#   ./scripts/local.sh check              # Quick check (returns 0 if running)
#
# Environment (all optional — sensible defaults for local testing):
#   UNITY_CONVERSATION_LOCAL_COMMS_ENABLED  Enable Unity-owned local comms ingress
#   UNITY_CONVERSATION_LOCAL_COMMS_MODE     local|hosted (default: local unless UNITY_COMMS_URL is set)
#   UNITY_CONVERSATION_LOCAL_COMMS_HOST     Local ingress bind host (default: 127.0.0.1)
#   UNITY_CONVERSATION_LOCAL_COMMS_PORT     Local ingress bind port (default: 8787)
#   UNITY_CONVERSATION_LOCAL_COMMS_PUBLIC_URL Public URL for external webhooks
#   UNITY_COMMS_URL         Hosted communication service URL (optional)
#   PUBSUB_EMULATOR_HOST    Pub/Sub emulator (echo mode only; default: localhost:8085)
#   GCP_PROJECT_ID          Project ID (echo mode / hosted comms only)
#   ASSISTANT_ID            Test assistant ID (default: default-test-assistant)
#   DEPLOY_ENV              Environment suffix (default: staging)
#   ORCHESTRA_URL           Orchestra URL (default: http://127.0.0.1:8000/v0)
#   ORCHESTRA_ADMIN_KEY     Admin key for Orchestra (optional)
#   OPENAI_API_KEY          Required for full CM mode
#   ANTHROPIC_API_KEY       Required for full CM mode
#   UNIFY_KEY               Required for full CM mode
#
set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
UNITY_REPO_PATH="$(cd "$SCRIPT_DIR/.." && pwd -P)"

if [[ -f "$UNITY_REPO_PATH/scripts/self_host_env.sh" ]]; then
  # shellcheck disable=SC1090
  source "$UNITY_REPO_PATH/scripts/self_host_env.sh"
fi

PUBSUB_EMULATOR_HOST_EXPLICIT="${PUBSUB_EMULATOR_HOST+x}"
PUBSUB_EMULATOR_HOST="${PUBSUB_EMULATOR_HOST:-localhost:8085}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-local-test-project}"
ASSISTANT_ID="${ASSISTANT_ID:-default-test-assistant}"
DEPLOY_ENV="${DEPLOY_ENV:-staging}"
ORCHESTRA_URL="${ORCHESTRA_URL:-http://127.0.0.1:8000/v0}"

if [[ -n "${UNITY_CONVERSATION_LOCAL_COMMS_MODE:-}" ]]; then
  LOCAL_COMMS_MODE="$UNITY_CONVERSATION_LOCAL_COMMS_MODE"
elif [[ -n "${UNITY_COMMS_URL:-}" ]]; then
  LOCAL_COMMS_MODE="hosted"
elif [[ -n "$PUBSUB_EMULATOR_HOST_EXPLICIT" ]]; then
  # Console --chat uses the Pub/Sub emulator end-to-end; local ingress outbox
  # mode disables the CM subscriber.
  LOCAL_COMMS_MODE="hosted"
else
  LOCAL_COMMS_MODE="local"
fi

if [[ -n "${UNITY_CONVERSATION_LOCAL_COMMS_ENABLED:-}" ]]; then
  LOCAL_COMMS_ENABLED="$UNITY_CONVERSATION_LOCAL_COMMS_ENABLED"
elif [[ "$LOCAL_COMMS_MODE" == "local" ]]; then
  LOCAL_COMMS_ENABLED="true"
else
  LOCAL_COMMS_ENABLED="false"
fi

# Self-host routes adapter traffic through unity.gateway, which delivers
# envelopes to the CM local ingress HTTP surface (8787 by default).
if [[ "${SELF_HOST:-0}" == "1" ]]; then
  LOCAL_COMMS_MODE="local"
  LOCAL_COMMS_ENABLED="true"
fi

LOCAL_COMMS_HOST="${UNITY_CONVERSATION_LOCAL_COMMS_HOST:-127.0.0.1}"
LOCAL_COMMS_PORT="${UNITY_CONVERSATION_LOCAL_COMMS_PORT:-8787}"
LOCAL_COMMS_PUBLIC_URL="${UNITY_CONVERSATION_LOCAL_COMMS_PUBLIC_URL:-}"
GATEWAY_HOST="${UNITY_GATEWAY_HOST:-127.0.0.1}"
GATEWAY_PORT="${UNITY_GATEWAY_PORT:-8001}"
GATEWAY_PUBLIC_URL="${UNITY_GATEWAY_PUBLIC_URL:-$LOCAL_COMMS_PUBLIC_URL}"

PIDFILE="/tmp/unity-local.pid"
LOGFILE="/tmp/unity-local.log"
MODEFILE="/tmp/unity-local.mode"
GATEWAY_PIDFILE="/tmp/unity-gateway.pid"
GATEWAY_LOGFILE="/tmp/unity-gateway.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }

# =============================================================================
# Prerequisite checks
# =============================================================================

has_llm_keys() {
  # Check if LLM API keys are available for full CM mode.
  # At minimum we need one LLM key and a Unify key.
  if [[ -z "${OPENAI_API_KEY:-}" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
    return 1
  fi
  if [[ -z "${UNIFY_KEY:-}" ]]; then
    return 1
  fi
  return 0
}

check_python() {
  local python_cmd
  python_cmd="$(get_python)"
  if ! $python_cmd --version &>/dev/null; then
    log_error "Python 3 is not available"
    return 1
  fi
  return 0
}

get_python() {
  # Prefer the venv python if available.
  if [[ -f "$UNITY_REPO_PATH/.venv/bin/python" ]]; then
    echo "$UNITY_REPO_PATH/.venv/bin/python"
  else
    echo "python3"
  fi
}

# =============================================================================
# Process management
# =============================================================================

is_running() {
  if [[ -f "$PIDFILE" ]]; then
    local pid
    pid=$(cat "$PIDFILE")
    if kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

get_mode() {
  cat "$MODEFILE" 2>/dev/null || echo "unknown"
}

local_comms_base_url() {
  if [[ -n "$LOCAL_COMMS_PUBLIC_URL" ]]; then
    echo "$LOCAL_COMMS_PUBLIC_URL"
  else
    echo "http://$LOCAL_COMMS_HOST:$LOCAL_COMMS_PORT"
  fi
}

gateway_base_url() {
  if [[ -n "$GATEWAY_PUBLIC_URL" ]]; then
    echo "$GATEWAY_PUBLIC_URL"
  else
    echo "http://$GATEWAY_HOST:$GATEWAY_PORT"
  fi
}

run_gateway_doctor() {
  cd "$UNITY_REPO_PATH"
  local python_cmd
  python_cmd="$(get_python)"
  local public_url_args=()
  [[ -n "$GATEWAY_PUBLIC_URL" ]] && public_url_args=(--public-url "$GATEWAY_PUBLIC_URL")
  "$python_cmd" -m unity.gateway doctor "${public_url_args[@]}"
}

describe_comms_backend() {
  if [[ "$LOCAL_COMMS_ENABLED" == "true" || "$LOCAL_COMMS_MODE" == "local" ]]; then
    echo "local gateway ($(gateway_base_url))"
  elif [[ -n "${UNITY_COMMS_URL:-}" ]]; then
    echo "hosted service ($UNITY_COMMS_URL)"
  else
    echo "simulated / no external comms"
  fi
}

is_gateway_running() {
  if [[ -f "$GATEWAY_PIDFILE" ]]; then
    local pid
    pid=$(cat "$GATEWAY_PIDFILE")
    if kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

start_gateway() {
  if is_gateway_running; then
    log_success "Unity gateway already running (PID $(cat "$GATEWAY_PIDFILE"))"
    if [[ -n "${UNITY_RUNTIME_GATEWAY_OWNER:-}" ]] \
      && declare -F self_host_write_gateway_state &>/dev/null; then
      self_host_write_gateway_state \
        "$UNITY_RUNTIME_GATEWAY_OWNER" \
        "$(cat "$GATEWAY_PIDFILE")"
    fi
    return 0
  fi

  cd "$UNITY_REPO_PATH"
  local python_cmd
  python_cmd="$(get_python)"
  local public_url_args=()
  [[ -n "$GATEWAY_PUBLIC_URL" ]] && public_url_args=(--public-url "$GATEWAY_PUBLIC_URL")

  env \
    ORCHESTRA_URL="$ORCHESTRA_URL" \
    ORCHESTRA_ADMIN_KEY="${ORCHESTRA_ADMIN_KEY:-}" \
    UNITY_COMMS_URL="$(gateway_base_url)" \
    UNITY_ADAPTERS_URL="$(gateway_base_url)" \
    UNITY_GATEWAY_LOCAL_INGRESS_URL="$(local_comms_base_url)" \
    "$python_cmd" -m unity.gateway serve \
      --host "$GATEWAY_HOST" \
      --port "$GATEWAY_PORT" \
      --mode all \
      --single-url \
      "${public_url_args[@]}" \
      > "$GATEWAY_LOGFILE" 2>&1 &

  local pid=$!
  echo "$pid" > "$GATEWAY_PIDFILE"
  sleep 2
  if ! kill -0 "$pid" 2>/dev/null; then
    log_error "Unity gateway failed to start. Check log: $GATEWAY_LOGFILE"
    tail -30 "$GATEWAY_LOGFILE" 2>/dev/null
    return 1
  fi
  if command -v curl >/dev/null 2>&1; then
    local attempt=0
    while (( attempt < 15 )); do
      if curl -sf "$(gateway_base_url)/health" >/dev/null 2>&1; then
        break
      fi
      sleep 1
      ((attempt++)) || true
    done
    if ! curl -sf "$(gateway_base_url)/health" >/dev/null 2>&1; then
      log_error "Unity gateway health check failed at $(gateway_base_url)/health"
      tail -30 "$GATEWAY_LOGFILE" 2>/dev/null
      return 1
    fi
  fi
  if [[ -n "${UNITY_RUNTIME_GATEWAY_OWNER:-}" ]] \
    && declare -F self_host_write_gateway_state &>/dev/null; then
    self_host_write_gateway_state "$UNITY_RUNTIME_GATEWAY_OWNER" "$pid"
  fi
  log_success "Unity gateway running (PID $pid)"
  log_info "Gateway URL: $(gateway_base_url)"
  log_info "Local ingress URL: $(local_comms_base_url)"
  if [[ -n "$GATEWAY_PUBLIC_URL" ]]; then
    log_info "Public callback URL: $GATEWAY_PUBLIC_URL"
  else
    log_warn "Public callback URL not set. Use UNITY_GATEWAY_PUBLIC_URL for provider webhooks."
  fi
}

stop_gateway() {
  if [[ "${UNITY_ALLOW_RUNTIME_STOP:-0}" != "1" ]] \
    && declare -F self_host_should_preserve_gateway_on_interactive_stop &>/dev/null \
    && self_host_should_preserve_gateway_on_interactive_stop; then
    log_info "Unity gateway is managed by unity service — not stopping"
    return 0
  fi

  if [[ -f "$GATEWAY_PIDFILE" ]]; then
    local gateway_pid
    gateway_pid=$(cat "$GATEWAY_PIDFILE")
    if kill -0 "$gateway_pid" 2>/dev/null; then
      log_info "Stopping Unity gateway (PID $gateway_pid)..."
      kill "$gateway_pid" 2>/dev/null || true
      sleep 2
      kill -9 "$gateway_pid" 2>/dev/null || true
    fi
    rm -f "$GATEWAY_PIDFILE"
  fi
  if declare -F self_host_patch_runtime_state &>/dev/null; then
    self_host_patch_runtime_state \
      "gateway_owner=" \
      "gateway_pid="
  fi
}

# =============================================================================
# Echo responder mode
# =============================================================================

start_echo() {
  log_info "Starting echo responder (Pub/Sub plumbing check) ..."

  cd "$UNITY_REPO_PATH"
  local python_cmd
  python_cmd="$(get_python)"

  env \
    PUBSUB_EMULATOR_HOST="$PUBSUB_EMULATOR_HOST" \
    GCP_PROJECT_ID="$GCP_PROJECT_ID" \
    $python_cmd scripts/echo_responder.py \
    > "$LOGFILE" 2>&1 &

  local pid=$!
  echo "$pid" > "$PIDFILE"
  echo "echo" > "$MODEFILE"

  # Wait briefly to make sure it didn't crash immediately.
  sleep 2
  if ! kill -0 "$pid" 2>/dev/null; then
    log_error "Echo responder failed to start. Check log: $LOGFILE"
    tail -20 "$LOGFILE" 2>/dev/null
    return 1
  fi

  log_success "Echo responder running (PID $pid)"
  log_info "Auto-discovers all unity-* topics and echoes messages back."
}

# =============================================================================
# Full ConversationManager mode
# =============================================================================

start_full_cm() {
  if [[ "${UNITY_RUNTIME_START_LOCK_HELD:-0}" == "1" ]]; then
    __start_full_cm_locked
    return $?
  fi
  if declare -F with_unity_runtime_start_lock &>/dev/null \
    && { [[ "${SELF_HOST:-0}" == "1" ]] || [[ -n "${UNITY_RUNTIME_OWNER:-}" ]]; }; then
    UNITY_RUNTIME_START_LOCK_HELD=1 with_unity_runtime_start_lock 30 bash "$0" __start_full_cm_locked
    return $?
  fi
  __start_full_cm_locked
}

__start_full_cm_locked() {
  if is_running; then
    local mode
    mode="$(get_mode)"
    log_success "Unity already running in $mode mode (PID $(cat "$PIDFILE"))"
    return 0
  fi
  __start_full_cm_impl
}

__start_full_cm_impl() {
  log_info "Starting ConversationManager for assistant=$ASSISTANT_ID ..."

  cd "$UNITY_REPO_PATH"
  local python_cmd
  python_cmd="$(get_python)"

  local env_suffix=""
  if [[ "$DEPLOY_ENV" != "production" ]]; then
    env_suffix="-$DEPLOY_ENV"
  fi

  local eventbus_publish="${EVENTBUS_PUBLISHING_ENABLED:-}"
  local eventbus_stream="${EVENTBUS_PUBSUB_STREAMING:-}"
  if [[ -z "$eventbus_publish" ]]; then
    if [[ "${SELF_HOST:-0}" == "1" ]]; then
      eventbus_publish="true"
      eventbus_stream="true"
    else
      eventbus_publish="false"
      eventbus_stream="false"
    fi
  elif [[ -z "$eventbus_stream" ]]; then
    eventbus_stream="$eventbus_publish"
  fi

  # Build env vars for the CM process.
  local env_vars=(
    "PUBSUB_EMULATOR_HOST=$PUBSUB_EMULATOR_HOST"
    "GCP_PROJECT_ID=$GCP_PROJECT_ID"
    "ASSISTANT_ID=$ASSISTANT_ID"
    "DEPLOY_ENV=$DEPLOY_ENV"
    "ORCHESTRA_URL=$ORCHESTRA_URL"
    "UNITY_CONVERSATION_LOCAL_COMMS_ENABLED=$LOCAL_COMMS_ENABLED"
    "UNITY_CONVERSATION_LOCAL_COMMS_MODE=$LOCAL_COMMS_MODE"
    "UNITY_CONVERSATION_LOCAL_COMMS_HOST=$LOCAL_COMMS_HOST"
    "UNITY_CONVERSATION_LOCAL_COMMS_PORT=$LOCAL_COMMS_PORT"
    "UNITY_COMMS_URL=${UNITY_COMMS_URL:-$(gateway_base_url)}"
    "UNITY_ADAPTERS_URL=${UNITY_ADAPTERS_URL:-$(gateway_base_url)}"
    "UNITY_VALIDATE_LLM_PROVIDERS=false"
    "EVENTBUS_PUBLISHING_ENABLED=$eventbus_publish"
    "EVENTBUS_PUBSUB_STREAMING=$eventbus_stream"
    "TEST=false"
    "UNITY_INACTIVITY_TIMEOUT_SECONDS=${UNITY_INACTIVITY_TIMEOUT_SECONDS:-0}"
  )
  [[ -n "${UNITY_LOCAL_ROOT:-}" ]] && env_vars+=("UNITY_LOCAL_ROOT=$UNITY_LOCAL_ROOT")
  [[ -n "${UNITY_LOCAL_SCHEDULER:-}" ]] && env_vars+=("UNITY_LOCAL_SCHEDULER=$UNITY_LOCAL_SCHEDULER")
  [[ -n "$LOCAL_COMMS_PUBLIC_URL" ]] && env_vars+=("UNITY_CONVERSATION_LOCAL_COMMS_PUBLIC_URL=$LOCAL_COMMS_PUBLIC_URL")

  # Forward API keys if present.
  [[ -n "${OPENAI_API_KEY:-}" ]]       && env_vars+=("OPENAI_API_KEY=$OPENAI_API_KEY")
  [[ -n "${ANTHROPIC_API_KEY:-}" ]]    && env_vars+=("ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY")
  [[ -n "${UNIFY_KEY:-}" ]]            && env_vars+=("UNIFY_KEY=$UNIFY_KEY")
  [[ -n "${ORCHESTRA_ADMIN_KEY:-}" ]]  && env_vars+=("ORCHESTRA_ADMIN_KEY=$ORCHESTRA_ADMIN_KEY")

  # Provide minimal identity defaults so SESSION_DETAILS populates.
  [[ -z "${ASSISTANT_FIRST_NAME:-}" ]] && env_vars+=("ASSISTANT_FIRST_NAME=Local")
  [[ -n "${ASSISTANT_FIRST_NAME:-}" ]] && env_vars+=("ASSISTANT_FIRST_NAME=$ASSISTANT_FIRST_NAME")
  [[ -z "${USER_FIRST_NAME:-}" ]]      && env_vars+=("USER_FIRST_NAME=User")
  [[ -n "${USER_FIRST_NAME:-}" ]]      && env_vars+=("USER_FIRST_NAME=$USER_FIRST_NAME")
  [[ -z "${USER_EMAIL:-}" ]]           && env_vars+=("USER_EMAIL=local@test.example.com")
  [[ -n "${USER_EMAIL:-}" ]]           && env_vars+=("USER_EMAIL=$USER_EMAIL")

  env "${env_vars[@]}" $python_cmd -m unity.conversation_manager.main \
    > "$LOGFILE" 2>&1 &

  local pid=$!
  echo "$pid" > "$PIDFILE"
  echo "full-cm" > "$MODEFILE"

  # Wait briefly for startup.
  sleep 3
  if ! kill -0 "$pid" 2>/dev/null; then
    log_error "ConversationManager failed to start. Check log: $LOGFILE"
    tail -30 "$LOGFILE" 2>/dev/null
    return 1
  fi

  log_success "ConversationManager running (PID $pid)"
  log_info "Full LLM-powered responses enabled."
  log_info "Comms backend: $(describe_comms_backend)"

  if [[ -n "${UNITY_RUNTIME_OWNER:-}" ]] \
    && declare -F self_host_write_runtime_state &>/dev/null; then
    self_host_write_runtime_state "$UNITY_RUNTIME_OWNER" "$pid" "$ASSISTANT_ID"
  fi
}

# =============================================================================
# Commands
# =============================================================================

cmd_start() {
  local force_mode=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --echo) force_mode="echo"; shift ;;
      --full) force_mode="full"; shift ;;
      *)      shift ;;
    esac
  done

  if is_running; then
    local mode
    mode="$(get_mode)"
    log_success "Unity already running in $mode mode (PID $(cat "$PIDFILE"))"
    return 0
  fi

  if ! check_python; then
    return 1
  fi

  echo "=============================================="
  echo "  Starting Local Unity"
  echo "=============================================="
  echo ""

  local mode="$force_mode"
  if [[ -z "$mode" ]]; then
    if has_llm_keys; then
      mode="full"
      log_info "LLM keys detected — starting full ConversationManager"
    else
      mode="echo"
      log_info "No LLM keys found — starting echo responder"
      log_info "Set OPENAI_API_KEY + UNIFY_KEY (or ANTHROPIC_API_KEY + UNIFY_KEY) for full CM mode."
    fi
  fi

  if [[ "$mode" == "echo" ]] && is_running; then
    mode="$(get_mode)"
    log_success "Unity already running in $mode mode (PID $(cat "$PIDFILE"))"
    return 0
  fi

  if [[ "$mode" == "full" ]]; then
    if ! has_llm_keys; then
      log_warn "LLM keys not found — full CM mode may fail at LLM calls."
    fi
    start_gateway
    start_full_cm
  else
    start_echo
  fi

  echo ""
  echo "  Mode:       $mode"
  echo "  Assistant:  $ASSISTANT_ID"
  if [[ "$mode" == "echo" ]]; then
    echo "  Echo bus:   $PUBSUB_EMULATOR_HOST"
  else
    echo "  Comms:      $(describe_comms_backend)"
    echo "  Gateway:    $(gateway_base_url)"
    echo "  Ingress:    $(local_comms_base_url)"
    if [[ -n "$GATEWAY_PUBLIC_URL" ]]; then
      echo "  Callbacks:  $GATEWAY_PUBLIC_URL"
    else
      echo "  Callbacks:  not set (export UNITY_GATEWAY_PUBLIC_URL)"
    fi
  fi
  echo "  Log:        $LOGFILE"
  echo ""
}

cmd_stop() {
  if [[ "${UNITY_ALLOW_RUNTIME_STOP:-0}" != "1" ]] \
    && declare -F self_host_should_preserve_runtime_on_interactive_stop &>/dev/null \
    && self_host_should_preserve_runtime_on_interactive_stop; then
    log_info "Coordinator runtime is managed by unity service — not stopping"
    return 0
  fi

  if [[ -f "$PIDFILE" ]]; then
    local pid
    pid=$(cat "$PIDFILE")
    if kill -0 "$pid" 2>/dev/null; then
      local mode
      mode="$(get_mode)"
      log_info "Stopping Unity $mode (PID $pid)..."
      kill "$pid" 2>/dev/null || true
      sleep 2
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PIDFILE" "$MODEFILE"
  fi
  if declare -F self_host_clear_runtime_state &>/dev/null; then
    self_host_clear_runtime_state
  fi
  stop_gateway
  log_success "Unity stopped"
}

cmd_status() {
  echo ""
  echo "Unity Local Status"
  echo "=================="
  echo ""
  echo -n "  Status:    "
  if is_running; then
    local mode
    mode="$(get_mode)"
    echo -e "${GREEN}running${NC} ($mode mode, PID $(cat "$PIDFILE"))"
  else
    echo -e "${RED}not running${NC}"
  fi
  echo "  Assistant: $ASSISTANT_ID"
  if [[ "$(get_mode)" == "echo" ]]; then
    echo "  Echo bus:  $PUBSUB_EMULATOR_HOST"
    echo "  Project:   $GCP_PROJECT_ID"
  else
    echo "  Comms:     $(describe_comms_backend)"
    if is_gateway_running; then
      echo "  Gateway:   running ($(gateway_base_url), PID $(cat "$GATEWAY_PIDFILE"))"
    else
      echo "  Gateway:   not running ($(gateway_base_url))"
    fi
    echo "  Ingress:   $(local_comms_base_url)"
    if [[ -n "$GATEWAY_PUBLIC_URL" ]]; then
      echo "  Callbacks: $GATEWAY_PUBLIC_URL"
    else
      echo "  Callbacks: not set (export UNITY_GATEWAY_PUBLIC_URL)"
    fi
    echo ""
    echo "Gateway Doctor"
    echo "--------------"
    run_gateway_doctor || true
  fi
  echo "  Log:       $LOGFILE"
  echo ""
}

cmd_check() {
  is_running
}

cmd_gateway_doctor() {
  run_gateway_doctor
}

cmd_start_gateway() {
  if ! check_python; then
    return 1
  fi
  start_gateway
}

cmd_stop_gateway() {
  stop_gateway
}

cmd_gateway_url() {
  gateway_base_url
}

cmd_help() {
  echo "Usage: $0 [command] [options]"
  echo ""
  echo "Commands:"
  echo "  start     Start Unity locally (auto-detects mode)"
  echo "  stop      Stop Unity"
  echo "  status    Show status"
  echo "  gateway-doctor"
  echo "            Run gateway config checks"
  echo "  start-gateway"
  echo "            Start unity.gateway (outbound + inbound HTTP)"
  echo "  stop-gateway"
  echo "            Stop unity.gateway"
  echo "  gateway-url"
  echo "            Print the local gateway base URL"
  echo "  check     Quick check (exit 0 if running)"
  echo ""
  echo "Start Options:"
  echo "  --echo    Force echo responder (no API keys needed)"
  echo "  --full    Force full ConversationManager (requires LLM keys)"
  echo ""
  echo "Modes:"
  echo "  echo      Echoes messages back via Pub/Sub (plumbing check, no LLM)"
  echo "  full-cm   Full ConversationManager with LLM responses"
  echo ""
  echo "Auto-detection: if OPENAI_API_KEY or ANTHROPIC_API_KEY and UNIFY_KEY"
  echo "are set, starts full CM. Otherwise falls back to echo responder."
  echo ""
  echo "Environment:"
  echo "  UNITY_CONVERSATION_LOCAL_COMMS_ENABLED  Enable local comms ingress"
  echo "  UNITY_CONVERSATION_LOCAL_COMMS_MODE     local|hosted"
  echo "  UNITY_CONVERSATION_LOCAL_COMMS_HOST     Local ingress bind host"
  echo "  UNITY_CONVERSATION_LOCAL_COMMS_PORT     Local ingress bind port"
  echo "  UNITY_GATEWAY_PUBLIC_URL                Public HTTPS callback URL"
  echo "  UNITY_COMMS_URL                         Hosted comms service URL"
  echo "  PUBSUB_EMULATOR_HOST                    Pub/Sub host for echo mode"
  echo "  GCP_PROJECT_ID                          Project ID for echo mode"
  echo "  ASSISTANT_ID            Assistant ID (default: default-test-assistant)"
  echo "  DEPLOY_ENV              Deploy env for topic suffix (default: staging)"
  echo "  OPENAI_API_KEY          OpenAI key (for full CM mode)"
  echo "  ANTHROPIC_API_KEY       Anthropic key (for full CM mode)"
  echo "  UNIFY_KEY               Unify key (for full CM mode)"
  echo ""
}

# =============================================================================
# Entry Point
# =============================================================================

main() {
  local cmd="${1:-help}"
  shift || true

  case "$cmd" in
    start)   cmd_start "$@" ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    gateway-doctor) cmd_gateway_doctor ;;
    start-gateway) cmd_start_gateway ;;
    stop-gateway) cmd_stop_gateway ;;
    gateway-url) cmd_gateway_url ;;
    check)   cmd_check ;;
    __start_full_cm_locked) __start_full_cm_locked ;;
    help|--help|-h) cmd_help ;;
    *)
      log_error "Unknown command: $cmd"
      echo "Run '$0 help' for usage"
      exit 1
      ;;
  esac
}

main "$@"
