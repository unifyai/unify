#!/usr/bin/env bash
# =============================================================================
# stack.sh — Self-host stack
# =============================================================================
#
# Brings up Orchestra, unity.gateway, Pub/Sub emulator, Console, and
# the Unity CM for the signed-in user's Coordinator when credentials exist.
#
# Usage:
#   ./scripts/stack.sh up           Start full stack (+ Coordinator if registered)
#   ./scripts/stack.sh down [--full]    Stop stack (--full stops background runtime too)
#   ./scripts/stack.sh status       Show service status
#   ./scripts/stack.sh doctor       Check prerequisites
#
# Environment:
#   UNIFY_STACK_ROOT          Parent dir with orchestra/console/unity siblings
#   OPENAI_API_KEY / ANTHROPIC_API_KEY  Required for Coordinator chat
#   DEEPGRAM_API_KEY / CARTESIA_API_KEY Required for browser calls (prompted by unity setup)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
UNITY_REPO_PATH="$(cd "$SCRIPT_DIR/.." && pwd -P)"
ENSURE_PREREQS_SCRIPT="$UNITY_REPO_PATH/scripts/ensure_prereqs.sh"
SELF_HOST_ENV_SCRIPT="$UNITY_REPO_PATH/scripts/self_host_env.sh"

UNIFY_STACK_ROOT="${UNIFY_STACK_ROOT:-$(cd "$UNITY_REPO_PATH/.." && pwd -P)}"
CONSOLE_REPO_PATH="${CONSOLE_REPO_PATH:-$UNIFY_STACK_ROOT/console}"
ORCHESTRA_REPO_PATH="${ORCHESTRA_REPO_PATH:-$UNIFY_STACK_ROOT/orchestra}"

CONSOLE_LOCAL_SCRIPT="$CONSOLE_REPO_PATH/scripts/local.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }

require_repo() {
  local label="$1"
  local path="$2"
  if [[ ! -d "$path" ]]; then
    log_error "$label repo not found at: $path"
    log_info "Clone sibling repos under UNIFY_STACK_ROOT or set ${label}_REPO_PATH"
    return 1
  fi
}

_has_env_key() {
  local key="$1"
  local env_file="$UNITY_REPO_PATH/.env"
  [[ -n "${!key:-}" ]] && return 0
  [[ -f "$env_file" ]] && grep -qE "^${key}=.+$" "$env_file"
}

cmd_doctor() {
  local ok=true
  echo ""
  echo "Self-host doctor"
  echo "================"
  echo ""
  echo "Stranger path: curl install → unity setup → unity → register → chat"
  echo ""

  echo "Infrastructure"
  echo "--------------"

  if ! command -v docker &>/dev/null; then
    log_error "Docker is not installed"
    ok=false
  elif ! docker info &>/dev/null; then
    log_error "Docker daemon is not running"
    ok=false
  else
    log_success "Docker is available"
  fi

  if [[ -f "$ENSURE_PREREQS_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$ENSURE_PREREQS_SCRIPT"
    if ! ensure_node; then
      ok=false
    else
      log_success "Node.js/npm ready"
    fi
    if ! ensure_java; then
      ok=false
    else
      log_success "Java JRE ready"
    fi
    if ! ensure_pubsub_emulator; then
      ok=false
    else
      log_success "Pub/Sub emulator ready"
    fi
    if [[ "${SELF_HOST_DESKTOP:-0}" == "1" ]]; then
      if ! ensure_rclone; then
        ok=false
      else
        log_success "rclone ready (desktop file sync)"
      fi
    fi
  else
    log_warn "ensure_prereqs.sh missing — checking Java/gcloud manually"
    if ! command -v gcloud &>/dev/null; then
      ok=false
      log_error "gcloud CLI not found"
    fi
    if ! command -v java &>/dev/null || ! java -version &>/dev/null 2>&1; then
      ok=false
      log_error "Java JRE required for Pub/Sub emulator"
    fi
  fi

  echo ""
  echo "Sibling repos"
  echo "-------------"
  require_repo "Console" "$CONSOLE_REPO_PATH" || ok=false
  require_repo "Orchestra" "$ORCHESTRA_REPO_PATH" || ok=false

  if [[ -f "$UNITY_REPO_PATH/.venv/bin/python" ]]; then
    local unity_py="$UNITY_REPO_PATH/.venv/bin/python"
    if "$unity_py" -c "import unity.gateway" &>/dev/null; then
      log_success "Unity venv + unity.gateway OK"
    else
      log_error "unity.gateway not importable — run: cd $UNITY_REPO_PATH && uv sync"
      ok=false
    fi
  else
    log_warn "Unity .venv missing — run: cd $UNITY_REPO_PATH && uv sync"
    ok=false
  fi

  if [[ -f "$CONSOLE_REPO_PATH/.env.local" ]]; then
    log_success "console/.env.local found"
  else
    log_warn "console/.env.local missing — copy from .env.development and set JWT_SECRET"
    ok=false
  fi

  echo ""
  echo "BYOK keys (unity/.env)"
  echo "----------------------"
  echo "  Required: LLM (OpenAI or Anthropic)"
  echo "  Voice:    Deepgram + Cartesia (browser calls; LiveKit auto-configured on stack up)"
  echo "  Optional: Google / Microsoft OAuth (workspace connect)"
  echo "  Optional: Tavily (web search), AntiCaptcha (computer use)"
  echo ""

  if _has_env_key OPENAI_API_KEY || _has_env_key ANTHROPIC_API_KEY; then
    log_success "LLM provider key configured"
  else
    log_error "No LLM API key — run: unity setup (or scripts/prompt_byok_keys.sh)"
    ok=false
  fi

  if _has_env_key DEEPGRAM_API_KEY; then
    log_success "DEEPGRAM_API_KEY set"
  else
    log_warn "DEEPGRAM_API_KEY missing — browser calls need STT"
  fi

  if _has_env_key CARTESIA_API_KEY; then
    log_success "CARTESIA_API_KEY set"
  else
    log_warn "CARTESIA_API_KEY missing — browser calls need TTS"
  fi

  if _has_env_key OAUTH_STATE_SIGNING_KEY \
    && { _has_env_key GOOGLE_OAUTH_CLIENT_ID || _has_env_key MICROSOFT_BYOD_CLIENT_ID; }; then
    if _has_env_key GOOGLE_OAUTH_CLIENT_ID && _has_env_key GOOGLE_OAUTH_CLIENT_SECRET; then
      log_success "Google workspace OAuth configured"
    elif _has_env_key MICROSOFT_BYOD_CLIENT_ID && _has_env_key MS365_BYOD_CLIENT_SECRET; then
      log_success "Microsoft workspace OAuth configured"
    else
      log_warn "Workspace OAuth partially configured — finish client id + secret in unity/.env"
    fi
  else
    log_info "Workspace OAuth not configured (optional — onboarding workspace connect disabled)"
  fi

  if _has_env_key UNITY_WEB_TAVILY_API_KEY; then
    log_success "UNITY_WEB_TAVILY_API_KEY set (web search)"
  else
    log_info "Web search not configured (optional — Tavily via prompt_byok_keys.sh)"
  fi

  if _has_env_key ANTICAPTCHA_KEY || _has_env_key UNITY_ACTOR_ANTICAPTCHA_KEY; then
    log_success "AntiCaptcha key set (computer automation)"
  else
    log_info "AntiCaptcha not configured (optional — computer use / CAPTCHA solving)"
  fi

  echo ""
  echo "Runtime"
  echo "-------"
  log_info "FileManager workspace: ${UNITY_LOCAL_ROOT:-$HOME/Unity/Local}"
  log_info "Scheduled tasks: LocalActivationScheduler in Coordinator CM"
  if [[ -f "$SELF_HOST_ENV_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$SELF_HOST_ENV_SCRIPT"
    self_host_runtime_doctor_line | sed 's/^/  /'
    echo ""
    log_info "Daily driver: unity stack up / unity stack down"
    log_info "Stop everything: unity stack down --full  (or: unity service disable)"
    log_info "Survive reboot without Console: unity setup --boot-runtime"
  else
    log_info "Stack must stay up for scheduled tasks until self-host runtime is wired"
  fi
  log_info "Live Actions stream via EventBus → Pub/Sub actions-sub"

  echo ""
  if [[ "$ok" == "true" ]]; then
    log_success "Doctor passed — run: unity stack up"
    return 0
  fi
  log_error "Doctor found blockers — fix above, then re-run: unity stack doctor"
  return 1
}

cmd_up() {
  echo ""
  echo "=============================================="
  echo "  Starting self-host stack"
  echo "=============================================="
  echo ""

  if ! cmd_doctor; then
    log_error "Fix doctor findings before running stack up"
    return 1
  fi

  if [[ -x "$UNITY_REPO_PATH/scripts/voice.sh" ]]; then
    log_info "Ensuring local LiveKit + voice BYOK keys..."
    UNITY_HOME="${UNITY_HOME:-$HOME/.unity}" \
      UNITY_REPO="${UNITY_REPO:-$UNITY_REPO_PATH}" \
      bash "$UNITY_REPO_PATH/scripts/voice.sh" setup || log_warn "LiveKit setup failed — meet may not work"
  fi

  if [[ ! -f "$CONSOLE_LOCAL_SCRIPT" ]]; then
    log_error "Missing $CONSOLE_LOCAL_SCRIPT"
    return 1
  fi

  export SELF_HOST=1
  export ORCHESTRA_REPO_PATH
  export UNITY_REPO_PATH
  export CONSOLE_REPO_PATH
  export UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
  export SELF_HOST_STATE_DIR="${SELF_HOST_STATE_DIR:-$UNITY_HOME}"

  if [[ -f "$SELF_HOST_ENV_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$SELF_HOST_ENV_SCRIPT"
    export_self_host_coordinator_runtime_file
    export_workspace_oauth_env "$UNITY_REPO_PATH/.env"
    if declare -F self_host_enable_runtime &>/dev/null; then
      self_host_enable_runtime
    fi
  fi

  # voice.sh runs a local LiveKit server with dev credentials. unity/.env
  # often also contains cloud LiveKit keys that override the dev pair when
  # sourced, which breaks browser meet token minting in Console.
  export LIVEKIT_URL="ws://localhost:7880"
  export LIVEKIT_API_KEY="devkey"  # pragma: allowlist secret
  export LIVEKIT_API_SECRET="secret"  # pragma: allowlist secret

  if declare -F self_host_ensure_service_supervisor &>/dev/null \
    && [[ -f "$UNITY_REPO_PATH/scripts/service.sh" ]]; then
    log_info "Ensuring background runtime (scheduled tasks while stack is down)..."
    if ! self_host_ensure_service_supervisor "$UNITY_REPO_PATH/scripts/service.sh"; then
      log_warn "Background runtime failed to start — stack down will stop scheduled tasks"
    fi
  fi

  if ! bash "$CONSOLE_LOCAL_SCRIPT" start --self-host; then
    log_error "Self-host stack failed to start"
    return 1
  fi

  local runtime_file="${SELF_HOST_COORDINATOR_RUNTIME_FILE:-}"

  if [[ -f "$runtime_file" ]]; then
    local cm_count="0"
    if declare -F unity_cm_instance_count &>/dev/null; then
      cm_count="$(unity_cm_instance_count)"
    fi
    if [[ "$cm_count" -eq 1 ]]; then
      if declare -F self_host_adopt_coordinator_for_service &>/dev/null; then
        self_host_adopt_coordinator_for_service "${SELF_HOST_COORDINATOR_AGENT_ID:-}" || true
      fi
      if ! bash "$CONSOLE_LOCAL_SCRIPT" ensure-coordinator-topics; then
        log_warn "Coordinator Pub/Sub setup failed — sign in at Console to refresh credentials"
      fi
      log_success "Reusing Coordinator runtime"
    elif [[ "$cm_count" -gt 1 ]]; then
      log_error "Multiple Coordinator runtimes detected — run: unity stack down --full"
    else
      log_info "Starting Coordinator runtime (saved login)..."
      if ! bash "$CONSOLE_LOCAL_SCRIPT" ensure-coordinator-topics; then
        log_warn "Coordinator Pub/Sub setup failed — sign in at Console to refresh credentials"
      elif ! bash "$CONSOLE_LOCAL_SCRIPT" start-coordinator; then
        log_warn "Coordinator start failed — sign in at Console to refresh credentials"
      else
        log_success "Coordinator runtime is ready"
      fi
    fi
  fi

  local console_port="${CONSOLE_PORT:-3000}"
  echo ""
  echo "=============================================="
  log_success "Self-host stack is ready"
  echo "=============================================="
  echo ""
  echo "  Console:   http://localhost:${console_port}"
  echo ""
  if [[ -f "$runtime_file" ]]; then
    echo "  Open Console and chat with your Coordinator."
    if declare -F self_host_headless_scheduling_ready &>/dev/null \
      && self_host_headless_scheduling_ready; then
      echo "  stack down stops the UI only — scheduled tasks keep running in the background."
    elif declare -F self_host_service_is_enabled &>/dev/null \
      && self_host_service_is_enabled; then
      echo "  Background runtime is not healthy — stack down stops scheduled tasks."
      echo "  Re-run: unity stack up"
    fi
  else
    echo "  First visit: create an account on /login — Coordinator starts automatically."
  fi
  echo ""
}

cmd_down() {
  local full_stop="false"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full) full_stop="true"; shift ;;
      -h|--help)
        echo "Usage: unity stack down [--full]"
        echo ""
        echo "  Default: stop Console and stack ingress; keep Coordinator + Orchestra for scheduled tasks."
        echo "  --full:  stop everything, including background runtime."
        echo ""
        echo "  Also: unity service disable  (same as --full for background runtime)"
        return 0
        ;;
      *)
        log_error "Unknown option: $1"
        echo "Run: unity stack down --help"
        return 1
        ;;
    esac
  done

  if [[ ! -f "$CONSOLE_LOCAL_SCRIPT" ]]; then
    log_error "Missing $CONSOLE_LOCAL_SCRIPT"
    return 1
  fi

  export UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
  export SELF_HOST_STATE_DIR="${SELF_HOST_STATE_DIR:-$UNITY_HOME}"

  if [[ -f "$SELF_HOST_ENV_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$SELF_HOST_ENV_SCRIPT"
  fi

  if [[ "$full_stop" == "true" ]]; then
    if [[ -x "$UNITY_REPO_PATH/scripts/self_host_desktop.sh" ]]; then
      bash "$UNITY_REPO_PATH/scripts/self_host_desktop.sh" stop || true
    fi
    bash "$CONSOLE_LOCAL_SCRIPT" stop
    if [[ -x "$UNITY_REPO_PATH/scripts/service.sh" ]]; then
      bash "$UNITY_REPO_PATH/scripts/service.sh" stop || true
    fi
    log_success "Self-host stack and background runtime stopped"
    return 0
  fi

  if declare -F self_host_ensure_service_supervisor &>/dev/null \
    && [[ -f "$UNITY_REPO_PATH/scripts/service.sh" ]]; then
    self_host_ensure_service_supervisor "$UNITY_REPO_PATH/scripts/service.sh" || true
  fi

  if declare -F self_host_headless_scheduling_ready &>/dev/null \
    && self_host_headless_scheduling_ready; then
    SELF_HOST=1 bash "$CONSOLE_LOCAL_SCRIPT" stop --interactive-only
  else
    bash "$CONSOLE_LOCAL_SCRIPT" stop
  fi
  log_success "Self-host stack stopped"
}

cmd_status() {
  if [[ -f "$SELF_HOST_ENV_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$SELF_HOST_ENV_SCRIPT"
  fi

  if [[ -f "$CONSOLE_LOCAL_SCRIPT" ]]; then
    bash "$CONSOLE_LOCAL_SCRIPT" status
  else
    log_error "Console local script not found"
    return 1
  fi

  if [[ -x "$UNITY_REPO_PATH/scripts/service.sh" ]]; then
    echo ""
    bash "$UNITY_REPO_PATH/scripts/service.sh" status
  fi
}

main() {
  local cmd="${1:-up}"
  shift || true
  case "$cmd" in
    up) cmd_up "$@" ;;
    down|stop) cmd_down "$@" ;;
    status) cmd_status "$@" ;;
    doctor|check) cmd_doctor "$@" ;;
    help|-h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      ;;
    *)
      log_error "Unknown command: $cmd"
      echo "Run: $0 help"
      return 1
      ;;
  esac
}

main "$@"
