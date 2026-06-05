#!/usr/bin/env bash
# =============================================================================
# stack.sh — Self-host stack
# =============================================================================
#
# Brings up Orchestra, Comms App, Pub/Sub emulator, Adapters, Console, and
# Unity CM for the bootstrapped personal Coordinator.
#
# Usage:
#   ./scripts/stack.sh up       Start the full self-host stack
#   ./scripts/stack.sh down     Stop all services
#   ./scripts/stack.sh status   Show service status
#   ./scripts/stack.sh doctor   Check prerequisites
#
# Environment:
#   UNIFY_STACK_ROOT          Parent dir with orchestra/console/communication siblings
#   SELF_HOST_OWNER_PASSWORD  Pin the bootstrap owner password (optional)
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
COMMUNICATION_REPO_PATH="${COMMUNICATION_REPO_PATH:-$UNIFY_STACK_ROOT/communication}"

CONSOLE_LOCAL_SCRIPT="$CONSOLE_REPO_PATH/scripts/local.sh"
CREDENTIALS_FILE="${SELF_HOST_CREDENTIALS_FILE:-$HOME/.unity/self-host-credentials.json}"

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
  echo "Stranger path: unity setup → unity stack doctor → unity stack up"
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

  if ! command -v node &>/dev/null || ! command -v npm &>/dev/null; then
    log_error "Node.js 20+ and npm are required for Console"
    ok=false
  else
    log_success "Node.js/npm found"
  fi

  if [[ -f "$ENSURE_PREREQS_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$ENSURE_PREREQS_SCRIPT"
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
  require_repo "Communication" "$COMMUNICATION_REPO_PATH" || ok=false
  if [[ -f "$COMMUNICATION_REPO_PATH/.venv/bin/python" ]]; then
    if "$COMMUNICATION_REPO_PATH/.venv/bin/python" -c "from unity.task_scheduler.offline_runner_contract import build_offline_run_key" &>/dev/null; then
      log_success "Comms App unity import OK"
    else
      log_warn "Comms venv missing unity — auto-installs on stack up"
    fi
  else
    log_warn "communication/.venv missing — run: cd $COMMUNICATION_REPO_PATH && uv sync"
    ok=false
  fi

  if [[ -f "$UNITY_REPO_PATH/.venv/bin/python" ]]; then
    log_success "Unity venv found"
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
  export COMMUNICATION_REPO_PATH
  export UNITY_REPO_PATH
  export CONSOLE_REPO_PATH

  if [[ -f "$SELF_HOST_ENV_SCRIPT" ]]; then
    # shellcheck disable=SC1090
    source "$SELF_HOST_ENV_SCRIPT"
    export_workspace_oauth_env "$UNITY_REPO_PATH/.env"
  elif [[ -f "$UNITY_REPO_PATH/.env" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$UNITY_REPO_PATH/.env"
    set +a
  fi

  # voice.sh runs a local LiveKit server with dev credentials. unity/.env
  # often also contains cloud LiveKit keys that override the dev pair when
  # sourced, which breaks browser meet token minting in Console.
  export LIVEKIT_URL="ws://localhost:7880"
  export LIVEKIT_API_KEY="devkey"  # pragma: allowlist secret
  export LIVEKIT_API_SECRET="secret"  # pragma: allowlist secret

  if ! bash "$CONSOLE_LOCAL_SCRIPT" start --self-host; then
    log_error "Self-host stack failed to start"
    return 1
  fi

  mkdir -p "$(dirname "$CREDENTIALS_FILE")"
  if [[ -f /tmp/self-host-bootstrap.json ]]; then
    cp /tmp/self-host-bootstrap.json "$CREDENTIALS_FILE"
  fi

  echo ""
  echo "=============================================="
  log_success "Self-host stack is ready"
  echo "=============================================="
  echo ""
  if [[ -f "$CREDENTIALS_FILE" ]]; then
    local email password console_port
    email="$(python3 -c "import json; print(json.load(open('$CREDENTIALS_FILE'))['email'])" 2>/dev/null || echo "owner@selfhost.dev")"
    password="$(python3 -c "import json; print(json.load(open('$CREDENTIALS_FILE'))['password'])" 2>/dev/null || echo "<see bootstrap output>")"
    console_port="${CONSOLE_PORT:-3000}"
    echo "  Console:   http://localhost:${console_port}"
    echo "  Email:     $email"
    echo "  Password:  $password"
    echo ""
    echo "  Credentials saved to: $CREDENTIALS_FILE"
  else
    log_warn "Bootstrap credentials file not found — check console local.sh output"
  fi
  echo ""
  echo "  Sign in on the Login tab, then chat with the Coordinator."
  echo ""
}

cmd_down() {
  if [[ -f "$CONSOLE_LOCAL_SCRIPT" ]]; then
    bash "$CONSOLE_LOCAL_SCRIPT" stop
  fi
  log_success "Self-host stack stopped"
}

cmd_status() {
  if [[ -f "$CONSOLE_LOCAL_SCRIPT" ]]; then
    bash "$CONSOLE_LOCAL_SCRIPT" status
  else
    log_error "Console local script not found"
    return 1
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
