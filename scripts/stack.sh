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

cmd_doctor() {
  local ok=true
  echo ""
  echo "Self-host doctor"
  echo "================"
  echo ""

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

  if ! command -v gcloud &>/dev/null; then
    log_warn "gcloud not found — Pub/Sub emulator requires gcloud beta emulators pubsub"
    ok=false
  else
    log_success "gcloud found"
  fi

  if ! command -v java &>/dev/null || ! java -version &>/dev/null; then
    log_error "Java JRE required for Pub/Sub emulator (macOS stub java is not enough)"
    log_info "Fix: brew install openjdk && export PATH=\"/opt/homebrew/opt/openjdk/bin:\$PATH\""
    ok=false
  else
    log_success "Java JRE found"
  fi

  require_repo "Console" "$CONSOLE_REPO_PATH" || ok=false
  require_repo "Orchestra" "$ORCHESTRA_REPO_PATH" || ok=false
  require_repo "Communication" "$COMMUNICATION_REPO_PATH" || ok=false
  if [[ -f "$COMMUNICATION_REPO_PATH/.venv/bin/python" ]]; then
    if "$COMMUNICATION_REPO_PATH/.venv/bin/python" -c "from unity.task_scheduler.offline_runner_contract import build_offline_run_key" &>/dev/null; then
      log_success "Comms App unity import OK"
    else
      log_warn "Comms venv missing unity — will auto-install on stack up (needs ../unity sibling)"
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

  if [[ -z "${OPENAI_API_KEY:-}" && -z "${ANTHROPIC_API_KEY:-}" ]]; then
    if [[ -f "$UNITY_REPO_PATH/.env" ]] && grep -qE '^(OPENAI_API_KEY|ANTHROPIC_API_KEY)=' "$UNITY_REPO_PATH/.env" 2>/dev/null; then
      log_success "LLM key present in unity/.env"
    else
      log_warn "No LLM API key — run: UNITY_REPO=$UNITY_REPO_PATH $UNITY_REPO_PATH/scripts/prompt_byok_keys.sh"
    fi
  else
    log_success "LLM API key present in environment"
  fi

  if [[ -f "$UNITY_REPO_PATH/.env" ]]; then
    if grep -qE '^DEEPGRAM_API_KEY=.+' "$UNITY_REPO_PATH/.env" 2>/dev/null; then
      log_success "DEEPGRAM_API_KEY set"
    else
      log_warn "DEEPGRAM_API_KEY missing — required for browser calls"
    fi
    if grep -qE '^CARTESIA_API_KEY=.+' "$UNITY_REPO_PATH/.env" 2>/dev/null; then
      log_success "CARTESIA_API_KEY set"
    else
      log_warn "CARTESIA_API_KEY missing — required for browser calls"
    fi
    if grep -qE '^LIVEKIT_URL=.+' "$UNITY_REPO_PATH/.env" 2>/dev/null; then
      log_success "LIVEKIT_URL set (run: unity voice setup if server not running)"
    else
      log_warn "LiveKit not configured — run: unity voice setup (or unity setup)"
    fi
  fi

  if [[ -f "$CONSOLE_REPO_PATH/.env.local" ]]; then
    log_success "console/.env.local found"
  else
    log_warn "console/.env.local missing — copy from .env.development and set JWT_SECRET"
  fi

  echo ""
  if [[ "$ok" == "true" ]]; then
    log_success "Doctor passed"
    return 0
  fi
  log_error "Doctor found blockers"
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

  # Forward LLM keys from unity/.env when not already exported.
  if [[ -f "$UNITY_REPO_PATH/.env" ]]; then
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
