#!/usr/bin/env bash
# =============================================================================
# compose-cli.sh — Docker Compose lifecycle for Unity self-host
# =============================================================================
set -euo pipefail

UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
COMPOSE_DIR="${UNITY_COMPOSE_DIR:-$UNITY_HOME}"
COMPOSE_FILE="${COMPOSE_FILE:-$COMPOSE_DIR/docker-compose.yml}"
ENV_FILE="${ENV_FILE:-$COMPOSE_DIR/.env}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info() { echo -e "${CYAN}→${NC} $1"; }
log_ok() { echo -e "${GREEN}✓${NC} $1"; }
log_warn() { echo -e "${YELLOW}⚠${NC} $1"; }
log_err() { echo -e "${RED}✗${NC} $1" >&2; }

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

require_compose() {
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    log_err "Compose stack not found at $COMPOSE_FILE"
    log_info "Run: curl -fsSL https://raw.githubusercontent.com/unifyai/unity/staging/scripts/install.sh | bash"
    exit 1
  fi
  if ! command -v docker >/dev/null 2>&1; then
    log_err "Docker is required"
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    log_err "Docker daemon is not running"
    exit 1
  fi
}

cmd_up() {
  require_compose
  mkdir -p "$(grep -E '^UNITY_WORKSPACE_HOST=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | sed "s/^\\${HOME}/$HOME/" || echo "$HOME/Unity/Local")"
  log_info "Starting Unity self-host stack..."
  compose up -d "$@"
  log_ok "Stack is up — open ${NEXTAUTH_URL:-http://127.0.0.1:3000}"
}

cmd_down() {
  require_compose
  if [[ "${1:-}" == "--full" ]]; then
    log_info "Stopping all services..."
    compose down
  else
    log_info "Stopping Console UI (runtime services keep running)..."
    compose stop console
    log_ok "Console stopped. CM, gateway, and scheduler remain active."
    log_info "Stop everything: unity stack down --full"
  fi
}

cmd_restart() {
  require_compose
  log_info "Recreating stack with updated .env..."
  compose up -d --force-recreate
  log_ok "Restart complete"
}

cmd_status() {
  require_compose
  compose ps
}

cmd_logs() {
  require_compose
  compose logs -f "${@:-}"
}

_has_env() {
  local key="$1"
  grep -qE "^${key}=.+$" "$ENV_FILE" 2>/dev/null
}

cmd_doctor() {
  require_compose
  echo -e "${BOLD}Self-host compose doctor${NC}"
  echo "========================"
  if docker info >/dev/null 2>&1; then
    log_ok "Docker daemon running"
  else
    log_err "Docker daemon not running"
  fi
  if [[ -f "$ENV_FILE" ]]; then
    log_ok ".env present"
    for key in ORCHESTRA_ADMIN_KEY NEXTAUTH_SECRET JWT_SECRET POSTGRES_PASSWORD \
      INTEGRATION_CONFIRMATION_SECRET; do
      if _has_env "$key"; then
        log_ok "Secret configured ($key)"
      else
        log_err "Missing installer secret: $key — re-run install or set manually"
      fi
    done
    if _has_env OPENAI_API_KEY || _has_env ANTHROPIC_API_KEY || _has_env DEEPSEEK_API_KEY; then
      log_ok "LLM provider key configured"
    else
      log_err "Missing LLM key — set OPENAI_API_KEY, ANTHROPIC_API_KEY, or DEEPSEEK_API_KEY"
    fi
    if _has_env OPENAI_API_KEY; then
      log_ok "OpenAI key present (chat and tool-search embeddings)"
    elif _has_env ANTHROPIC_API_KEY || _has_env DEEPSEEK_API_KEY; then
      log_warn "No OPENAI_API_KEY — tool-search embeddings need OpenAI"
    fi
    if _has_env DEEPGRAM_API_KEY && _has_env CARTESIA_API_KEY; then
      log_ok "Voice BYOK keys configured"
    else
      log_warn "Voice calls need DEEPGRAM_API_KEY and CARTESIA_API_KEY"
    fi
    if _has_env COMPOSIO_API_KEY; then
      log_ok "Composio app integrations configured"
    else
      log_info "COMPOSIO_API_KEY not set — third-party app integrations disabled (optional)"
    fi
  else
    log_err ".env missing at $ENV_FILE"
  fi
  local seed_status
  seed_status="$(compose ps -a --format '{{.Service}}\t{{.State}}\t{{.ExitCode}}' 2>/dev/null \
    | awk '$1=="orchestra-seed"{print $2"\t"$3; exit}')"
  if [[ "$seed_status" == "exited	0" ]]; then
    log_ok "Orchestra billing seed completed"
  elif [[ -n "$seed_status" ]]; then
    log_warn "orchestra-seed status: ${seed_status//$'\t'/ }"
  else
    log_warn "orchestra-seed not found — run: unity stack up"
  fi
  compose ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'
}

cmd_pull() {
  require_compose
  compose pull
}

main() {
  local sub="${1:-up}"
  shift || true
  case "$sub" in
    up) cmd_up "$@" ;;
    down|stop) cmd_down "$@" ;;
    restart) cmd_restart "$@" ;;
    status|ps) cmd_status "$@" ;;
    logs) cmd_logs "$@" ;;
    doctor) cmd_doctor "$@" ;;
    pull) cmd_pull "$@" ;;
    *)
      echo "Usage: compose-cli.sh {up|down|restart|status|logs|doctor|pull}" >&2
      exit 1
      ;;
  esac
}

main "$@"
