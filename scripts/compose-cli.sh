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
    for key in OPENAI_API_KEY ANTHROPIC_API_KEY; do
      if grep -qE "^${key}=.+$" "$ENV_FILE" 2>/dev/null; then
        log_ok "LLM key configured ($key)"
        break
      fi
    done
  else
    log_err ".env missing at $ENV_FILE"
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
