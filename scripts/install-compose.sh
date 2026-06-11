#!/usr/bin/env bash
# =============================================================================
# install-compose.sh — Stranger install via prebuilt Docker Compose images
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || true)"
BRANCH="${BRANCH:-staging}"
REPO_RAW="https://raw.githubusercontent.com/unifyai/unity/${BRANCH}"
SELFHOST_SRC="${INSTALL_SELFHOST_SRC:-${SCRIPT_DIR:+$SCRIPT_DIR/../deploy/selfhost}}"

UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
CLI_DIR="${CLI_DIR:-$HOME/.local/bin}"
CREATE_CLI=true
NON_INTERACTIVE="${NON_INTERACTIVE:-false}"

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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir) UNITY_HOME="$2"; shift 2 ;;
    --no-cli) CREATE_CLI=false; shift ;;
    --non-interactive) NON_INTERACTIVE=true; shift ;;
    -h|--help)
      cat <<EOF
Unity compose installer — requires Docker only.

  curl -fsSL .../install.sh | bash

Options:
  --dir PATH            Install directory (default: ~/.unity)
  --no-cli              Skip unity CLI shim
  --non-interactive     Import keys from environment; skip prompts
EOF
      exit 0
      ;;
    *) log_err "Unknown option: $1"; exit 1 ;;
  esac
done

print_banner() {
  echo ""
  echo -e "${BOLD}Unity Self-Host (Docker Compose)${NC}"
  echo "Pull prebuilt images, configure BYOK keys, and open Console."
  echo ""
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    log_err "Docker is required."
    case "$(uname -s)" in
      Darwin) log_info "Install Docker Desktop: https://www.docker.com/products/docker-desktop/" ;;
      *) log_info "Install Docker: https://docs.docker.com/get-docker/" ;;
    esac
    exit 1
  fi
  if ! docker info >/dev/null 2>&1; then
    log_err "Docker daemon is not running."
    exit 1
  fi
  log_ok "Docker ready"
}

fetch_remote() {
  local rel="$1"
  local dest="$2"
  curl -fsSL "${REPO_RAW}/${rel}" -o "$dest"
}

install_compose_bundle() {
  mkdir -p "$UNITY_HOME"
  mkdir -p "${UNITY_HOME}/workspace"
  local file
  for file in docker-compose.yml Caddyfile .env.example ensure-pubsub-topics.sh; do
    if [[ -n "$SELFHOST_SRC" && -f "$SELFHOST_SRC/$file" ]]; then
      cp "$SELFHOST_SRC/$file" "$UNITY_HOME/$file"
    else
      fetch_remote "deploy/selfhost/$file" "$UNITY_HOME/$file"
    fi
  done
  if [[ ! -f "$UNITY_HOME/.env" ]]; then
    cp "$UNITY_HOME/.env.example" "$UNITY_HOME/.env"
    # Expand ${HOME} for workspace path
    sed -i.bak "s|\${HOME}|$HOME|g" "$UNITY_HOME/.env" 2>/dev/null || \
      sed -i '' "s|\${HOME}|$HOME|g" "$UNITY_HOME/.env"
    rm -f "$UNITY_HOME/.env.bak"
  fi
  log_ok "Compose bundle installed to $UNITY_HOME"
}

generate_secrets() {
  local env_file="$UNITY_HOME/.env"
  if ! grep -qE '^ORCHESTRA_ADMIN_KEY=.+$' "$env_file" 2>/dev/null; then
    echo "ORCHESTRA_ADMIN_KEY=$(openssl rand -base64 32 | tr -d '/+=' | head -c 43)" >> "$env_file"
  fi
  if ! grep -qE '^NEXTAUTH_SECRET=.+$' "$env_file" 2>/dev/null; then
    echo "NEXTAUTH_SECRET=$(openssl rand -base64 32)" >> "$env_file"
  fi
  if ! grep -qE '^POSTGRES_PASSWORD=.+$' "$env_file" 2>/dev/null; then
    echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)" >> "$env_file"
  fi
}

run_byok_wizard() {
  local wizard="${SCRIPT_DIR:+$SCRIPT_DIR/prompt_byok_keys.sh}"
  if [[ -z "$wizard" || ! -f "$wizard" ]]; then
    local tmp_wizard="$UNITY_HOME/.prompt_byok_keys.sh"
    fetch_remote "scripts/prompt_byok_keys.sh" "$tmp_wizard"
    wizard="$tmp_wizard"
  fi
  if [[ ! -f "$wizard" ]]; then
    log_warn "BYOK wizard not found — add API keys to $UNITY_HOME/.env manually"
    return 0
  fi
  log_info "BYOK wizard (keys written to $UNITY_HOME/.env)..."
  UNITY_ENV_FILE="$UNITY_HOME/.env" \
    NON_INTERACTIVE="$NON_INTERACTIVE" \
    bash "$wizard" ${NON_INTERACTIVE:+--non-interactive}
}

create_compose_cli() {
  [[ "$CREATE_CLI" == "true" ]] || return 0
  local compose_cli="${SCRIPT_DIR:+$SCRIPT_DIR/compose-cli.sh}"
  if [[ -z "$compose_cli" || ! -f "$compose_cli" ]]; then
    compose_cli="$UNITY_HOME/compose-cli.sh"
    fetch_remote "scripts/compose-cli.sh" "$compose_cli"
  fi
  mkdir -p "$CLI_DIR"
  local shim="$CLI_DIR/unity"
  cat > "$shim" <<EOF
#!/usr/bin/env bash
set -e
UNITY_HOME="${UNITY_HOME}"
export UNITY_HOME
COMPOSE_CLI="${compose_cli}"

if [[ -f "\$UNITY_HOME/docker-compose.yml" ]]; then
  case "\${1:-}" in
    ""|stack)
      shift || true
      sub="\${1:-up}"
      shift || true
      case "\$sub" in
        up) exec bash "\$COMPOSE_CLI" up "\$@" ;;
        down|stop) exec bash "\$COMPOSE_CLI" down "\$@" ;;
        doctor) exec bash "\$COMPOSE_CLI" doctor "\$@" ;;
        status|ps) exec bash "\$COMPOSE_CLI" status "\$@" ;;
        logs) shift; exec bash "\$COMPOSE_CLI" logs "\$@" ;;
        restart) exec bash "\$COMPOSE_CLI" restart "\$@" ;;
        *) exec bash "\$COMPOSE_CLI" "\$sub" "\$@" ;;
      esac
      ;;
    setup)
      echo "Compose install is already configured at \$UNITY_HOME" >&2
      echo "Edit \$UNITY_HOME/.env for keys, then: unity stack restart" >&2
      exit 0
      ;;
    doctor) exec bash "\$COMPOSE_CLI" doctor "\$@" ;;
    restart) exec bash "\$COMPOSE_CLI" restart "\$@" ;;
    stop|down) exec bash "\$COMPOSE_CLI" down "\$@" ;;
    status) exec bash "\$COMPOSE_CLI" status "\$@" ;;
    logs) exec bash "\$COMPOSE_CLI" logs "\$@" ;;
    *)
      exec bash "\$COMPOSE_CLI" up "\$@"
      ;;
  esac
fi

echo "Compose stack not found. Re-run the installer." >&2
exit 1
EOF
  chmod +x "$shim"
  log_ok "Installed unity CLI at $shim"
}

pull_and_start() {
  log_info "Pulling images (first run may take several minutes)..."
  docker compose -f "$UNITY_HOME/docker-compose.yml" --env-file "$UNITY_HOME/.env" pull
  log_info "Starting stack..."
  docker compose -f "$UNITY_HOME/docker-compose.yml" --env-file "$UNITY_HOME/.env" up -d
  log_ok "Stack started"
}

open_browser() {
  local url
  url="$(grep -E '^NEXTAUTH_URL=' "$UNITY_HOME/.env" 2>/dev/null | cut -d= -f2- || echo 'http://127.0.0.1:3000')"
  log_info "Open $url to register and chat with your Coordinator"
  case "$(uname -s)" in
    Darwin) open "$url" 2>/dev/null || true ;;
    Linux) xdg-open "$url" 2>/dev/null || true ;;
  esac
}

main() {
  print_banner
  require_docker
  install_compose_bundle
  generate_secrets
  run_byok_wizard
  create_compose_cli
  if [[ "${UNITY_COMPOSE_SKIP_START:-0}" == "1" ]]; then
    log_ok "Skipping image pull/start (UNITY_COMPOSE_SKIP_START=1)"
  else
    pull_and_start
    open_browser
  fi
  echo ""
  log_ok "Installation complete"
  echo ""
  echo "  Daily driver:  unity / unity stack up"
  echo "  UI off:        unity stack down"
  echo "  Stop all:      unity stack down --full"
  echo "  Edit keys:     \$UNITY_HOME/.env  then  unity restart"
  echo ""
}

main "$@"
