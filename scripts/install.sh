#!/usr/bin/env bash
# ============================================================================
# Unity Installer (public, hosted-backend path)
# ============================================================================
# Installs the Unity agent runtime locally on macOS / Linux / WSL2 and points
# it at the hosted Orchestra backend (https://api.unify.ai). Unity runs on your
# machine; persistence, accounts, and your assistant live in the hosted product
# at https://console.unify.ai.
#
# Quick install:
#   curl -fsSL https://raw.githubusercontent.com/unifyai/unity/staging/scripts/install.sh | bash
#
# Options:
#   --dir PATH        Installation directory (default: ~/.unity)
#   --branch NAME     Git branch to install (default: staging)
#   --no-cli          Skip creating the `unity` command shim
#   --skip-deps       Skip system-dependency checks
#   --reconfigure     Re-run the key/credential wizard only (no clone/sync)
#   -h, --help        Show this help
#
# The full local self-host stack (local Orchestra + Console + Coordinator) is
# an internal-only path and lives in the private unity-deploy repo.
# ============================================================================

set -e

INSTALL_SCRIPT_DIR=""
if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    INSTALL_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log_info()    { echo -e "${CYAN}→${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warn()    { echo -e "${YELLOW}⚠${NC} $1"; }
log_error()   { echo -e "${RED}✗${NC} $1" >&2; }

UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
UNITY_REPO="${UNITY_REPO:-$UNITY_HOME/unity}"
BRANCH="${BRANCH:-staging}"
SHALLOW_CLONE_DEPTH="${SHALLOW_CLONE_DEPTH:-1}"
CREATE_CLI=true
CHECK_DEPS=true
RECONFIGURE_ONLY=false
NON_INTERACTIVE="${NON_INTERACTIVE:-false}"
CLI_DIR="${CLI_DIR:-$HOME/.local/bin}"
REPO_BASE="https://github.com/unifyai"
HOSTED_ORCHESTRA_URL="${HOSTED_ORCHESTRA_URL:-https://api.unify.ai/v0}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dir) UNITY_HOME="$2"; UNITY_REPO="$UNITY_HOME/unity"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        --no-cli) CREATE_CLI=false; shift ;;
        --skip-deps) CHECK_DEPS=false; shift ;;
        --reconfigure) RECONFIGURE_ONLY=true; shift ;;
        --non-interactive) NON_INTERACTIVE=true; shift ;;
        -h|--help) sed -n '2,23p' "${BASH_SOURCE[0]:-/dev/null}" | sed 's/^# \?//'; exit 0 ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

# ----------------------------------------------------------------------------
# Prerequisites: git, python3.12+, uv
# ----------------------------------------------------------------------------
ensure_prereqs() {
    [ "$CHECK_DEPS" = "false" ] && return 0

    if ! command -v git >/dev/null 2>&1; then
        log_error "git is required. Install it and re-run."
        exit 1
    fi

    if ! command -v python3 >/dev/null 2>&1; then
        log_error "Python 3.12+ is required. Install it and re-run."
        exit 1
    fi

    if ! command -v uv >/dev/null 2>&1; then
        log_info "Installing uv (Python package manager)..."
        curl -fsSL https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    fi
    command -v uv >/dev/null 2>&1 || { log_error "uv install failed"; exit 1; }
    log_success "Prerequisites ready"
}

# ----------------------------------------------------------------------------
# Clone (or update) the unity checkout. unify / unillm are resolved as git
# dependencies by uv, so they do not need separate clones.
# ----------------------------------------------------------------------------
clone_or_update_unity() {
    mkdir -p "$UNITY_HOME"
    if [ -d "$UNITY_REPO/.git" ]; then
        log_info "Updating unity checkout at $UNITY_REPO..."
        git -C "$UNITY_REPO" fetch --depth "$SHALLOW_CLONE_DEPTH" origin "$BRANCH" 2>/dev/null || true
        git -C "$UNITY_REPO" checkout "$BRANCH" 2>/dev/null || true
        git -C "$UNITY_REPO" pull --rebase 2>/dev/null || true
    else
        log_info "Cloning unity ($BRANCH) into $UNITY_REPO..."
        git clone --depth "$SHALLOW_CLONE_DEPTH" --branch "$BRANCH" \
            "$REPO_BASE/unity.git" "$UNITY_REPO"
    fi
    log_success "unity checkout ready"
}

uv_sync() {
    log_info "Syncing Python dependencies (uv)..."
    (cd "$UNITY_REPO" && uv sync --all-groups)
    log_success "Dependencies synced"
}

# ----------------------------------------------------------------------------
# Clone (or update) the magnitude repo, which agent-service depends on via
# local file references in agent-service/package.json.
# ----------------------------------------------------------------------------
clone_or_update_magnitude() {
    local magnitude_dir="$UNITY_HOME/magnitude"
    local magnitude_branch="unity-modifications"
    if [ -d "$magnitude_dir/.git" ]; then
        log_info "Updating magnitude checkout at $magnitude_dir..."
        git -C "$magnitude_dir" fetch --depth 1 origin "$magnitude_branch" 2>/dev/null || true
        git -C "$magnitude_dir" checkout "$magnitude_branch" 2>/dev/null || true
        git -C "$magnitude_dir" pull --rebase 2>/dev/null || true
    else
        log_info "Cloning magnitude into $magnitude_dir..."
        git clone --depth 1 --branch "$magnitude_branch" \
            "$REPO_BASE/magnitude.git" "$magnitude_dir" 2>/dev/null || {
            log_warn "Could not clone magnitude (continuing without it — agent-service / computer use will be unavailable until it is present)"
            return 0
        }
    fi
    log_success "magnitude checkout ready"
}

# ----------------------------------------------------------------------------
# Install agent-service Node dependencies (requires magnitude to be present).
# ----------------------------------------------------------------------------
install_agent_service() {
    local agent_service_dir="$UNITY_REPO/agent-service"
    if [ ! -d "$agent_service_dir" ]; then
        log_warn "agent-service directory not found — skipping npm install"
        return 0
    fi
    if ! command -v npm >/dev/null 2>&1; then
        log_warn "npm not found — skipping agent-service install (install Node.js to enable computer use)"
        return 0
    fi
    if [ ! -d "$UNITY_HOME/magnitude" ]; then
        log_warn "magnitude not cloned — skipping agent-service install"
        return 0
    fi
    log_info "Installing agent-service dependencies (npm ci)..."
    (cd "$agent_service_dir" && npm ci --silent) || {
        log_warn "npm ci failed — agent-service may not start (run 'cd $agent_service_dir && npm ci' to retry)"
        return 0
    }
    log_success "agent-service dependencies installed"
}

scaffold_env() {
    if [ ! -f "$UNITY_REPO/.env" ] && [ -f "$UNITY_REPO/.env.example" ]; then
        cp "$UNITY_REPO/.env.example" "$UNITY_REPO/.env"
        log_success "Created $UNITY_REPO/.env"
    fi
}

upsert_env() {
    local key="$1" val="$2"
    python3 - "$UNITY_REPO/.env" "$key" "$val" <<'PY'
import re, sys
from pathlib import Path
path, key, val = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
lines = path.read_text().splitlines() if path.exists() else []
pat = re.compile(rf"^{re.escape(key)}=")
out, replaced = [], False
for line in lines:
    if pat.match(line):
        out.append(f"{key}={val}")
        replaced = True
    else:
        out.append(line)
if not replaced:
    out.append(f"{key}={val}")
path.write_text("\n".join(out) + "\n")
PY
}

env_value() {
    local key="$1"
    [ -f "$UNITY_REPO/.env" ] || return 0
    grep -E "^${key}=" "$UNITY_REPO/.env" | head -1 | cut -d= -f2- | tr -d '"' || true
}

# ----------------------------------------------------------------------------
# Configure hosted credentials + BYOK keys.
# ----------------------------------------------------------------------------
configure_env() {
    scaffold_env

    local orchestra_url
    orchestra_url="$(env_value ORCHESTRA_URL)"
    [ -n "$orchestra_url" ] || upsert_env "ORCHESTRA_URL" "$HOSTED_ORCHESTRA_URL"

    # The local install has no Console front-end; suppress Console-UI knowledge
    # and onboarding prompts in the ConversationManager.
    upsert_env "UNITY_CONSOLE_UI" "false"

    if [ "$NON_INTERACTIVE" = "true" ]; then
        [ -n "${UNIFY_KEY:-}" ] && upsert_env "UNIFY_KEY" "$UNIFY_KEY"
        [ -n "${ASSISTANT_ID:-}" ] && upsert_env "ASSISTANT_ID" "$ASSISTANT_ID"
    else
        echo ""
        echo -e "${BOLD}Connect to your hosted assistant${NC}"
        echo "Get your API key and assistant id at https://console.unify.ai"
        echo ""
        local unify_key assistant_id
        if [ -z "$(env_value UNIFY_KEY)" ]; then
            read -r -p "Unify API key (UNIFY_KEY): " unify_key || true
            [ -n "$unify_key" ] && upsert_env "UNIFY_KEY" "$unify_key"
        fi
        if [ -z "$(env_value ASSISTANT_ID)" ]; then
            read -r -p "Assistant id (ASSISTANT_ID): " assistant_id || true
            [ -n "$assistant_id" ] && upsert_env "ASSISTANT_ID" "$assistant_id"
        fi
    fi

    # LLM / voice / research BYOK keys.
    if [ -x "$UNITY_REPO/scripts/prompt_byok_keys.sh" ]; then
        UNITY_REPO="$UNITY_REPO" NON_INTERACTIVE="$NON_INTERACTIVE" \
            bash "$UNITY_REPO/scripts/prompt_byok_keys.sh" || true
    fi
    log_success "Configuration written to $UNITY_REPO/.env"
}

# ----------------------------------------------------------------------------
# CLI shim at ~/.local/bin/unity
# ----------------------------------------------------------------------------
create_cli() {
    [ "$CREATE_CLI" = "false" ] && return 0
    mkdir -p "$CLI_DIR"
    local shim="$CLI_DIR/unity"

    cat > "$shim" <<EOF
#!/usr/bin/env bash
# Unity CLI shim — runs the local agent runtime against the hosted backend.
# Generated by install.sh; safe to edit.
set -e
UNITY_HOME="${UNITY_HOME}"
UNITY_REPO="${UNITY_REPO}"
export UNITY_HOME

if [ ! -d "\$UNITY_REPO" ]; then
    echo "Unity is not installed at \$UNITY_REPO. Re-run install.sh or set UNITY_HOME." >&2
    exit 1
fi

PY="\$UNITY_REPO/.venv/bin/python"
[ -x "\$PY" ] || PY="python3"

case "\${1:-}" in
    ""|chat|sandbox)
        # Interactive local chat with the full ConversationManager.
        shift || true
        cd "\$UNITY_REPO"
        exec "\$PY" -m sandboxes.conversation_manager.sandbox "\$@"
        ;;
    serve|run)
        # Headless: start the ConversationManager + gateway against hosted Orchestra.
        shift || true
        exec bash "\$UNITY_REPO/scripts/local.sh" start --full "\$@"
        ;;
    stop|down)
        exec bash "\$UNITY_REPO/scripts/local.sh" stop
        ;;
    status)
        exec bash "\$UNITY_REPO/scripts/local.sh" status
        ;;
    logs|tail)
        LOG_FILE="/tmp/unity-local.log"
        touch "\$LOG_FILE" 2>/dev/null || true
        echo "📡 Tailing \$LOG_FILE (Ctrl-C to detach)" >&2
        exec tail -F "\$LOG_FILE"
        ;;
    doctor)
        exec bash "\$UNITY_REPO/scripts/local.sh" gateway-doctor
        ;;
    voice)
        shift || true
        exec bash "\$UNITY_REPO/scripts/voice.sh" "\$@"
        ;;
    setup|reconfigure)
        exec bash "\$UNITY_REPO/scripts/install.sh" --reconfigure
        ;;
    update|pull)
        echo "Updating unity checkout..."
        git -C "\$UNITY_REPO" pull --rebase || true
        (cd "\$UNITY_REPO" && uv sync --all-groups)
        MAGNITUDE_DIR="\$UNITY_HOME/magnitude"
        if [ -d "\$MAGNITUDE_DIR/.git" ]; then
            echo "Updating magnitude checkout (unity-modifications)..."
            git -C "\$MAGNITUDE_DIR" fetch --depth 1 origin unity-modifications 2>/dev/null || true
            git -C "\$MAGNITUDE_DIR" checkout unity-modifications 2>/dev/null || true
            git -C "\$MAGNITUDE_DIR" pull --rebase || true
        fi
        if command -v npm >/dev/null 2>&1 && [ -d "\$UNITY_REPO/agent-service" ] && [ -d "\$MAGNITUDE_DIR" ]; then
            echo "Refreshing agent-service dependencies..."
            (cd "\$UNITY_REPO/agent-service" && npm ci --silent) || true
        fi
        ;;
    help|--help|-h)
        cat <<USAGE
unity                  Interactive local chat (alias: unity chat)
unity serve            Start CM + gateway headless against hosted Orchestra
unity stop             Stop the local runtime
unity status           Show runtime status
unity logs             Follow the runtime log
unity doctor           Gateway/config checks
unity voice [...]      Local LiveKit setup for --live-voice
unity setup            Re-run the key/credential wizard
unity update           Update the checkout and re-sync deps
USAGE
        ;;
    *)
        # Forward unknown args to the sandbox.
        cd "\$UNITY_REPO"
        exec "\$PY" -m sandboxes.conversation_manager.sandbox "\$@"
        ;;
esac
EOF
    chmod +x "$shim"
    log_success "Installed unity CLI at $shim"
}

inject_path() {
    [ "$CREATE_CLI" = "false" ] && return 0
    case ":$PATH:" in
        *":$CLI_DIR:"*) return 0 ;;
    esac
    local rc=""
    case "$(basename "${SHELL:-}")" in
        zsh) rc="$HOME/.zshrc" ;;
        bash) rc="$HOME/.bashrc" ;;
        *) rc="$HOME/.profile" ;;
    esac
    if [ -n "$rc" ] && ! grep -q "# >>> unity PATH >>>" "$rc" 2>/dev/null; then
        {
            echo ""
            echo "# >>> unity PATH >>>"
            echo "export PATH=\"$CLI_DIR:\$PATH\""
            echo "# <<< unity PATH <<<"
        } >> "$rc"
        log_info "Added $CLI_DIR to PATH in $rc (open a new terminal)"
    fi
}

print_done() {
    echo ""
    echo -e "${BOLD}Unity installed.${NC}"
    echo ""
    echo "  unity            Start an interactive local chat"
    echo "  unity serve      Run headless (CM + gateway)"
    echo "  unity help       Command reference"
    echo ""
    echo "Keys live in $UNITY_REPO/.env — edit and re-run 'unity setup' any time."
    echo "Manage your assistant and account at https://console.unify.ai"
    echo ""
}

main() {
    if [ "$RECONFIGURE_ONLY" = "true" ]; then
        configure_env
        exit 0
    fi
    echo -e "${BOLD}Unity installer${NC} (branch: $BRANCH)"
    ensure_prereqs
    clone_or_update_unity
    clone_or_update_magnitude
    uv_sync
    install_agent_service
    configure_env
    create_cli
    inject_path
    print_done
}

main
