#!/usr/bin/env bash
# ============================================================================
# Unity Installer
# ============================================================================
# Installs Unity (https://github.com/unifyai/unity) locally on macOS / Linux /
# WSL2. Clones unity, unify, unillm, and orchestra as siblings under
# $UNITY_HOME and editable-installs the Python repos with uv.
#
# Quick install:
#   curl -fsSL https://raw.githubusercontent.com/unifyai/unity/main/scripts/install.sh | bash
#
# Options:
#   --dir PATH        Installation directory (default: ~/.unity)
#   --branch NAME     Git branch to install (default: main)
#   --no-cli          Skip creating the `unity` command shim
#   --skip-deps       Skip system-dependency checks (PortAudio, etc.)
#   --skip-setup      Skip the local orchestra spin-up at the end
#                     (just install the code; run `unity setup` later)
#   -h, --help        Show this help
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Configuration
UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
UNITY_REPO="${UNITY_HOME}/unity"
UNIFY_REPO="${UNITY_HOME}/unify"
UNILLM_REPO="${UNITY_HOME}/unillm"
BRANCH="main"
PYTHON_VERSION="3.12"
CREATE_CLI=true
CHECK_DEPS=true
RUN_SETUP=true
CLI_DIR="${CLI_DIR:-$HOME/.local/bin}"
REPO_BASE="https://github.com/unifyai"

# ----------------------------------------------------------------------------
# Parse options
# ----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        --dir) UNITY_HOME="$2"; UNITY_REPO="$UNITY_HOME/unity"; UNIFY_REPO="$UNITY_HOME/unify"; UNILLM_REPO="$UNITY_HOME/unillm"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        --no-cli) CREATE_CLI=false; shift ;;
        --skip-deps) CHECK_DEPS=false; shift ;;
        --skip-setup) RUN_SETUP=false; shift ;;
        -h|--help)
            sed -n '2,20p' "$0" | sed 's|^# ||;s|^#$||'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
log_info()    { echo -e "${CYAN}→${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warn()    { echo -e "${YELLOW}⚠${NC} $1"; }
log_error()   { echo -e "${RED}✗${NC} $1"; }

print_banner() {
    echo ""
    echo -e "${MAGENTA}${BOLD}"
    echo "┌─────────────────────────────────────────────────────────┐"
    echo "│                  Unity Installer                         │"
    echo "├─────────────────────────────────────────────────────────┤"
    echo "│  Steerable AI agent orchestration, open-sourced by      │"
    echo "│  Unify. https://github.com/unifyai/unity                │"
    echo "└─────────────────────────────────────────────────────────┘"
    echo -e "${NC}"
}

# ----------------------------------------------------------------------------
# OS detection
# ----------------------------------------------------------------------------
detect_os() {
    case "$(uname -s)" in
        Linux*)
            OS="linux"
            if [ -f /etc/os-release ]; then
                . /etc/os-release
                DISTRO="$ID"
            else
                DISTRO="unknown"
            fi
            ;;
        Darwin*)
            OS="macos"
            DISTRO="macos"
            ;;
        CYGWIN*|MINGW*|MSYS*)
            log_error "Native Windows is not supported."
            log_info "Please install Unity inside WSL2:"
            log_info "  https://learn.microsoft.com/en-us/windows/wsl/install"
            exit 1
            ;;
        *)
            OS="unknown"
            DISTRO="unknown"
            log_warn "Unknown operating system: $(uname -s). Proceeding at your own risk."
            ;;
    esac
    log_success "Detected: $OS ($DISTRO)"
}

# ----------------------------------------------------------------------------
# uv
# ----------------------------------------------------------------------------
ensure_uv() {
    log_info "Checking for uv..."
    if command -v uv &> /dev/null; then
        UV_CMD="uv"
    elif [ -x "$HOME/.local/bin/uv" ]; then
        UV_CMD="$HOME/.local/bin/uv"
    elif [ -x "$HOME/.cargo/bin/uv" ]; then
        UV_CMD="$HOME/.cargo/bin/uv"
    else
        log_info "Installing uv (fast Python package manager)..."
        curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1 || {
            log_error "Failed to install uv. See https://docs.astral.sh/uv/getting-started/installation/"
            exit 1
        }
        if [ -x "$HOME/.local/bin/uv" ]; then
            UV_CMD="$HOME/.local/bin/uv"
        elif [ -x "$HOME/.cargo/bin/uv" ]; then
            UV_CMD="$HOME/.cargo/bin/uv"
        elif command -v uv &> /dev/null; then
            UV_CMD="uv"
        else
            log_error "uv installed but not found on PATH. Add ~/.local/bin to your PATH and re-run."
            exit 1
        fi
    fi
    log_success "uv: $($UV_CMD --version 2>/dev/null)"
}

# ----------------------------------------------------------------------------
# Python
# ----------------------------------------------------------------------------
ensure_python() {
    log_info "Ensuring Python $PYTHON_VERSION is available..."
    if ! $UV_CMD python find "$PYTHON_VERSION" &> /dev/null; then
        log_info "Python $PYTHON_VERSION not found locally — uv will download it during sync."
    else
        log_success "Python $PYTHON_VERSION: $($UV_CMD python find "$PYTHON_VERSION")"
    fi
}

# ----------------------------------------------------------------------------
# System deps (informational — we don't attempt install)
# ----------------------------------------------------------------------------
check_system_deps() {
    [ "$CHECK_DEPS" = "false" ] && return 0
    log_info "Checking system dependencies..."

    local hard_missing=() warn_missing=()

    case "$OS" in
        macos)
            # Xcode CLI tools — required for any package building from sdist (pyaudio etc.)
            xcode-select -p >/dev/null 2>&1 || hard_missing+=("Xcode Command Line Tools  (install: xcode-select --install)")
            pkg-config --exists portaudio-2.0 2>/dev/null || warn_missing+=("portaudio  (install: brew install portaudio)")
            ;;
        linux)
            # pyaudio has no manylinux wheel for Py3.12; it builds from sdist and needs gcc + Python headers.
            command -v gcc >/dev/null 2>&1 || hard_missing+=("gcc / build-essential  (install: sudo apt-get install -y build-essential python3-dev)")
            pkg-config --exists portaudio-2.0 2>/dev/null || hard_missing+=("portaudio development headers  (install: sudo apt-get install -y portaudio19-dev)")
            ;;
    esac

    # Docker — required to run the local Orchestra backend on the
    # install-and-live path. Skip the check entirely when --skip-setup is on,
    # because in that mode the user explicitly opts out of local Orchestra.
    if [ "$RUN_SETUP" != "false" ]; then
        if ! command -v docker >/dev/null 2>&1; then
            case "$OS" in
                macos)
                    hard_missing+=("Docker Desktop  (install: https://www.docker.com/products/docker-desktop/)")
                    ;;
                linux)
                    hard_missing+=("Docker engine  (install: https://docs.docker.com/engine/install/)")
                    ;;
                *)
                    hard_missing+=("Docker  (install: https://docs.docker.com/get-docker/)")
                    ;;
            esac
        elif ! docker info >/dev/null 2>&1; then
            # Docker installed but daemon not running. Hard fail with a
            # specific message, because the user already has what they need
            # and just needs to start it.
            log_error "Docker is installed but the daemon isn't running."
            case "$OS" in
                macos) echo "    Start Docker Desktop, then re-run install.sh." ;;
                linux) echo "    Start the Docker daemon, then re-run install.sh. (e.g. \`sudo systemctl start docker\`)" ;;
                *)     echo "    Start Docker, then re-run install.sh." ;;
            esac
            echo "    (Bypass with --skip-setup to install the code without starting local Orchestra.)"
            exit 1
        fi
    fi

    if [ ${#hard_missing[@]} -gt 0 ]; then
        log_error "Required system packages are missing:"
        for m in "${hard_missing[@]}"; do echo "    - $m"; done
        echo ""
        case "$OS" in
            linux)
                echo "    Quick fix:"
                echo "      sudo apt-get update && sudo apt-get install -y build-essential python3-dev portaudio19-dev"
                echo "    Then re-run: curl -fsSL https://raw.githubusercontent.com/unifyai/unity/main/scripts/install.sh | bash"
                ;;
            macos)
                echo "    Quick fix:"
                echo "      xcode-select --install   # if not already installed"
                echo "      brew install portaudio"
                echo "    Then re-run install.sh."
                ;;
        esac
        echo ""
        echo "    (Bypass with --skip-deps at your own risk; uv sync will fail on native extensions."
        echo "     Bypass Docker requirement with --skip-setup to install the code only.)"
        exit 1
    fi

    if [ ${#warn_missing[@]} -gt 0 ]; then
        log_warn "Optional system packages missing (voice features will be limited):"
        for m in "${warn_missing[@]}"; do echo "    - $m"; done
    else
        log_success "System dependencies OK"
    fi
}

# ----------------------------------------------------------------------------
# Clone a repo (idempotent — pulls if already present, clones otherwise)
# ----------------------------------------------------------------------------
clone_or_update() {
    local name="$1"
    local dest="$2"
    local url="$REPO_BASE/$name.git"

    if [ -d "$dest/.git" ]; then
        log_info "Updating $name at $dest..."
        git -C "$dest" fetch --quiet origin "$BRANCH"
        git -C "$dest" checkout --quiet "$BRANCH" || {
            log_warn "Couldn't checkout $BRANCH in $dest (uncommitted changes?). Skipping update."
            return 0
        }
        git -C "$dest" pull --quiet --ff-only origin "$BRANCH" || log_warn "Fast-forward pull failed in $dest; leaving as-is."
    else
        log_info "Cloning $url into $dest..."
        git clone --quiet --branch "$BRANCH" "$url" "$dest"
    fi
    log_success "$name: $(git -C "$dest" rev-parse --short HEAD)"
}

# ----------------------------------------------------------------------------
# Core install
# ----------------------------------------------------------------------------
do_install() {
    mkdir -p "$UNITY_HOME"
    clone_or_update "unify" "$UNIFY_REPO"
    clone_or_update "unillm" "$UNILLM_REPO"
    clone_or_update "unity" "$UNITY_REPO"

    log_info "Syncing dependencies via uv (this pulls Python 3.12 if missing, may take a few minutes)..."
    (cd "$UNITY_REPO" && $UV_CMD sync)
    log_success "Dependencies installed in $UNITY_REPO/.venv"

    # .env scaffolding
    if [ ! -f "$UNITY_REPO/.env" ] && [ -f "$UNITY_REPO/.env.example" ]; then
        cp "$UNITY_REPO/.env.example" "$UNITY_REPO/.env"
        log_success "Created $UNITY_REPO/.env from .env.example"
    fi
}

# ----------------------------------------------------------------------------
# Interactive LLM-key capture
# ----------------------------------------------------------------------------
# Goal: keep the install-and-live UX to literally one command. If we have a
# controlling terminal (i.e. the user is interactively running `curl … | bash`
# rather than CI / a pipeline), ask them inline for one LLM provider key and
# write it into .env. Otherwise skip silently and rely on the post-install
# hint to tell them what to add.
prompt_llm_key() {
    local env_file="$UNITY_REPO/.env"
    [ -f "$env_file" ] || return 0

    # Bail out if either the openai or anthropic key already has a value.
    if grep -Eq '^(OPENAI_API_KEY|ANTHROPIC_API_KEY)=.+' "$env_file"; then
        LLM_KEY_STATUS=already_set
        return 0
    fi

    # Need an interactive terminal we can read from. `curl | bash` keeps the
    # controlling tty even though stdin is piped, so /dev/tty is the path.
    if [ ! -r /dev/tty ] || [ ! -w /dev/tty ]; then
        LLM_KEY_STATUS=skipped_no_tty
        return 0
    fi

    echo "" > /dev/tty
    echo -e "${BOLD}Add one LLM provider key now (or skip and add later to $env_file).${NC}" > /dev/tty
    echo "  1) OpenAI       (https://platform.openai.com/api-keys)" > /dev/tty
    echo "  2) Anthropic    (https://console.anthropic.com/)" > /dev/tty
    echo "  3) Skip — I'll edit .env myself" > /dev/tty
    local choice=""
    printf "Choice [1-3, default 3]: " > /dev/tty
    IFS= read -r choice < /dev/tty || choice=""
    choice="${choice:-3}"

    local var_name=""
    case "$choice" in
        1) var_name="OPENAI_API_KEY" ;;
        2) var_name="ANTHROPIC_API_KEY" ;;
        *) LLM_KEY_STATUS=skipped_by_user; return 0 ;;
    esac

    local key=""
    printf "Paste %s value (input is hidden): " "$var_name" > /dev/tty
    # -s: silent (don't echo to terminal). Falls back to plain read on shells
    # that don't support -s (extremely rare on macOS/Linux/WSL2 bash).
    if ! IFS= read -rs key < /dev/tty 2>/dev/null; then
        IFS= read -r key < /dev/tty || key=""
    fi
    echo "" > /dev/tty
    if [ -z "$key" ]; then
        LLM_KEY_STATUS=skipped_empty
        return 0
    fi

    # Write the key into .env. We escape forward slashes for sed and use a
    # different delimiter (|) so api-key characters don't collide. Then
    # rewrite the existing empty line in place.
    local tmp="${env_file}.tmp.$$"
    awk -v name="$var_name" -v val="$key" '
        BEGIN { written=0 }
        {
            if ($0 ~ "^"name"=") {
                print name"="val
                written=1
            } else {
                print $0
            }
        }
        END {
            if (!written) {
                print ""
                print name"="val
            }
        }
    ' "$env_file" > "$tmp" && mv "$tmp" "$env_file"
    LLM_KEY_STATUS=written
    LLM_KEY_VAR="$var_name"
    log_success "Wrote $var_name to $env_file"
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
# Unity CLI shim — dispatches subcommands, falls through to the sandbox.
# Generated by install.sh; safe to edit.
set -e
UNITY_HOME="${UNITY_HOME}"
UNITY_REPO="${UNITY_REPO}"
ORCHESTRA_REPO="\$UNITY_HOME/orchestra"
export UNITY_HOME

if [ ! -d "\$UNITY_REPO" ]; then
    echo "Unity is not installed at \$UNITY_REPO. Re-run install.sh or set UNITY_HOME." >&2
    exit 1
fi

case "\${1:-}" in
    setup)
        shift
        exec bash "\$UNITY_REPO/scripts/setup.sh" "\$@"
        ;;
    stop)
        if [ -x "\$ORCHESTRA_REPO/scripts/local.sh" ]; then
            exec bash "\$ORCHESTRA_REPO/scripts/local.sh" stop
        else
            echo "orchestra not installed at \$ORCHESTRA_REPO — nothing to stop." >&2
            exit 1
        fi
        ;;
    status)
        if [ -x "\$ORCHESTRA_REPO/scripts/local.sh" ]; then
            exec bash "\$ORCHESTRA_REPO/scripts/local.sh" status
        else
            echo "orchestra not installed at \$ORCHESTRA_REPO." >&2
            exit 1
        fi
        ;;
    restart)
        if [ -x "\$ORCHESTRA_REPO/scripts/local.sh" ]; then
            exec bash "\$ORCHESTRA_REPO/scripts/local.sh" restart
        else
            echo "orchestra not installed at \$ORCHESTRA_REPO — run \\\`unity setup\\\` first." >&2
            exit 1
        fi
        ;;
    logs|tail)
        LOG_FILE="\$UNITY_REPO/.logs_conversation_sandbox.txt"
        # Ensure the file exists so 'tail' starts cleanly even before the first
        # 'unity' run has written anything.
        touch "\$LOG_FILE" 2>/dev/null || true
        echo "📡 Tailing \$LOG_FILE (Ctrl-C to detach)" >&2
        # -F = follow + retry on rename/truncate (works on macOS BSD tail and GNU tail).
        exec tail -F "\$LOG_FILE"
        ;;
    update)
        # Pull --rebase across the four sibling repos and re-sync the venv.
        # Per-repo failures are surfaced inline but never abort other repos;
        # the user can always run \`unity doctor\` afterwards.
        GREEN="\$(printf '\\033[0;32m')"; RED="\$(printf '\\033[0;31m')"
        YELLOW="\$(printf '\\033[0;33m')"; NC="\$(printf '\\033[0m')"
        BOLD="\$(printf '\\033[1m')"

        pull_repo() {
            local name="\$1" dir="\$2"
            if [ ! -d "\$dir/.git" ]; then
                printf '  %s[skip]%s %s — not a git repo at %s\\n' "\$YELLOW" "\$NC" "\$name" "\$dir"
                return 0
            fi
            local branch
            branch=\$(git -C "\$dir" symbolic-ref --short HEAD 2>/dev/null || echo "")
            if [ -z "\$branch" ]; then
                printf '  %s[skip]%s %s — detached HEAD, leaving as-is\\n' "\$YELLOW" "\$NC" "\$name"
                return 0
            fi
            printf '  %s>%s %s (%s)\\n' "\$BOLD" "\$NC" "\$name" "\$branch"
            local fetch_out
            if ! fetch_out=\$(git -C "\$dir" fetch --quiet origin "\$branch" 2>&1); then
                printf '  %s[FAIL]%s   fetch failed:\\n' "\$RED" "\$NC"
                printf '%s\\n' "\$fetch_out" | sed 's/^/      /'
                return 0
            fi
            local pull_out
            if pull_out=\$(git -C "\$dir" pull --rebase --quiet origin "\$branch" 2>&1); then
                local head
                head=\$(git -C "\$dir" rev-parse --short HEAD 2>/dev/null || echo "?")
                printf '  %s[ok]%s     now at %s\\n' "\$GREEN" "\$NC" "\$head"
            else
                printf '  %s[FAIL]%s   pull --rebase failed:\\n' "\$RED" "\$NC"
                printf '%s\\n' "\$pull_out" | sed 's/^/      /'
                return 0
            fi
            # Failures are reported via stdout; we always return 0 so set -e
            # in the shim doesn't abort after the first per-repo failure.
            return 0
        }

        printf '%sunity update%s\\n' "\$BOLD" "\$NC"
        printf '════════════════════════════════════════════════════════════\\n'

        pull_repo unity          "\$UNITY_REPO"
        pull_repo unify          "\$UNITY_HOME/unify"
        pull_repo unillm         "\$UNITY_HOME/unillm"
        pull_repo orchestra "\$ORCHESTRA_REPO"

        printf '\\n  %s>%s syncing Python dependencies (uv sync)\\n' "\$BOLD" "\$NC"
        if (cd "\$UNITY_REPO" && command -v uv >/dev/null 2>&1 && uv sync >/dev/null 2>&1); then
            printf '  %s[ok]%s     venv synced\\n' "\$GREEN" "\$NC"
        else
            printf '  %s[WARN]%s   uv sync skipped or failed — run manually: cd %s && uv sync\\n' "\$YELLOW" "\$NC" "\$UNITY_REPO"
        fi

        printf '\\n%sDone.%s Run %sunity doctor%s to verify, then %sunity%s to start.\\n' "\$BOLD" "\$NC" "\$BOLD" "\$NC" "\$BOLD" "\$NC"
        ;;
    doctor)
        # Diagnose whether the install-and-live setup is in shape to start
        # the runtime. Each check prints one PASS / WARN / FAIL line plus a
        # one-liner remediation when applicable. Exit code is 0 if everything
        # is green, 1 if any FAIL was reported (WARN does not fail).
        FAIL=0
        WARN=0
        GREEN="\$(printf '\\033[0;32m')"; RED="\$(printf '\\033[0;31m')"
        YELLOW="\$(printf '\\033[0;33m')"; NC="\$(printf '\\033[0m')"
        BOLD="\$(printf '\\033[1m')"
        pass() { printf '  %s[PASS]%s %s\\n' "\$GREEN" "\$NC" "\$1"; }
        warn() { printf '  %s[WARN]%s %s\\n' "\$YELLOW" "\$NC" "\$1"; WARN=\$((WARN+1)); }
        fail() { printf '  %s[FAIL]%s %s\\n' "\$RED" "\$NC" "\$1"; FAIL=\$((FAIL+1)); }
        fix()  { printf '         %s→%s %s\\n' "\$YELLOW" "\$NC" "\$1"; }

        printf '%sunity doctor%s\\n' "\$BOLD" "\$NC"
        printf '════════════════════════════════════════════════════════════\\n'

        printf '\\n%sFilesystem%s\\n' "\$BOLD" "\$NC"
        [ -d "\$UNITY_REPO" ]                        && pass "unity repo at \$UNITY_REPO"                 || { fail "unity repo missing at \$UNITY_REPO"; fix "Re-run install.sh"; }
        [ -d "\$UNITY_HOME/unify" ]                  && pass "unify repo at \$UNITY_HOME/unify"           || { warn "unify repo missing at \$UNITY_HOME/unify";   fix "Re-run install.sh"; }
        [ -d "\$UNITY_HOME/unillm" ]                 && pass "unillm repo at \$UNITY_HOME/unillm"         || { warn "unillm repo missing at \$UNITY_HOME/unillm"; fix "Re-run install.sh"; }
        case "\$ORCHESTRA_REPO" in
            *orchestra-core*)
                fail "CLI shim points at obsolete orchestra-core (use orchestra)"
                fix "Regenerate CLI: bash \$UNITY_REPO/scripts/install.sh --skip-setup --skip-deps"
                ;;
        esac
        [ -d "\$ORCHESTRA_REPO" ]                    && pass "orchestra repo at \$ORCHESTRA_REPO"         || { warn "orchestra repo missing at \$ORCHESTRA_REPO"; fix "Run: unity setup"; }
        [ -f "\$UNITY_REPO/.env" ]                   && pass ".env at \$UNITY_REPO/.env"                  || { fail ".env missing at \$UNITY_REPO/.env";          fix "Re-run install.sh"; }
        [ -d "\$UNITY_REPO/.venv" ]                  && pass "Python venv at \$UNITY_REPO/.venv"          || { warn "Python venv missing at \$UNITY_REPO/.venv";  fix "Run: cd \$UNITY_REPO && uv sync"; }

        printf '\\n%sSystem dependencies%s\\n' "\$BOLD" "\$NC"
        if command -v docker >/dev/null 2>&1; then
            pass "docker installed (\$(docker --version 2>/dev/null | head -1))"
            if docker info >/dev/null 2>&1; then
                pass "docker daemon running"
            else
                fail "docker daemon not running"
                case "\$(uname -s)" in
                    Darwin*) fix "Start Docker Desktop" ;;
                    Linux*)  fix "Start the docker daemon (e.g. sudo systemctl start docker)" ;;
                    *)       fix "Start Docker" ;;
                esac
            fi
        else
            fail "docker not installed"
            case "\$(uname -s)" in
                Darwin*) fix "Install Docker Desktop: https://www.docker.com/products/docker-desktop/" ;;
                Linux*)  fix "Install Docker engine: https://docs.docker.com/engine/install/" ;;
                *)       fix "Install Docker: https://docs.docker.com/get-docker/" ;;
            esac
        fi

        printf '\\n%sReboot persistence%s\\n' "\$BOLD" "\$NC"
        # Postgres data lives in a Docker named volume with --restart
        # unless-stopped, so the *container* comes back when Docker does.
        # The remaining question is whether the Docker daemon itself
        # auto-starts at boot — that's outside Unity's install scope but
        # determines whether reboot persistence works end-to-end.
        case "\$(uname -s)" in
            Darwin*)
                # macOS: Docker Desktop ships with "Start Docker Desktop when
                # you log in" enabled by default. Programmatic detection
                # across Docker Desktop versions is fragile, so just point
                # the user at the setting and let them verify.
                pass "macOS: Docker Desktop autostart is enabled by default"
                fix "Verify in Docker Desktop → Settings → General → \"Start Docker Desktop when you log in\""
                ;;
            Linux*)
                if command -v systemctl >/dev/null 2>&1; then
                    if systemctl is-enabled docker.service >/dev/null 2>&1; then
                        pass "docker.service enabled — daemon auto-starts at boot"
                    else
                        warn "docker.service is not enabled at boot"
                        fix "Enable it once with:  sudo systemctl enable docker"
                        fix "(otherwise Docker won't auto-start after reboot and the Postgres container won't either)"
                    fi
                else
                    warn "systemctl not found — can't verify Docker autostart"
                    fix "Ensure your init system starts Docker at boot"
                fi
                ;;
            *)
                warn "unknown OS — can't verify Docker autostart"
                fix "Ensure Docker is configured to auto-start at boot"
                ;;
        esac

        printf '\\n%sLocal Orchestra%s\\n' "\$BOLD" "\$NC"
        if [ -x "\$ORCHESTRA_REPO/scripts/local.sh" ]; then
            if "\$ORCHESTRA_REPO/scripts/local.sh" check >/dev/null 2>&1; then
                pass "local orchestra reachable (\$(\"\$ORCHESTRA_REPO/scripts/local.sh\" check 2>/dev/null))"
            else
                warn "local orchestra not running"
                fix "Run: unity setup"
            fi
        else
            warn "orchestra local.sh not found"
            fix "Run: unity setup"
        fi

        printf '\\n%s.env keys%s\\n' "\$BOLD" "\$NC"
        if [ -f "\$UNITY_REPO/.env" ]; then
            ENV_FILE="\$UNITY_REPO/.env"
            grep -Eq '^UNIFY_KEY=.+'         "\$ENV_FILE" && pass "UNIFY_KEY set"          || { fail "UNIFY_KEY not set";          fix "Run: unity setup (writes a local key)"; }
            grep -Eq '^ORCHESTRA_URL=.+'     "\$ENV_FILE" && pass "ORCHESTRA_URL set"      || { fail "ORCHESTRA_URL not set";      fix "Run: unity setup"; }
            if   grep -Eq '^OPENAI_API_KEY=.+'    "\$ENV_FILE"; then pass "OPENAI_API_KEY set"
            elif grep -Eq '^ANTHROPIC_API_KEY=.+' "\$ENV_FILE"; then pass "ANTHROPIC_API_KEY set"
            else
                fail "no LLM provider key set"
                fix "Add OPENAI_API_KEY=... or ANTHROPIC_API_KEY=... to \$ENV_FILE"
            fi
        fi

        printf '\\n%sShell PATH%s\\n' "\$BOLD" "\$NC"
        SHIM_DIR="\$(dirname "\$(command -v unity 2>/dev/null || echo /none/unity)")"
        case ":\$PATH:" in
            *":\$SHIM_DIR:"*)
                pass "unity on PATH (\$SHIM_DIR)"
                ;;
            *)
                warn "unity not on this shell's PATH"
                fix "Open a new terminal or source your shell rc"
                ;;
        esac

        printf '\\n'
        if [ "\$FAIL" -gt 0 ]; then
            printf '%sNot ready:%s %s failure(s), %s warning(s).\\n' "\$RED" "\$NC" "\$FAIL" "\$WARN"
            exit 1
        elif [ "\$WARN" -gt 0 ]; then
            printf '%sUsable:%s 0 failures, %s warning(s).\\n' "\$YELLOW" "\$NC" "\$WARN"
            exit 0
        else
            printf '%sAll green — ready to roll.%s\\n' "\$GREEN" "\$NC"
            exit 0
        fi
        ;;
    stack)
        shift
        if [ -x "\$UNITY_REPO/scripts/stack.sh" ]; then
            exec bash "\$UNITY_REPO/scripts/stack.sh" "\$@"
        else
            echo "stack.sh not found at \$UNITY_REPO/scripts/stack.sh" >&2
            exit 1
        fi
        ;;
    voice)
        shift
        if [ -x "\$UNITY_REPO/scripts/voice.sh" ]; then
            exec bash "\$UNITY_REPO/scripts/voice.sh" "\$@"
        else
            echo "voice.sh not found at \$UNITY_REPO/scripts/voice.sh — run \\\`unity setup\\\` first." >&2
            exit 1
        fi
        ;;
    help|--help|-h)
        cat <<'HELP'
Unity CLI

Two-terminal layout (the recommended way to live with your assistant):

  Terminal 1 (chat):     unity
  Terminal 2 (logs):     unity logs

The chat terminal is where you talk to the assistant. The logs terminal
streams everything the runtime is doing in the background — useful for
seeing tool calls, plans, and reasoning unfold while you work.

Usage:
  unity                              Start the full runtime locally (install-and-live).
                                     State persists in the 'Assistants' workspace.
  unity --live-voice                 Same, with live voice calls in the browser.
  unity logs                         Tail the runtime log in a second terminal.

  unity setup                        Bootstrap local orchestra + BYOK wizard (LLM, voice, OAuth)
  unity stack doctor                 Check self-host prerequisites (run before stack up)
  unity stack up                     Start full self-host stack (Console + Coordinator)
  unity stack down                   Stop self-host stack
  unity stop                         Stop local orchestra (preserves data)
  unity status                       Show local orchestra status
  unity restart                      Restart local orchestra (preserves data)
  unity doctor                       Diagnose missing deps, keys, and PATH
  unity update                       git pull --rebase the four repos + uv sync

  unity voice setup                  Install + start local LiveKit for --live-voice
  unity voice stop                   Stop local LiveKit server
  unity voice status                 Report local LiveKit status

  unity help                         Show this message

For live voice calls (unity --live-voice ...):
  unity voice setup    one-time + per-boot LiveKit bring-up
  README "Live voice" section for BYOK voice-provider keys

Dev / eval mode (different workspace, simulated managers, real-comms, etc.):
  see sandboxes/conversation_manager/README.md. Any unrecognized first
  argument is forwarded to that sandbox, so flags like --project_name,
  --overwrite, --real-comms still work.
HELP
        ;;
    *)
        cd "\$UNITY_REPO"
        # shellcheck disable=SC1091
        source .venv/bin/activate
        exec python -m sandboxes.conversation_manager.sandbox "\$@"
        ;;
esac
EOF
    chmod +x "$shim"
    log_success "Installed \`unity\` command at $shim"

    # Check PATH and, if needed, append a clearly-marked Unity block to the
    # user's shell rc so `unity` works in new shells without any manual edit.
    case ":$PATH:" in
        *":$CLI_DIR:"*)
            PATH_STATUS=already_on_path
            ;;
        *)
            ensure_cli_on_path
            ;;
    esac
}

# ----------------------------------------------------------------------------
# Append `export PATH=...` to the user's shell rc (idempotent)
# ----------------------------------------------------------------------------
ensure_cli_on_path() {
    # Pick the rc file appropriate for the user's current shell. Bash + zsh
    # cover ~all macOS / Linux / WSL2 setups; other shells we leave alone
    # with a warning so we don't silently scribble into something exotic.
    local current_shell shell_rc=""
    current_shell="$(basename "${SHELL:-bash}")"
    case "$current_shell" in
        zsh)  shell_rc="$HOME/.zshrc" ;;
        bash)
            # macOS: ~/.bash_profile is the login shell rc; Linux: ~/.bashrc.
            if [ "$OS" = "macos" ] && [ -f "$HOME/.bash_profile" ]; then
                shell_rc="$HOME/.bash_profile"
            else
                shell_rc="$HOME/.bashrc"
            fi
            ;;
        *)
            log_warn "$CLI_DIR is not on your PATH and your shell ($current_shell) isn't bash/zsh."
            log_info "Add this line to your shell profile so \`unity\` is on PATH:"
            echo "    export PATH=\"$CLI_DIR:\$PATH\""
            PATH_STATUS=needs_manual
            return 0
            ;;
    esac

    # Already injected? Bail out.
    if [ -f "$shell_rc" ] && grep -Fq "# >>> unity CLI PATH >>>" "$shell_rc" 2>/dev/null; then
        PATH_STATUS=block_already_present
        log_info "$CLI_DIR is not on your current PATH, but $shell_rc already has the Unity PATH block."
        log_info "Open a new terminal or run:  source $shell_rc"
        return 0
    fi

    # Append a clearly-marked block so future installs / `unity update` can
    # detect and update it without touching the rest of the user's rc.
    {
        printf '\n# >>> unity CLI PATH >>>\n'
        printf '# Added by unity installer on %s\n' "$(date +%Y-%m-%d)"
        printf 'case ":$PATH:" in *":%s:"*) ;; *) export PATH="%s:$PATH" ;; esac\n' "$CLI_DIR" "$CLI_DIR"
        printf '# <<< unity CLI PATH <<<\n'
    } >> "$shell_rc"

    PATH_STATUS=appended
    PATH_RC_FILE="$shell_rc"
    log_success "Added $CLI_DIR to PATH in $shell_rc"
    log_info "Open a new terminal or run:  source $shell_rc"
}

# ----------------------------------------------------------------------------
# Run setup.sh at the end (clones orchestra, spins up local backend, writes .env)
# ----------------------------------------------------------------------------
run_setup() {
    [ "$RUN_SETUP" = "false" ] && {
        log_info "Skipping local orchestra spin-up (--skip-setup). Run \`unity setup\` later."
        SETUP_OK=skipped
        return 0
    }

    if [ ! -x "$UNITY_REPO/scripts/setup.sh" ]; then
        log_warn "setup.sh not found at $UNITY_REPO/scripts/setup.sh — skipping orchestra spin-up."
        SETUP_OK=failed
        return 0
    fi

    # Export UNITY_HOME so setup.sh uses the same layout. Export PATH
    # additions so setup.sh finds uv (which install.sh just installed
    # to ~/.local/bin but didn't add to PATH globally yet).
    UNITY_HOME="$UNITY_HOME" \
        PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH" \
        bash "$UNITY_REPO/scripts/setup.sh"
    local setup_exit=$?

    if (( setup_exit == 0 )); then
        SETUP_OK=ok
    else
        SETUP_OK=failed
        log_warn "Local orchestra setup didn't complete (exit=$setup_exit)."
        log_info "Re-run after fixing the issue:  unity setup"
    fi
}

# ----------------------------------------------------------------------------
# Post-install hints
# ----------------------------------------------------------------------------
print_next_steps() {
    local step=1
    echo ""
    if [ "${SETUP_OK:-}" = "failed" ]; then
        echo -e "${YELLOW}${BOLD}Installation partially complete.${NC}"
        echo "  Code is installed; local orchestra didn't start. See warnings above."
    else
        echo -e "${GREEN}${BOLD}Installation complete.${NC}"
    fi
    echo ""
    echo -e "${BOLD}Next steps:${NC}"
    echo ""

    # ---- Step: finish orchestra bootstrap if it didn't complete ----
    if [ "${SETUP_OK:-}" != "ok" ]; then
        echo "  $step. Bootstrap local orchestra:"
        echo -e "     ${CYAN}\$ unity setup${NC}"
        echo ""
        step=$((step + 1))
    fi

    # ---- Step: LLM key, only if we still need one ----
    case "${LLM_KEY_STATUS:-}" in
        written)
            : ;;  # nothing to ask; user already wrote a key during install
        already_set)
            : ;;  # something was already in .env, leave it alone
        *)
            echo "  $step. Add an LLM provider key to $UNITY_REPO/.env"
            echo "     OPENAI_API_KEY=... or ANTHROPIC_API_KEY=..."
            if [ "${SETUP_OK:-}" = "ok" ]; then
                echo "     (ORCHESTRA_URL and UNIFY_KEY are already wired to local orchestra.)"
            fi
            echo ""
            step=$((step + 1))
            ;;
    esac

    # ---- Step: the two-terminal flow ----
    if [ "$CREATE_CLI" = "true" ]; then
        echo "  $step. Run your assistant in two terminals:"
        echo ""
        echo -e "     ${BOLD}Terminal 1${NC} — chat with your assistant"
        echo -e "         ${CYAN}\$ unity${NC}"
        echo ""
        echo -e "     ${BOLD}Terminal 2${NC} — stream the live runtime log"
        echo -e "         ${CYAN}\$ unity logs${NC}"
        echo ""
        case "${PATH_STATUS:-}" in
            appended)
                echo -e "     ${YELLOW}First time only:${NC} the installer added $CLI_DIR to your PATH in"
                echo "     ${PATH_RC_FILE:-your shell rc}. Open a new terminal (or"
                echo -e "     run ${CYAN}source ${PATH_RC_FILE:-<rc>}${NC}) so \`unity\` is on PATH."
                echo ""
                ;;
            block_already_present)
                echo -e "     ${YELLOW}First time only:${NC} \`unity\` isn't on this terminal's PATH yet —"
                echo "     open a new terminal so the existing shell-rc block takes effect."
                echo ""
                ;;
            needs_manual)
                echo -e "     ${YELLOW}First time only:${NC} add this to your shell profile so \`unity\` is on PATH:"
                echo "         export PATH=\"$CLI_DIR:\$PATH\""
                echo ""
                ;;
        esac
        echo "     State persists across runs in the \`Assistants\` workspace."
        echo "     Stop with Ctrl+C in Terminal 1; \`unity\` again picks up where you left off."
        echo ""
        echo "  Also available:"
        echo -e "     ${CYAN}\$ unity stack doctor${NC}     Check self-host prerequisites"
        echo -e "     ${CYAN}\$ unity stack up${NC}           Full self-host (Console + Coordinator + voice)"
        echo -e "     ${CYAN}\$ unity --live-voice${NC}     Talk to your assistant in the browser (sandbox)"
        echo -e "     ${CYAN}\$ unity voice setup${NC}      Re-run LiveKit + voice BYOK prompts"
        echo -e "     ${CYAN}\$ unity status${NC}           Local orchestra status"
        echo -e "     ${CYAN}\$ unity stop${NC}             Stop local orchestra"
        echo -e "     ${CYAN}\$ unity help${NC}             Subcommand reference"
    else
        echo "  $step. Activate the venv and start the runtime:"
        echo "     \$ cd $UNITY_REPO"
        echo -e "     ${CYAN}\$ source .venv/bin/activate${NC}"
        echo -e "     ${CYAN}\$ python -m sandboxes.conversation_manager.sandbox${NC}"
    fi
    echo ""
    echo "  Documentation: $UNITY_REPO/README.md"
    echo "  Architecture:  $UNITY_REPO/ARCHITECTURE.md"
    echo ""
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
    print_banner
    detect_os
    ensure_uv
    ensure_python
    check_system_deps
    do_install
    create_cli
    run_setup
    if [ "${SETUP_OK:-}" != "ok" ] && [ -x "$UNITY_REPO/scripts/prompt_byok_keys.sh" ]; then
        UNITY_REPO="$UNITY_REPO" bash "$UNITY_REPO/scripts/prompt_byok_keys.sh" || true
    fi
    print_next_steps
}

main "$@"
