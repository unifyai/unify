#!/usr/bin/env bash
# ============================================================================
# Unity voice setup — bring up standalone local LiveKit for `--live-voice`
# ============================================================================
# Sub-commands:
#   setup    Install livekit-server (if missing), boot it in --dev mode,
#            and write LIVEKIT_URL / LIVEKIT_API_KEY / LIVEKIT_API_SECRET
#            to ~/.unity/unity/.env. Idempotent: re-running is safe.
#   stop     Stop the backgrounded livekit-server process.
#   status   Report whether livekit-server is running + reachable.
#
# After `unity voice setup`, the user still needs:
#   - DEEPGRAM_API_KEY  (https://deepgram.com — free tier)
#   - CARTESIA_API_KEY or ELEVEN_API_KEY  (https://cartesia.ai or
#     https://elevenlabs.io — both have free credits)
# These are voice-provider keys that must come from a paid (or free-tier)
# account; we deliberately do not auto-provision them.
#
# Then:
#   unity --live-voice --project_name Sandbox --overwrite
#   cm> call
# opens a LiveKit Agents Playground in the browser (auto-bootstrapped on
# first use; needs Node.js + npm/pnpm) and connects to the voice agent.
#
# The all-repo source stack in unity-deploy/selfhost does not use this local
# server; it loads LiveKit Cloud credentials from the self-host state directory.
# ============================================================================

set -e

UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
UNITY_REPO="${UNITY_REPO:-${UNITY_REPO_PATH:-$UNITY_HOME/unity}}"
VOICE_PIDFILE="${UNITY_HOME}/.livekit-server.pid"
VOICE_LOGFILE="${UNITY_HOME}/.livekit-server.log"

# LiveKit `--dev` mode hard-codes these placeholder credentials. They are
# fine for local-only development since the server only binds 127.0.0.1.
LIVEKIT_DEV_URL="ws://localhost:7880"
LIVEKIT_DEV_KEY="devkey"
LIVEKIT_DEV_SECRET="secret"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
log_info()    { echo -e "${CYAN}→${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warn()    { echo -e "${YELLOW}⚠${NC} $1"; }
log_error()   { echo -e "${RED}✗${NC} $1"; }

# ---------------------------------------------------------------------------
# livekit-server installation
# ---------------------------------------------------------------------------
ensure_livekit_installed() {
    if command -v livekit-server >/dev/null 2>&1; then
        log_success "livekit-server present: $(livekit-server --version 2>&1 | head -1)"
        return 0
    fi
    if [ ! -x "$UNITY_REPO/scripts/install_livekit.sh" ]; then
        log_error "Installer not found at $UNITY_REPO/scripts/install_livekit.sh"
        log_info  "  Re-run \`unity setup\` first, then retry \`unity voice setup\`."
        return 1
    fi
    log_info "Installing livekit-server (one binary, one-time)..."
    local install_dir="${HOME}/.local/bin"
    mkdir -p "$install_dir"
    bash "$UNITY_REPO/scripts/install_livekit.sh" "$install_dir" || {
        log_error "livekit-server install failed."
        return 1
    }
    case ":$PATH:" in
        *":$install_dir:"*) ;;
        *)
            log_warn "$install_dir is not on your PATH."
            log_info "  Add this to your shell profile:"
            echo  "    export PATH=\"$install_dir:\$PATH\""
            export PATH="$install_dir:$PATH"
            ;;
    esac
    log_success "livekit-server installed: $(livekit-server --version 2>&1 | head -1)"
}

# ---------------------------------------------------------------------------
# livekit-server lifecycle
# ---------------------------------------------------------------------------
is_livekit_running() {
    [ -f "$VOICE_PIDFILE" ] || return 1
    local pid
    pid="$(cat "$VOICE_PIDFILE" 2>/dev/null || true)"
    [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null
}

start_livekit() {
    if is_livekit_running; then
        if (echo > "/dev/tcp/127.0.0.1/7880") 2>/dev/null; then
            log_success "livekit-server already running (pid $(cat "$VOICE_PIDFILE"))"
            return 0
        fi
        log_warn "Stale livekit pidfile — restarting livekit-server"
        rm -f "$VOICE_PIDFILE"
    fi
    log_info "Starting livekit-server in --dev mode (logs -> $VOICE_LOGFILE)..."
    # Detach so the server survives stack/console shell exit. Linux uses
    # setsid; macOS has no setsid, so nohup is the reliable fallback.
    local pid=""
    if command -v setsid >/dev/null 2>&1; then
        setsid bash -c "exec livekit-server --dev >>\"$VOICE_LOGFILE\" 2>&1" &
        pid=$!
    else
        nohup livekit-server --dev >>"$VOICE_LOGFILE" 2>&1 &
        pid=$!
    fi
    disown "$pid" 2>/dev/null || true
    echo "$pid" >"$VOICE_PIDFILE"

    # Wait for port 7880 to listen
    local waited=0
    while ! (echo > "/dev/tcp/127.0.0.1/7880") 2>/dev/null; do
        if (( waited >= 15 )); then
            log_error "livekit-server did not bind 7880 within 15s. Last log lines:"
            tail -10 "$VOICE_LOGFILE" >&2 || true
            rm -f "$VOICE_PIDFILE"
            return 1
        fi
        sleep 1
        (( ++waited ))
    done
    log_success "livekit-server ready on $LIVEKIT_DEV_URL (pid $pid)"
}

stop_livekit() {
    if [ ! -f "$VOICE_PIDFILE" ]; then
        log_info "livekit-server is not running."
        return 0
    fi
    local pid
    pid="$(cat "$VOICE_PIDFILE" 2>/dev/null || true)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 1
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$VOICE_PIDFILE"
    log_success "livekit-server stopped"
}

# ---------------------------------------------------------------------------
# .env wiring
# ---------------------------------------------------------------------------
wire_voice_env() {
    local env_file="$UNITY_REPO/.env"
    if [ ! -f "$env_file" ]; then
        log_error ".env not found at $env_file. Run \`unity setup\` first."
        return 1
    fi
    upsert() {
        local key="$1" val="$2"
        if grep -qE "^${key}=" "$env_file"; then
            python3 - "$env_file" "$key" "$val" <<'PYEOF'
import sys, re
path, key, val = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    lines = f.readlines()
pat = re.compile(rf'^{re.escape(key)}=')
for i, line in enumerate(lines):
    if pat.match(line):
        lines[i] = f'{key}={val}\n'
        break
with open(path, 'w') as f:
    f.writelines(lines)
PYEOF
        else
            printf '%s=%s\n' "$key" "$val" >> "$env_file"
        fi
    }
    upsert LIVEKIT_URL        "$LIVEKIT_DEV_URL"
    upsert LIVEKIT_API_KEY    "$LIVEKIT_DEV_KEY"
    upsert LIVEKIT_API_SECRET "$LIVEKIT_DEV_SECRET"
    log_success "Wrote LIVEKIT_URL / LIVEKIT_API_KEY / LIVEKIT_API_SECRET to $env_file"
}

# ---------------------------------------------------------------------------
# Helper: report which voice-provider keys still need to be filled in
# ---------------------------------------------------------------------------
report_byok_status() {
    local env_file="$UNITY_REPO/.env"
    [ -f "$env_file" ] || return 0
    has_value() { grep -qE "^${1}=.+$" "$env_file"; }

    echo ""
    echo -e "${BOLD}You still need to add voice-provider keys to ${env_file}:${NC}"
    echo ""
    if has_value DEEPGRAM_API_KEY; then
        log_success "DEEPGRAM_API_KEY  (already set)"
    else
        log_warn "DEEPGRAM_API_KEY  — speech-to-text. Get one free at https://console.deepgram.com"
    fi
    if has_value CARTESIA_API_KEY; then
        log_success "CARTESIA_API_KEY  (already set)"
    elif has_value ELEVEN_API_KEY; then
        log_success "ELEVEN_API_KEY  (already set; using ElevenLabs as TTS provider)"
    else
        log_warn "CARTESIA_API_KEY  — text-to-speech. Free credits at https://play.cartesia.ai"
        echo  "                    (or ELEVEN_API_KEY from https://elevenlabs.io)"
    fi
    echo ""
    echo -e "${BOLD}Then start a live call:${NC}"
    echo "  ${CYAN}\$ unity --live-voice --project_name Sandbox --overwrite${NC}"
    echo "  ${CYAN}cm> call${NC}    (browser opens; talk to the assistant)"
    echo ""
}

# ---------------------------------------------------------------------------
# Subcommand dispatch
# ---------------------------------------------------------------------------
cmd="${1:-setup}"
case "$cmd" in
    setup)
        echo ""
        echo -e "${BOLD}Unity voice setup${NC} — bootstrapping local LiveKit"
        echo ""
        if [ ! -d "$UNITY_REPO" ]; then
            log_error "Unity is not installed at $UNITY_REPO. Run \`unity setup\` first."
            exit 1
        fi
        ensure_livekit_installed || exit 1
        start_livekit            || exit 1
        wire_voice_env           || exit 1
        report_byok_status
        ;;
    stop)
        stop_livekit
        ;;
    status)
        if is_livekit_running; then
            log_success "livekit-server: running (pid $(cat "$VOICE_PIDFILE"))"
            if (echo > "/dev/tcp/127.0.0.1/7880") 2>/dev/null; then
                log_success "ws://localhost:7880 listening"
            else
                log_error "port 7880 not listening (server may have crashed)"
            fi
        else
            log_error "livekit-server: not running"
        fi
        ;;
    *)
        echo "usage: unity voice {setup|stop|status}" >&2
        exit 2
        ;;
esac
