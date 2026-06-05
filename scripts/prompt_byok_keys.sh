#!/usr/bin/env bash
# =============================================================================
# prompt_byok_keys.sh — Interactive BYOK wizard for local / self-host installs
# =============================================================================
#
# Prompts for keys missing from unity/.env. Idempotent: skips keys already set.
# Voice keys (Deepgram + Cartesia) are prompted by default — voice is core.
#
# Usage:
#   UNITY_REPO=/path/to/unity ./scripts/prompt_byok_keys.sh
#   ./scripts/prompt_byok_keys.sh --non-interactive   # skip prompts (CI)
#
set -euo pipefail

UNITY_HOME="${UNITY_HOME:-$HOME/.unity}"
UNITY_REPO="${UNITY_REPO:-${UNITY_REPO_PATH:-$UNITY_HOME/unity}}"
ENV_FILE="${UNITY_ENV_FILE:-$UNITY_REPO/.env}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'
log_info()    { echo -e "${CYAN}→${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warn()    { echo -e "${YELLOW}⚠${NC} $1"; }

NON_INTERACTIVE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --non-interactive) NON_INTERACTIVE=true; shift ;;
    *) shift ;;
  esac
done

has_env_value() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] && grep -qE "^${key}=.+$" "$ENV_FILE"
}

upsert_env() {
  local key="$1"
  local val="$2"
  if [[ ! -f "$ENV_FILE" ]]; then
    touch "$ENV_FILE"
  fi
  if grep -qE "^${key}=" "$ENV_FILE"; then
    python3 - "$ENV_FILE" "$key" "$val" <<'PYEOF'
import sys, re
path, key, val = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    lines = f.readlines()
pat = re.compile(rf'^{re.escape(key)}=')
for i, line in enumerate(lines):
    if pat.match(line):
        lines[i] = f'{key}={val}\n'
        break
else:
    lines.append(f'{key}={val}\n')
with open(path, 'w') as f:
    f.writelines(lines)
PYEOF
  else
    printf '%s=%s\n' "$key" "$val" >> "$ENV_FILE"
  fi
}

prompt_secret() {
  local label="$1"
  local var_name="$2"
  local hint="$3"
  local value=""

  if has_env_value "$var_name"; then
    log_success "$var_name already set"
    return 0
  fi

  if [[ "$NON_INTERACTIVE" == "true" ]] || [[ ! -r /dev/tty ]] || [[ ! -w /dev/tty ]]; then
    log_warn "$var_name not set — add it to $ENV_FILE"
    log_info "  $hint"
    return 0
  fi

  echo "" >/dev/tty
  echo -e "${BOLD}$label${NC}" >/dev/tty
  echo "  $hint" >/dev/tty
  printf "Paste %s (hidden, Enter to skip): " "$var_name" >/dev/tty
  if ! IFS= read -rs value </dev/tty 2>/dev/null; then
    IFS= read -r value </dev/tty || value=""
  fi
  echo "" >/dev/tty

  if [[ -z "$value" ]]; then
    log_warn "Skipped $var_name"
    return 0
  fi

  upsert_env "$var_name" "$value"
  log_success "Wrote $var_name to $ENV_FILE"
}

prompt_llm_key() {
  if has_env_value OPENAI_API_KEY || has_env_value ANTHROPIC_API_KEY; then
    log_success "LLM provider key already set"
    return 0
  fi

  if [[ "$NON_INTERACTIVE" == "true" ]] || [[ ! -r /dev/tty ]] || [[ ! -w /dev/tty ]]; then
    log_warn "No LLM key set — add OPENAI_API_KEY or ANTHROPIC_API_KEY to $ENV_FILE"
    return 0
  fi

  echo "" >/dev/tty
  echo -e "${BOLD}LLM provider (required for chat)${NC}" >/dev/tty
  echo "  1) OpenAI    — https://platform.openai.com/api-keys" >/dev/tty
  echo "  2) Anthropic — https://console.anthropic.com/" >/dev/tty
  echo "  3) Skip" >/dev/tty
  local choice=""
  printf "Choice [1-3, default 1]: " >/dev/tty
  IFS= read -r choice </dev/tty || choice=""
  choice="${choice:-1}"

  local var_name=""
  case "$choice" in
    1) var_name="OPENAI_API_KEY" ;;
    2) var_name="ANTHROPIC_API_KEY" ;;
    *) log_warn "Skipped LLM key"; return 0 ;;
  esac

  prompt_secret "LLM" "$var_name" "Required for Coordinator chat."
}

main() {
  if [[ ! -d "$UNITY_REPO" ]]; then
    log_warn "Unity repo not found at $UNITY_REPO — skipping BYOK prompts"
    exit 0
  fi

  echo ""
  echo -e "${BOLD}BYOK setup${NC} — provider keys for chat and voice"
  echo ""

  prompt_llm_key
  prompt_secret \
    "Speech-to-text (required for browser calls)" \
    "DEEPGRAM_API_KEY" \
    "Free tier: https://console.deepgram.com"
  prompt_secret \
    "Text-to-speech (required for browser calls)" \
    "CARTESIA_API_KEY" \
    "Free credits: https://play.cartesia.ai"

  if has_env_value CARTESIA_API_KEY && ! has_env_value VOICE_PROVIDER; then
    upsert_env "VOICE_PROVIDER" "cartesia"
    log_success "Set VOICE_PROVIDER=cartesia (default TTS for local voice)"
  fi

  echo ""
  if has_env_value DEEPGRAM_API_KEY && has_env_value CARTESIA_API_KEY; then
    log_success "Voice BYOK keys configured"
  else
    log_warn "Voice calls need DEEPGRAM_API_KEY + CARTESIA_API_KEY in $ENV_FILE"
  fi
  echo ""
}

main "$@"
