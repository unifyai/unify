#!/bin/bash
# Virtual desktop: TigerVNC (display :99), XFCE4, noVNC proxy.
# Runs as unityuser with HOME=/Droid so the session matches pool VM layout.

set -euo pipefail

export HOME="${HOME:-/Droid}"
export DISPLAY="${DISPLAY:-:99}"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/tmp/runtime-unityuser}"

mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

rm -f /tmp/.X99-lock
rm -f /tmp/.X11-unix/X99

mkdir -p "$HOME/.vnc"
echo "${UNIFY_KEY:-changeme}" | vncpasswd -f >"$HOME/.vnc/passwd"
chmod 600 "$HOME/.vnc/passwd"

Xtigervnc :99 -geometry 1920x1080 -depth 24 \
  -rfbport 5900 -rfbauth "$HOME/.vnc/passwd" \
  -AlwaysShared -desktop "Droid Desktop" &
sleep 2

eval "$(dbus-launch --sh-syntax)"
export DBUS_SESSION_BUS_ADDRESS

startxfce4 &
sleep 2

xset s off
xset -dpms
xset s noblank

/usr/libexec/xdg-desktop-portal &
/usr/libexec/xdg-desktop-portal-gtk &

mkdir -p /tmp/unify/assistant/install

exec websockify --web=/opt/novnc 6080 localhost:5900
