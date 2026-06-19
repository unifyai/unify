#!/bin/bash
set -euo pipefail

mkdir -p /run/dbus
rm -f /run/dbus/pid
if ! pgrep -x dbus-daemon >/dev/null 2>&1; then
  dbus-daemon --system --fork
fi

mkdir -p /tmp/runtime-unityuser
chown unityuser:unityuser /tmp/runtime-unityuser
chmod 700 /tmp/runtime-unityuser

bash /app/desktop/inject-ssh-public-key.sh

mkdir -p /Droid/Local /Droid/.config /Droid/.local /Droid/.cache /Droid/.vnc /Droid/.dbus
for dir in Desktop Downloads Documents Music Pictures Videos Templates Public; do
  mkdir -p "/Droid/$dir"
done
touch /Droid/.ICEauthority
chown -R unityuser:unityuser \
  /Droid/Local /Droid/.config /Droid/.local /Droid/.cache /Droid/.vnc /Droid/.dbus \
  /Droid/.ICEauthority \
  /Droid/Desktop /Droid/Downloads /Droid/Documents /Droid/Music \
  /Droid/Pictures /Droid/Videos /Droid/Templates /Droid/Public
chmod 700 /Droid/.dbus /Droid/.vnc
chmod 600 /Droid/.ICEauthority
chmod 755 /Droid/Local

if [[ -d /root/.cache/ms-playwright && ! -e /Droid/.cache/ms-playwright ]]; then
  ln -sfn /root/.cache/ms-playwright /Droid/.cache/ms-playwright
  chown -h unityuser:unityuser /Droid/.cache/ms-playwright
fi

chown unityuser:unityuser /var/log/magnitude 2>/dev/null || true

apt-get update

exec /usr/bin/supervisord -n -c /app/desktop/supervisord.conf
