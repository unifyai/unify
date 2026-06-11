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

mkdir -p /Unity/Local /Unity/.config /Unity/.local /Unity/.cache /Unity/.vnc /Unity/.dbus
for dir in Desktop Downloads Documents Music Pictures Videos Templates Public; do
  mkdir -p "/Unity/$dir"
done
touch /Unity/.ICEauthority
chown -R unityuser:unityuser \
  /Unity/Local /Unity/.config /Unity/.local /Unity/.cache /Unity/.vnc /Unity/.dbus \
  /Unity/.ICEauthority \
  /Unity/Desktop /Unity/Downloads /Unity/Documents /Unity/Music \
  /Unity/Pictures /Unity/Videos /Unity/Templates /Unity/Public
chmod 700 /Unity/.dbus /Unity/.vnc
chmod 600 /Unity/.ICEauthority
chmod 755 /Unity/Local

if [[ -d /root/.cache/ms-playwright && ! -e /Unity/.cache/ms-playwright ]]; then
  ln -sfn /root/.cache/ms-playwright /Unity/.cache/ms-playwright
  chown -h unityuser:unityuser /Unity/.cache/ms-playwright
fi

chown unityuser:unityuser /var/log/magnitude 2>/dev/null || true

apt-get update

exec /usr/bin/supervisord -n -c /app/desktop/supervisord.conf
