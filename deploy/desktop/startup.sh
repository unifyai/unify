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

# Older images symlinked /Unity/.cache/ms-playwright -> /root/.cache/ms-playwright,
# which unityuser cannot read. Copy browsers into the user-owned cache instead.
if [[ -L /Unity/.cache/ms-playwright ]] || { [[ -d /root/.cache/ms-playwright ]] && [[ ! -d /Unity/.cache/ms-playwright/chromium-1228 ]]; }; then
  rm -f /Unity/.cache/ms-playwright
  mkdir -p /Unity/.cache/ms-playwright
  cp -a /root/.cache/ms-playwright/. /Unity/.cache/ms-playwright/
  chown -R unityuser:unityuser /Unity/.cache/ms-playwright
fi

CHROME_PATH=$(find /Unity/.cache/ms-playwright -name "chrome" -type f -executable 2>/dev/null | head -1)
if [[ -n "${CHROME_PATH}" ]]; then
  printf '#!/bin/bash\nexec "%s" --no-sandbox "$@"\n' "${CHROME_PATH}" > /usr/local/bin/chromium-browser
  chmod +x /usr/local/bin/chromium-browser
fi

chown unityuser:unityuser /var/log/magnitude 2>/dev/null || true

apt-get update

exec /usr/bin/supervisord -n -c /app/desktop/supervisord.conf
