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

# Older images symlinked /Droid/.cache/ms-playwright -> /root/.cache/ms-playwright,
# which unityuser cannot read. Copy browsers into the user-owned cache instead.
if [[ -L /Droid/.cache/ms-playwright ]] || { [[ -d /root/.cache/ms-playwright ]] && [[ ! -d /Droid/.cache/ms-playwright/chromium-1228 ]]; }; then
  rm -f /Droid/.cache/ms-playwright
  mkdir -p /Droid/.cache/ms-playwright
  cp -a /root/.cache/ms-playwright/. /Droid/.cache/ms-playwright/
  chown -R unityuser:unityuser /Droid/.cache/ms-playwright
fi

CHROME_PATH=$(find /Droid/.cache/ms-playwright -name "chrome" -type f -executable 2>/dev/null | head -1)
if [[ -n "${CHROME_PATH}" ]]; then
  printf '#!/bin/bash\nexec "%s" --no-sandbox "$@"\n' "${CHROME_PATH}" > /usr/local/bin/chromium-browser
  chmod +x /usr/local/bin/chromium-browser
fi

chown unityuser:unityuser /var/log/magnitude 2>/dev/null || true

apt-get update

exec /usr/bin/supervisord -n -c /app/desktop/supervisord.conf
