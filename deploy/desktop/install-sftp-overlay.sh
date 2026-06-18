#!/bin/bash
# Pool-parity SFTP overlay for the local droid-desktop container.
# Exposes unityuser SFTP on port 2222 with chroot at /Droid (remote /Droid/Local).
set -euo pipefail

echo "=== Installing SFTP overlay for droid-desktop ==="

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server
rm -rf /var/lib/apt/lists/*

if ! id unityuser &>/dev/null; then
  useradd -m -d /Droid -s /bin/bash unityuser
fi

mkdir -p /Droid/.ssh /Droid/Local /Droid/.config /Droid/.local /Droid/.cache /Droid/.vnc \
  /Droid/Desktop /Droid/Downloads /Droid/Documents /Droid/Music \
  /Droid/Pictures /Droid/Videos /Droid/Templates /Droid/Public
chown root:root /Droid
chmod 755 /Droid
for dir in Desktop Downloads Documents Music Pictures Videos Templates Public; do
  mkdir -p "/Droid/$dir"
done
chown -R unityuser:unityuser /Droid/.ssh /Droid/Local /Droid/.config /Droid/.local /Droid/.cache /Droid/.vnc \
  /Droid/Desktop /Droid/Downloads /Droid/Documents /Droid/Music \
  /Droid/Pictures /Droid/Videos /Droid/Templates /Droid/Public
chmod 700 /Droid/.ssh
chmod 755 /Droid/Local

cat >/Droid/.bashrc <<'BASHRC'
if [[ -d /Droid ]] && [[ $- == *i* ]] && [[ -n "$DISPLAY" ]] && [[ -z "$DROID_SHELL_INIT" ]]; then
    export DROID_SHELL_INIT=1
    cd /Droid
fi
BASHRC
chown unityuser:unityuser /Droid/.bashrc

mkdir -p /var/run/sshd

cat >/etc/ssh/sshd_config.d/droid-filesync.conf <<'EOF'
Port 22
Port 2222
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
AuthorizedKeysFile /Droid/.ssh/authorized_keys

Match User unityuser
    ForceCommand internal-sftp
    ChrootDirectory /Droid
    AllowTcpForwarding no
    X11Forwarding no
EOF

echo "  SFTP overlay installed (unityuser, port 2222, chroot /Droid)"
