#!/bin/bash
# Pool-parity SFTP overlay for the local unity-desktop container.
# Exposes unityuser SFTP on port 2222 with chroot at /Unity (remote /Unity/Local).
set -euo pipefail

echo "=== Installing SFTP overlay for unity-desktop ==="

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server
rm -rf /var/lib/apt/lists/*

if ! id unityuser &>/dev/null; then
  useradd -m -d /Unity -s /bin/bash unityuser
fi

mkdir -p /Unity/.ssh /Unity/Local /Unity/.config /Unity/.local /Unity/.cache /Unity/.vnc \
  /Unity/Desktop /Unity/Downloads /Unity/Documents /Unity/Music \
  /Unity/Pictures /Unity/Videos /Unity/Templates /Unity/Public
chown root:root /Unity
chmod 755 /Unity
for dir in Desktop Downloads Documents Music Pictures Videos Templates Public; do
  mkdir -p "/Unity/$dir"
done
chown -R unityuser:unityuser /Unity/.ssh /Unity/Local /Unity/.config /Unity/.local /Unity/.cache /Unity/.vnc \
  /Unity/Desktop /Unity/Downloads /Unity/Documents /Unity/Music \
  /Unity/Pictures /Unity/Videos /Unity/Templates /Unity/Public
chmod 700 /Unity/.ssh
chmod 755 /Unity/Local

cat >/Unity/.bashrc <<'BASHRC'
if [[ -d /Unity ]] && [[ $- == *i* ]] && [[ -n "$DISPLAY" ]] && [[ -z "$UNITY_SHELL_INIT" ]]; then
    export UNITY_SHELL_INIT=1
    cd /Unity
fi
BASHRC
chown unityuser:unityuser /Unity/.bashrc

mkdir -p /var/run/sshd

cat >/etc/ssh/sshd_config.d/unity-filesync.conf <<'EOF'
Port 22
Port 2222
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
AuthorizedKeysFile /Unity/.ssh/authorized_keys

Match User unityuser
    ForceCommand internal-sftp
    ChrootDirectory /Unity
    AllowTcpForwarding no
    X11Forwarding no
EOF

echo "  SFTP overlay installed (unityuser, port 2222, chroot /Unity)"
