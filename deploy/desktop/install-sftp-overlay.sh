#!/bin/bash
# Pool-parity SFTP overlay for the local unity-desktop container.
# Exposes unityuser SFTP on port 2222 with chroot at /Unity (remote /Unity/Local).
set -euo pipefail

echo "=== Installing SFTP overlay for unity-desktop ==="

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server
rm -rf /var/lib/apt/lists/*

if ! id unityuser &>/dev/null; then
  useradd -m -d /Unity -s /usr/sbin/nologin unityuser
fi

mkdir -p /Unity/Local /Unity/.ssh
chown root:root /Unity
chmod 755 /Unity
chown -R unityuser:unityuser /Unity/Local /Unity/.ssh
chmod 755 /Unity/Local

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
