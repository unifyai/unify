#!/bin/bash
# Install the orchestrated SFTP public key before sshd starts.
set -euo pipefail

if [[ -z "${UNITY_SSH_PUBLIC_KEY:-}" ]]; then
  echo "UNITY_SSH_PUBLIC_KEY not set — skipping SFTP authorized_keys update"
  exit 0
fi

mkdir -p /Unity/.ssh
printf '%s\n' "$UNITY_SSH_PUBLIC_KEY" >/Unity/.ssh/authorized_keys
chown -R unityuser:unityuser /Unity/.ssh
chmod 700 /Unity/.ssh
chmod 600 /Unity/.ssh/authorized_keys
echo "Installed SFTP authorized key for unityuser"
