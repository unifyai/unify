#!/bin/bash
# Install the orchestrated SFTP public key before sshd starts.
set -euo pipefail

if [[ -z "${DROID_SSH_PUBLIC_KEY:-}" ]]; then
  echo "DROID_SSH_PUBLIC_KEY not set — skipping SFTP authorized_keys update"
  exit 0
fi

mkdir -p /Droid/.ssh
printf '%s\n' "$DROID_SSH_PUBLIC_KEY" >/Droid/.ssh/authorized_keys
chown -R unityuser:unityuser /Droid/.ssh
chmod 700 /Droid/.ssh
chmod 600 /Droid/.ssh/authorized_keys
echo "Installed SFTP authorized key for unityuser"
