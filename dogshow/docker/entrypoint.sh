#!/usr/bin/env bash
# Privileged setup, then drop to the host user to run the suite.
#
# Why run as the host user: editable installs import the siblings straight from
# the bind-mounted tree, and the run writes coverage/, REPORT.md and .pytest_cache
# there too. Doing that as root leaves host-unremovable, root-owned files behind
# — and a chown-back trap does not fire on a hard crash. Running *as* the host
# UID makes every byte we write host-owned no matter how the run ends.
#
# sshd needs root, so it stays root; its state lives outside the bind mount. The
# suite (as the host user) ssh's to root@localhost and the rsync target is the
# container's /tmp, so the ssh backend is exercised without touching the host.
set -euo pipefail
cd /code/apps/datazimmer/dogshow

# --- a container user matching the bind mount's owner ----------------------
HOST_UID=$(stat -c %u pyproject.toml)
HOST_GID=$(stat -c %g pyproject.toml)
getent group "$HOST_GID" >/dev/null || groupadd -g "$HOST_GID" runner
if ! id -u "$HOST_UID" >/dev/null 2>&1; then
  useradd -u "$HOST_UID" -g "$HOST_GID" -m -s /bin/bash runner
fi
RUN_USER=$(id -nu "$HOST_UID")
RUN_HOME=$(getent passwd "$HOST_UID" | cut -d: -f6)

# --- ssh-to-self: a real rsync-over-ssh target for the ssh remote backend ---
ssh-keygen -A                       # host keys; sshd won't start without them
/usr/sbin/sshd
install -d -m700 -o "$HOST_UID" -g "$HOST_GID" "$RUN_HOME/.ssh" /tmp/ssh-remote
gosu "$RUN_USER" ssh-keygen -t ed25519 -f "$RUN_HOME/.ssh/id_ed25519" -N "" -q
printf 'Host localhost\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n' \
  > "$RUN_HOME/.ssh/config"
chown "$HOST_UID:$HOST_GID" "$RUN_HOME/.ssh/config"
# let the host user log in as root over ssh via its pubkey (root login defaults
# to prohibit-password, i.e. pubkey allowed); the rsync target stays in /tmp.
install -d -m700 /root/.ssh
cat "$RUN_HOME/.ssh/id_ed25519.pub" >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
# user@host:path form — backend_from_source only treats it as ssh (vs a local
# path) when the user@ is present
export DOGSHOW_SSH_REMOTE="root@localhost:/tmp/ssh-remote"
export HOME="$RUN_HOME"

# --- run the suite as the host user (env preserved by gosu) ----------------
exec gosu "$RUN_USER" bash docker/run-coverage.sh
