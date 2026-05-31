#!/usr/bin/env bash
# Build the env, run the suite under coverage (including aswan's spawned
# workers), and report the combined coverage of every ecosystem package.
set -euo pipefail

# --- ssh-to-self: a real rsync-over-ssh target for the ssh remote backend ---
mkdir -p ~/.ssh /tmp/ssh-remote
if [ ! -f ~/.ssh/id_ed25519 ]; then
  ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -q
  cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
  chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys
  printf 'Host localhost\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n' \
    >> ~/.ssh/config
fi
ssh-keygen -A          # generate host keys; sshd won't start without them
/usr/sbin/sshd
# user@host:path form — backend_from_source only treats it as ssh (vs a local
# path) when the user@ is present
export DOGSHOW_SSH_REMOTE="root@localhost:/tmp/ssh-remote"

# --- dependencies (editable siblings resolved under the bind-mounted /code) ---
uv sync --extra test

# --- coverage across the whole ecosystem, subprocess workers included ---
export COVERAGE_PROCESS_START="$PWD/pyproject.toml"
export PYTHONPATH="$PWD"
uv run coverage erase
set +e  # always report, but surface the test exit code
uv run coverage run -m pytest tests/ -q
rc=$?
set -e
uv run coverage combine
uv run coverage report
uv run coverage html -d coverage/html
echo "combined coverage report written to coverage/html/index.html"
exit "$rc"
