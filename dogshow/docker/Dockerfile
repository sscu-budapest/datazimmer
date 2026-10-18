# Reproducible cross-package coverage runner for the dogshow.
#
# The ecosystem (datazimmer + dogshow + the editable siblings) is bind-mounted
# at /code by compose rather than COPYed, so this image stays tiny and always
# reflects the working tree. rsync + openssh-server give the ssh remote backend
# a real rsync-over-ssh target (the runner ssh's to itself).
FROM python:3.12-slim

# zstd: dirark packs the depot ark with `tar -I zstd`; rsync + openssh: the ssh
# remote backend's rsync-over-ssh target; git: editable installs; gosu: drop from
# root (needed for sshd) to the host user for the run so nothing written to the
# bind mount is ever root-owned.
RUN apt-get update && apt-get install -y --no-install-recommends \
        zstd rsync openssh-server openssh-client git ca-certificates gosu \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# PYTHONDONTWRITEBYTECODE: never litter the bind-mounted sibling trees with
# __pycache__; UV_*: keep the venv and cache off the bind mount (in the
# container's /tmp), so the build writes nothing host-visible.
ENV PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/tmp/dogshow-venv \
    UV_CACHE_DIR=/tmp/uv-cache

WORKDIR /code/apps/datazimmer/dogshow
ENTRYPOINT ["bash", "docker/entrypoint.sh"]
