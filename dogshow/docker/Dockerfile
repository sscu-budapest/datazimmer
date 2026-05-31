# Reproducible cross-package coverage runner for the dogshow.
#
# The ecosystem (datazimmer + dogshow + the editable siblings) is bind-mounted
# at /code by compose rather than COPYed, so this image stays tiny and always
# reflects the working tree. rsync + openssh-server give the ssh remote backend
# a real rsync-over-ssh target (the runner ssh's to itself).
FROM python:3.12-slim

# zstd: dirark packs the depot ark with `tar -I zstd`; rsync + openssh: the ssh
# remote backend's rsync-over-ssh target; git: editable installs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        zstd rsync openssh-server openssh-client git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/tmp/dogshow-venv

WORKDIR /code/apps/datazimmer/dogshow
ENTRYPOINT ["bash", "docker/run-coverage.sh"]
