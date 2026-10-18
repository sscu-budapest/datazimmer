"""Serve the federation site for a manual showcase run.

    python -m tests.site        # then, in another shell, from dogshow/:
    dz run && dz validate && dz draw && dz publish

The test suite boots the site itself; this entrypoint exists so the showcase is
runnable by hand. It serves the Flask "federation" long-lived and keeps a relay
worker available (re-spawning the bounded worker) so the relayed results feed is
delivered on every ``dz run``.
"""

import threading

from tests.site.federation import federation_app_creator, federation_port
from tests.site.runner import start_relay_worker


def _keep_relay_available():
    while True:
        start_relay_worker().join()


def main():
    threading.Thread(target=_keep_relay_available, daemon=True).start()
    print(
        f"federation site on http://localhost:{federation_port} "
        "(relay worker running) — Ctrl-C to stop"
    )
    federation_app_creator().run(port=federation_port)


if __name__ == "__main__":
    main()
