"""Minimal Zenodo deposition: create a draft, upload files, optionally publish.

Credentials come from the standard env vars ``ZENODO_TOKEN`` (or
``ZENODO_SANDBOX_TOKEN`` when ``sandbox=True``); every request is bounded by
``remote.TIMEOUT``.
"""

import os
from pathlib import Path

from .remote import TIMEOUT

_LIVE = "https://zenodo.org/api"
_SANDBOX = "https://sandbox.zenodo.org/api"


class ZenodoDeposit:
    def __init__(self, *, sandbox: bool = False, token: str | None = None) -> None:
        import requests

        self.base = _SANDBOX if sandbox else _LIVE
        env_var = "ZENODO_SANDBOX_TOKEN" if sandbox else "ZENODO_TOKEN"
        self._session = requests.Session()
        self._session.params = {"access_token": token or os.environ[env_var]}
        self._deposition: dict | None = None

    def create(
        self,
        title: str,
        version: str,
        *,
        description: str = "",
        upload_type: str = "dataset",
        creators: list[dict] | None = None,
    ) -> int:
        meta = {
            "metadata": {
                "title": title,
                "version": version,
                "description": description or title,
                "upload_type": upload_type,
                "creators": creators or [{"name": "Unknown"}],
            }
        }
        resp = self._session.post(
            f"{self.base}/deposit/depositions", json=meta, timeout=TIMEOUT
        )
        resp.raise_for_status()
        self._deposition = resp.json()
        return self._deposition["id"]

    def upload(self, path: Path | str) -> dict:
        path = Path(path)
        bucket = self._require_deposition()["links"]["bucket"]
        with open(path, "rb") as fh:
            resp = self._session.put(f"{bucket}/{path.name}", data=fh, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def publish(self) -> dict:
        dep_id = self._require_deposition()["id"]
        resp = self._session.post(
            f"{self.base}/deposit/depositions/{dep_id}/actions/publish",
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _require_deposition(self) -> dict:
        if self._deposition is None:
            raise RuntimeError("call create() before upload()/publish()")
        return self._deposition
