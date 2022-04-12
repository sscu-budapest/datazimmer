import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import toml
from structlog import get_logger

from .naming import AUTH_ENV_VAR

dvc_local = Path(".dvc", "config.local")
_DVC_FRAME = """
['remote "{name}"']
    url = s3://{name}
    access_key_id = {key}
    secret_access_key = {s}"""


@dataclass
class Auth:
    key: str
    secret: str
    endpoint: Optional[str] = None


@dataclass
class Remote:
    name: str
    auth: Auth

    def to_dvc_conf(self):
        s = _DVC_FRAME.format(name=self.name, key=self.auth.key, s=self.auth.secret)
        if self.auth.endpoint:
            s += f"    endpointurl = {self.auth.endpoint}"
        return s


class ZimmerAuth:
    def __init__(self) -> None:
        dic = toml.loads(os.environ.get(AUTH_ENV_VAR, ""))
        auths = {k: Auth(**v) for k, v in dic.pop("keys", {}).items()}
        self.remotes = {}
        for k, r in dic.items():
            self.remotes[k] = Remote(k, auths[r["key"]])

    def get_auth(self, remote_id: str) -> Auth:
        return self.remotes[remote_id].auth

    def dump_dvc(self):
        if dvc_local.exists():
            return
        get_logger().info("writing dvc auth", remotes=[*self.remotes.keys()])
        dvc_local.write_text(
            "\n".join([rem.to_dvc_conf() for rem in self.remotes.values()])
        )
