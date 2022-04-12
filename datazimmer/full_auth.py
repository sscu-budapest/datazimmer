import os
from dataclasses import dataclass
from typing import Optional

import toml
from dvc.config import Config
from structlog import get_logger

from .naming import AUTH_ENV_VAR


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
        d = {
            "url": f"s3://{self.name}",
            "access_key_id": self.auth.key,
            "secret_access_key": self.auth.secret,
        }
        if self.auth.endpoint:
            d["endpointurl"] = self.auth.endpoint
        return d


class ZimmerAuth:
    def __init__(self) -> None:
        dic = toml.loads(os.environ.get(AUTH_ENV_VAR, ""))
        auths = {k: Auth(**v) for k, v in dic.pop("keys", {}).items()}
        self.remotes = {}
        for k, r in dic.items():
            self.remotes[k] = Remote(k, auths[r["key"]])

    def get_auth(self, remote_id: str) -> Auth:
        return self.remotes[remote_id].auth

    def dump_dvc(self, local=True):
        conf = Config()
        get_logger().info("writing dvc auth", remotes=[*self.remotes.keys()])
        with conf.edit("local" if local else "global") as ced:
            ced["remote"] = {k: v.to_dvc_conf() for k, v in self.remotes.items()}
