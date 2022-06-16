import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import toml
from dvc.config import Config
from paramiko.config import SSHConfig
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
    def __init__(self, au_dic=None) -> None:
        dic = au_dic or toml.loads(os.environ.get(AUTH_ENV_VAR, ""))

        def _pop(key):
            return dic.pop(key, {})

        auths = {k: Auth(**v) for k, v in _pop("keys").items()}
        self._ssh_hosts = _pop("ssh")
        self._rsa_keys = _pop("rsa_keys")

        self.remotes: Dict[str, Remote] = {}
        self._ssh_remotes = {}
        _prefix = f"ssh://{socket.gethostname()}"

        for k, r in dic.items():
            if "key" in r.keys():
                self.remotes[k] = Remote(k, auths[r["key"]])
            if "url" in r.keys():
                self._ssh_remotes[k] = {"url": r["url"].replace(_prefix, "")}

    def get_auth(self, remote_id: str) -> Auth:
        return self.remotes[remote_id].auth

    def dump_dvc(self, local=True):
        conf = Config()
        get_logger().info("writing dvc auth", remotes=[*self.remotes.keys()])
        with conf.edit("local" if local else "global") as ced:
            ced["remote"] = {k: v.to_dvc_conf() for k, v in self.remotes.items()}
            ced["remote"].update(self._ssh_remotes)

        self._dump_ssh_conf()

    def _dump_ssh_conf(self, root=Path.home()):
        ssh_dir = root / ".ssh"
        ssh_conf_path = ssh_dir / "config"
        if not ssh_conf_path.exists():
            return
        ssh_config = SSHConfig.from_path(ssh_conf_path)
        conf_strings = [ssh_conf_path.read_text()]
        for hostname, dic in self._ssh_hosts.items():
            if len(ssh_config.lookup(hostname)) > 1:
                continue
            keyname = dic.pop("key")
            key_path = (ssh_dir / f"dz-key-{keyname}").absolute()
            key_path.write_text(self._rsa_keys[keyname])
            key_path.chmod(0o600)
            dic["IdentityFile"] = key_path.as_posix()
            conf_elems = [f"Host {hostname}", *[f"{k} {v}" for k, v in dic.items()]]
            conf_strings.append("\n\t".join(conf_elems))
        ssh_conf_path.write_text("\n\n".join(conf_strings))
