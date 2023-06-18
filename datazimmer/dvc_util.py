import venv
from pathlib import Path
from subprocess import check_output

import yaml
from structlog import get_logger

from .naming import BASE_CONF_PATH

logger = get_logger("dvc-util")

DVC_ENV = Path.home() / ".dvc-env"
DVC_ENV_EXC = DVC_ENV / "bin" / "python"


def setup_dvc(update: bool = False):
    venv.create(DVC_ENV, system_site_packages=True)
    uarg = ("-U",) if update else ()
    _erun("-m", "pip", "install", *uarg, "dvc[ssh,s3]")


def import_dvc(uri, path, out, rev=None, no_exec=False):
    comm = ["import", "-o", out]
    if rev:
        comm += ["--rev", rev]
    if no_exec:
        comm.append("--no-exec")
    run_dvc(*comm, uri, path)


def pull():
    run_dvc("pull")


def push(targets: list, remote: str):
    run_dvc("push", "-r", remote, *targets)


def add(file):
    run_dvc("add", file)


def list_stages():
    if not Path("dvc.yaml").exists():
        return []
    return run_dvc("stage", "list", "--name-only").strip().split()


def remove(stage_name):
    run_dvc("remove", stage_name, "--outs")


def reproduce(targets):
    return run_dvc("repro", "--pull", *(targets or [])).strip()


def add_stage(cmd, name, outs_no_cache, outs, outs_persist, deps, params):
    comms = ["stage", "add", "-n", name, "-f"]
    for k, v in {
        "-o": outs,
        "-O": outs_no_cache,
        "--outs-persist": outs_persist,
        "-d": deps,
        "-p": params,
    }.items():
        for e in v or []:
            comms += [k, e]

    run_dvc(*comms, cmd)


def get_locked_param(stage_name, param):
    lock_file = Path("dvc.lock")
    if not lock_file.exists():
        return
    return (
        yaml.safe_load(lock_file.read_text())
        .get("stages", {})
        .get(stage_name, {})
        .get("params", {})
        .get(BASE_CONF_PATH.as_posix(), {})
        .get(param)
    )


def get_default_remote():
    return run_dvc("config", "core.remote").strip() or None


def run_dvc(*comm):
    logger.info("running dvc command", comm=comm)
    return _erun("-m", "dvc", *comm)


def _erun(*comm):
    return check_output([DVC_ENV_EXC.as_posix(), *comm]).decode()
