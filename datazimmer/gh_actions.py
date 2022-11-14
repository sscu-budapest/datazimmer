from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from .naming import (
    AUTH_HEX_ENV_VAR,
    AUTH_PASS_ENV_VAR,
    GIT_TOKEN_ENV_VAR,
    REQUIREMENTS_FILE,
    cli_run,
)

if TYPE_CHECKING:
    from .aswan_integration import DzAswan

_GHA = Path(".github", "workflows")


def write_action(dic, path: Path):
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(yaml.safe_dump(dic, sort_keys=False).replace("'on':", "on:"))


def write_project_cron(cron):
    from . import typer_commands as tc

    funs = tc.build_meta, tc.update, (tc.run, "--commit"), tc.publish_data
    cron_dic = _get_cron_dic(cron, "zimmer-project", *funs)
    write_action(cron_dic, _GHA / "zimmer_schedule.yml")


def write_aswan_crons(projects: list["DzAswan"]):
    from . import typer_commands as tc

    for project in projects:
        if not project.cron:
            continue
        funs = [(tc.run_aswan_project, f"--project={project.name}")]
        cron_dic = _get_cron_dic(project.cron, project.name, *funs)
        write_action(cron_dic, _GHA / f"asw_{project.name}_schedule.yml")


def write_book_actions(cron):
    write_action(_get_book_dic(cron), _GHA / "deploy.yml")


_env_keys = [AUTH_HEX_ENV_VAR, AUTH_PASS_ENV_VAR, GIT_TOKEN_ENV_VAR]
try:
    from aswan.depot import DEFAULT_REMOTE_ENV_VAR, HEX_ENV, PW_ENV

    _env_keys.extend([PW_ENV, HEX_ENV, DEFAULT_REMOTE_ENV_VAR])
except ImportError:
    pass  # pragma: no cover

_env = {k: r"${{ secrets." + k + r" }}" for k in _env_keys}


def _get_base_steps():
    instr = f"python -m pip install --upgrade pip; pip install -r {REQUIREMENTS_FILE}"
    uconfs = ['user.email "leo@dumbartonserum.com"', 'user.name "Leo Dumbarton"']
    confs = ["init.defaultBranch main", *uconfs]
    git_comm = ";".join([f"git config --global {c}" for c in confs])
    return [
        {"uses": "actions/checkout@v3"},
        {"uses": "actions/setup-python@v4", "with": {"python-version": "3.10"}},
        {"name": "Install dependencies", "run": instr},
        {"name": "Setup Git", "run": f"{git_comm};git pull --tags"},
    ]


def _get_jobs_dic(name, add_steps):
    return {name: {"runs-on": "ubuntu-latest", "steps": _get_base_steps() + add_steps}}


def _get_cron_dic(cron, name, *funs):
    step = {"name": f"Scheduled {name}", "env": _env, "run": cli_run(*funs)}
    return {
        "name": f"Scheduled {name}",
        "on": {"schedule": [{"cron": cron}]},
        "jobs": _get_jobs_dic(f"cron-run-{name}", [step]),
    }


def _get_book_dic(cron):
    from . import typer_commands as tc

    book_comm = cli_run(tc.load_explorer_data, tc.build_explorer)

    steps = [
        {"name": "Build the book", "run": book_comm, "env": _env},
        {
            "name": "GitHub Pages action",
            "uses": "peaceiris/actions-gh-pages@v3",
            "with": {
                "github_token": "${{ secrets.GITHUB_TOKEN }}",
                "publish_dir": "book/_build/html",
            },
        },
    ]
    return {
        "name": "Build and Deploy Book",
        "on": {"push": {"branches": ["main"]}, "schedule": [{"cron": cron}]},
        "jobs": _get_jobs_dic("build-and-deploy-book", steps),
    }
