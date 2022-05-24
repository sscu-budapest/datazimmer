from pathlib import Path

import yaml

from .naming import (
    AUTH_ENV_VAR,
    CLI,
    CRON_ENV_VAR,
    GIT_TOKEN_ENV_VAR,
    REQUIREMENTS_FILE,
)

_GHA_PATH = Path(".github", "workflows")


def write_action(dic, path: Path):
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(yaml.safe_dump(dic, sort_keys=False).replace("'on':", "on:"))


def write_cron_actions(cron_exprs):
    write_action(_get_cron_dic(cron_exprs), _GHA_PATH / "zimmer_crons.yml")


def write_book_actions(cron):
    write_action(_get_book_dic(cron), _GHA_PATH / "deploy.yml")


cron_comm = f"{CLI} build-meta && dvc pull && {CLI} run-cronjobs && {CLI} publish-data"
book_comm = f"{CLI} load-explorer-data && {CLI} build-explorer"

_env_keys = [AUTH_ENV_VAR, GIT_TOKEN_ENV_VAR]
_env = {k: r"${{ secrets." + k + r" }}" for k in _env_keys}


def _get_base_steps():
    instr = f"python -m pip install --upgrade pip; pip install -r {REQUIREMENTS_FILE}"
    uconfs = ['user.email "leo@dumbartonserum.com"', 'user.name "Leo Dumbarton"']
    confs = ["init.defaultBranch main", *uconfs]
    git_comm = ";".join([f"git config --global {c}" for c in confs])
    return [
        {"uses": "actions/checkout@v3"},
        {"uses": "actions/setup-python@v1", "with": {"python-version": "3.x"}},
        {"name": "Install dependencies", "run": instr},
        {"name": "Setup Git", "run": f"{git_comm};git pull --tags"},
    ]


def _get_jobs_dic(name, add_steps):
    return {name: {"runs-on": "ubuntu-latest", "steps": _get_base_steps() + add_steps}}


def _get_cron_dic(cron_exprs):
    step = {
        "name": "Bump crons",
        "env": {CRON_ENV_VAR: r"${{ github.event.schedule }}", **_env},
        "run": cron_comm,
    }
    return {
        "name": "Scheduled Run",
        "on": {"schedule": [{"cron": cexspr} for cexspr in cron_exprs]},
        "jobs": _get_jobs_dic("cron-run", [step]),
    }


def _get_book_dic(cron):
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
