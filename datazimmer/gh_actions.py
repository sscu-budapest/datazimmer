from pathlib import Path

import yaml

from .naming import CLI, CRON_ENV_VAR, EXPLORE_AK_ENV, EXPLORE_SECRET_ENV

_GHA_PATH = Path(".github", "workflows")


def write_action(dic, path: Path):
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(yaml.safe_dump(dic, sort_keys=False).replace("'on':", "on:"))


def write_cron_actions(cron_exprs):
    write_action(_get_cron_dic(cron_exprs), _GHA_PATH / "zimmer_crons.yml")


def write_book_actions(cron):
    write_action(_get_book_dic(cron), _GHA_PATH / "deploy.yml")


cron_comm = f"{CLI} build-meta && {CLI} run-cronjobs && {CLI} publish-data"
book_comm = f"{CLI} load-explorer-data && jupyter-book build book"

_env = {
    "AWS_ACCESS_KEY_ID": r"${{ secrets.AWS_ACCESS_KEY_ID }}",
    "AWS_SECRET_ACCESS_KEY": r"${{ secrets.AWS_SECRET_ACCESS_KEY }}",
}


def _get_base(req_file):
    instr = f"python -m pip install --upgrade pip; pip install -r {req_file}"
    uconfs = ['user.email "leo@dumbartonserum.com"', 'user.name "Leo Dumbarton"']
    confs = ["init.defaultBranch main", *uconfs]
    git_comm = ";".join([f"git config --global {c}" for c in confs])
    return [
        {"uses": "actions/checkout@v2"},
        {"uses": "actions/setup-python@v1", "with": {"python-version": "3.x"}},
        {"name": "Install dependencies", "run": instr},
        {"name": "Setup Git", "run": git_comm},
    ]


def _get_cron_dic(cron_exprs):
    return {
        "name": "Scheduled Run",
        "on": {"schedule": [{"cron": cexspr} for cexspr in cron_exprs]},
        "jobs": {
            "cron_run": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *_get_base("requirements.txt"),
                    {
                        "name": "Setup Auth",
                        "env": {"DVC_LOCAL_CONF": r"${{ secrets.DVC_LOCAL }}"},
                        "run": ('echo "$DVC_LOCAL_CONF" > .dvc/config.local'),
                    },
                    {
                        "name": "Bump crons",
                        "env": {CRON_ENV_VAR: r"${{ github.event.schedule }}", **_env},
                        "run": cron_comm,
                    },
                ],
            }
        },
    }


def _get_book_dic(cron):
    return {
        "name": "Build and Deploy Book",
        "on": {"push": {"branches": ["main"], "schedule": [{"cron": cron}]}},
        "jobs": {
            "build-and-deploy-book": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *_get_base("book/requirements.txt"),
                    {
                        "name": "Build the book",
                        "run": book_comm,
                        "env": {
                            EXPLORE_AK_ENV: r"${{ secrets.EXPLORE_AK }}",
                            EXPLORE_SECRET_ENV: r"${{ secrets.EXPLORE_SECRET }}",
                            **_env,
                        },
                    },
                    {
                        "name": "GitHub Pages action",
                        "uses": "peaceiris/actions-gh-pages@v3",
                        "with": {
                            "github_token": "${{ secrets.GITHUB_TOKEN }}",
                            "publish_dir": "book/_build/html",
                        },
                    },
                ],
            }
        },
    }
