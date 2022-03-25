from pathlib import Path

import yaml

from .naming import CRON_ENV_VAR

_GHA_PATH = Path(".github", "workflows", "zimmer_crons.yml")


def write_actions(cron_exprs):
    _GHA_PATH.parent.mkdir(exist_ok=True, parents=True)
    _s = yaml.safe_dump(_get_dic(cron_exprs), sort_keys=False).replace("'on':", "on:")
    _GHA_PATH.write_text(_s)


def _get_dic(cron_exprs):
    return {
        "name": "Scheduled Run",
        "on": {"schedule": [{"cron": cexspr} for cexspr in cron_exprs]},
        "jobs": {
            "cron_run": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    {"uses": "actions/checkout@v2"},
                    {
                        "uses": "actions/setup-python@v1",
                        "with": {"python-version": "3.x"},
                    },
                    {
                        "name": "Install dependencies",
                        "run": (
                            "python -m pip install --upgrade pip;"
                            "pip install -r requirements.txt"
                        ),
                    },
                    {
                        "name": "Setup Git",
                        "run": (
                            "git config --global init.defaultBranch main;"
                            'git config --global user.email "leo@dumbartonserum.com";'
                            'git config --global user.name "Leo Dumbarton"'
                        ),
                    },
                    {
                        "name": "Setup Auth",
                        "env": {"DVC_LOCAL_CONF": r"${{ secrets.DVC_LOCAL }}"},
                        "run": ('echo "$DVC_LOCAL_CONF" > .dvc/config.local'),
                    },
                    {
                        "name": "Bump crons",
                        "env": {
                            CRON_ENV_VAR: r"${{ github.event.schedule }}",
                            "ZIMMER_REGISTRY": r"${{ secrets.ZIMMER_REGISTRY }}",
                        },
                        "run": "inv build && inv run-cronjobs && inv publish-data",
                    },
                ],
            }
        },
    }
