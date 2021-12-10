from contextlib import contextmanager
from functools import partial
from pathlib import Path
from subprocess import CalledProcessError, check_call
from typing import Optional

from cookiecutter.main import generate_files
from structlog import get_logger
from yaml import safe_load

from sscutils.naming import (
    CONFIG_DIR,
    IMPORTED_NAMESPACES_SCRIPTS_PATH,
    METADATA_DIR,
    SRC_PATH,
    ProjectConfigPaths,
    dataset_template_repo,
    project_template_repo,
)
from sscutils.utils import cd_into

logger = get_logger()

package_root = Path(__file__).parent.parent.parent
dogshow_root = package_root / "dogshow"
artifact_src_root = dogshow_root / "artifacts"
expectation_root = dogshow_root / "expectations"
csv_path = Path(artifact_src_root, "data").absolute().as_posix()


class DogshowContextCreator:
    def __init__(
        self,
        local_output_root: Path,
        git_remote_root: Optional[Path] = None,
        dvc_remotes: Optional[list] = None,
        git_user="John Doe",
        git_email="johndoe@example.com",
        dataset_template=dataset_template_repo,
        project_template=project_template_repo,
    ) -> None:

        self.local_root = local_output_root
        self.git_remote_root = Path(
            git_remote_root or self.local_root / "git-remotes"
        )
        self.csv_path = csv_path
        self.dvc_remotes = dvc_remotes or self._get_dvc_remotes(2)

        self.git_user = git_user
        self.git_email = git_email

        self.cc_context = {}

        project_ctx = partial(
            artifact_context,
            prefix="project",
            ctx=self,
            template_repo=project_template,
        )

        dataset_ctx = partial(
            project_ctx,
            prefix="dataset",
            template_repo=dataset_template,
        )

        self.dataset_a = dataset_ctx("a")
        self.dataset_b = dataset_ctx("b")
        self.project_a = project_ctx("a")
        self.project_b = project_ctx("b")

    def get_git_remote(self, name):
        remote = self.git_remote_root / f"dogshow-{name}"
        self._init_if_local(remote)
        self.cc_context[name.replace("-", "_") + "_repo"] = str(remote)
        return remote

    def init_git_repo(
        self,
        dirpath: str,
        to_tmp_branch=False,
        remote="",
        template_repo="",
    ):

        if template_repo:
            commands = [["git", "clone", template_repo, "."]]
        else:
            commands = [["git", "init"]]

        commands += [
            ["git", "config", "--local", "user.name", self.git_user],
            ["git", "config", "--local", "user.email", self.git_email],
        ]
        if remote:
            commands.append(
                [
                    "git",
                    "remote",
                    "set-url" if template_repo else "add",
                    "origin",
                    remote,
                ]
            )
        if to_tmp_branch:
            # useful if you intend to push to a local repo
            commands.append(["git", "checkout", "-b", "tmp-branch"])

        for comm in commands:
            check_call(comm, cwd=dirpath)

    def modify_project_a(self):
        "add one output for something"
        "add one dependency for something"
        "delete one of either"
        {"sex_match": "sex_pairing"}
        ["top_report.py", "__init__.py"]

    def _get_dvc_remotes(self, n):
        out = []
        for i in range(n):
            dvc_dir = self.local_root / "dvc-remotes" / f"remote-{i+1}"
            dvc_dir.mkdir(parents=True)
            out.append(dvc_dir.absolute().as_posix())
        return out

    def _init_if_local(self, repo_path):
        if not str(repo_path).startswith("git@"):
            repo_path.mkdir(parents=True, exist_ok=True)
            self.init_git_repo(
                repo_path,
                to_tmp_branch=True,
            )


@contextmanager
def artifact_context(
    suffix, prefix, ctx: DogshowContextCreator, template_repo
):
    name = "-".join([prefix, suffix])
    root_dir = ctx.local_root / name
    root_dir.mkdir()
    template_path = artifact_src_root / f"cc-{name}"
    git_remote = ctx.get_git_remote(name)

    ctx.init_git_repo(
        root_dir,
        remote=git_remote,
        template_repo=template_repo,
    )
    add_dvc_remotes(root_dir, ctx.dvc_remotes)
    generate_files(
        template_path,
        {"cookiecutter": {"artifact": name}, **ctx.cc_context},
        ctx.local_root,
        overwrite_if_exists=True,
    )
    with cd_into(root_dir):
        _add_readme(template_repo)
        _commit_changes(root_dir)
        yield partial(check_expectations, name=name)


def add_dvc_remotes(dirpath, remotes):
    "adds remotes with names remote1, remote2, ..."
    commands = []

    for i, remote in enumerate(remotes):
        # remote1 and remote2 names are set in the yamls
        commands.append(["dvc", "remote", "add", f"remote{i+1}", remote])

    if commands:
        commands += [
            ["git", "add", ".dvc"],
            ["git", "commit", "-m", "setup dvc"],
        ]

    for comm in commands:
        check_call(comm, cwd=dirpath)


def check_expectations(name: str):
    script_dir, meta_dir = [
        expectation_root / d / name for d in ["script", "metadata"]
    ]
    for meta_exp_file in meta_dir.glob("*.yaml"):
        _check_files(
            meta_exp_file, METADATA_DIR / meta_exp_file.name, safe_load
        )

    for script_file in script_dir.glob("*.py"):
        _check_files(
            script_file, IMPORTED_NAMESPACES_SCRIPTS_PATH / script_file.name
        )


def _check_files(exp_path, true_path, wrapper=str):
    logger.info("Validating", true=true_path, exp=exp_path)
    exp_obj = wrapper(exp_path.read_text())
    true_obj = wrapper(true_path.read_text())
    assert exp_obj == true_obj


def _commit_changes(dirpath):
    commands = []
    for addp in [
        SRC_PATH,
        METADATA_DIR,
        CONFIG_DIR,
        ProjectConfigPaths.PARAMS,
    ]:
        commands += [
            ["git", "add", addp.as_posix()],
            ["git", "commit", "-m", f"add {addp}"],
        ]
    commands += [
        ["git", "add", "*.py"],
        ["git", "commit", "-m", "add plus script"],
        ["git", "add", "*.md"],
        ["git", "commit", "-m", "add documentation"],
    ]
    for comm in commands:
        try:
            check_call(comm, cwd=dirpath)
        except CalledProcessError:
            pass


def _add_readme(template_repo):
    Path("README.md").write_text(
        f"test artifact for " f"[this]({template_repo}) template"
    )
