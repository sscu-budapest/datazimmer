from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from itertools import groupby
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, List, Optional, Union

import boto3
import yaml
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup, Tag
from cookiecutter.main import cookiecutter
from cron_descriptor import ExpressionDescriptor
from sqlmermaid import get_mermaid

from .config_loading import Config, ImportedProject, ProjectEnv, RunConfig
from .full_auth import ZimmerAuth
from .get_runtime import get_runtime
from .gh_actions import write_book_actions
from .metadata.scrutable import ScruTable
from .naming import (
    DEFAULT_ENV_NAME,
    DEFAULT_REGISTRY,
    EXPLORE_CONF_PATH,
    SANDBOX_NAME,
    repo_link,
)
from .registry import Registry
from .sql.loader import tmp_constr
from .utils import camel_to_snake, gen_rmtree, reset_meta_module
from .validation_functions import sandbox_project

if TYPE_CHECKING:  # pragma: no cover
    from .pipeline_registry import PipelineRegistry
    from .project_runtime import ProjectRuntime

CC_DIR = repo_link("explorer-template")
BOOK_DIR = Path("book")
HOMES, DATASETS = [BOOK_DIR / sd for sd in ["homes", "datasets"]]


class S3Remote:
    # TODO: cover s3
    def __init__(self, rem_id):

        auth = ZimmerAuth().get_auth(rem_id)
        self.bucket = rem_id
        self.endpoint = auth.endpoint
        self.s3 = boto3.resource(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=auth.key,
            aws_secret_access_key=auth.secret,
        )

    def push(self, content, key):
        new_md5 = md5(content.encode("utf-8")).hexdigest()
        bucket = self.s3.Bucket(self.bucket)
        last = bucket.Object(key)
        try:
            if last.e_tag[1:-1] == new_md5:
                return last.last_modified
        except ClientError:
            pass
        return bucket.put_object(Body=content, Key=key).last_modified

    def full_link(self, key):
        if self.endpoint is None:
            return f"https://{self.bucket}.s3.amazonaws.com/{key}"
        return f"{self.endpoint}/{self.bucket}/{key}"


class LocalRemote:
    def __init__(self, root: Optional[str]) -> None:
        self.root = root or TemporaryDirectory().name
        Path(self.root).mkdir(exist_ok=True)

    def push(self, content, key):
        ppath = Path(self.root, key)
        ppath.parent.mkdir(exist_ok=True, parents=True)
        ppath.write_text(content)
        return datetime.now()

    def full_link(self, key):
        return f"file://{self.root}/{key}"


@dataclass
class ExplorerDataset:
    name: str
    project: str
    namespace: str
    tables: Optional[List[str]] = None
    env: str = DEFAULT_ENV_NAME
    version: str = ""
    minimal: bool = False
    erd: bool = True
    cc_context: dict = field(init=False, default_factory=dict)
    to_write: list = field(init=False, default_factory=list)

    def set_from_project(self, remote: S3Remote, runtime: "ProjectRuntime", book_root):
        meta = runtime.metadata_dic[self.project]
        ns_meta = meta.namespaces[self.namespace]
        scrutable_list = ns_meta.tables
        if self.tables:
            scrutable_list = [t for t in ns_meta.tables if t.name in self.tables]
        self.cc_context.update(
            {
                "project_url": meta.uri,
                "slug": self._slug,
                "name": self.name,
                "source_urls": ns_meta.source_urls,
                "tables": [],
                "n_tables": len(scrutable_list),
                "update_str": self._get_cron_desc(runtime.pipereg),
                "erd_mermaid": self._get_erd(scrutable_list),
            }
        )
        with RunConfig(read_env=self.env):
            for table in scrutable_list:
                self.cc_context["tables"].append(
                    self._get_table_cc(table, remote, book_root)
                )

    def dump(self):
        for path, text in self.to_write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)

    def id_(self):
        return (self.project, self.v_str)

    @property
    def v_str(self):
        return f"=={self.version}" if self.version else ""

    def _get_table_cc(self, scrutable: ScruTable, remote: S3Remote, book_root):
        # slow import...
        from pandas_profiling import ProfileReport

        df = scrutable.get_full_df(env=self.env)
        if scrutable.index_cols:
            df = df.reset_index()
        csv_str = df.to_csv(index=None)
        profile_str = _shorten(ProfileReport(df, minimal=self.minimal))
        name = scrutable.name

        profile_key = f"{self._slug}/{name}-profile.html"
        csv_key = f"{self._slug}/{name}.csv"
        csv_path = book_root / DATASETS / self._slug / f"{name}.csv"
        remote.push(profile_str, profile_key)
        mod_date = remote.push(csv_str, csv_key)
        self.to_write.append((csv_path, csv_str))
        return {
            "name": _title(scrutable.name),
            "slug": scrutable.name,
            "profile_url": remote.full_link(profile_key),
            "update_date": mod_date.isoformat(" ", "minutes")[:16],
            "n_cols": df.shape[1],
            "n_rows": df.shape[0],
            "csv_url": remote.full_link(csv_key),
            "csv_filename": csv_path.name,
            "csv_filesize": _get_filesize(csv_str),
        }

    def _get_cron_desc(self, pipereg: "PipelineRegistry"):
        raw_cron = pipereg.get_external_cron(self.project, self.namespace)
        if not raw_cron:
            return ""
        return (
            ExpressionDescriptor(
                raw_cron, locale_code="en", throw_exception_on_parse_error=False
            )
            .get_description()
            .lower()
        )

    def _get_erd(self, scrutables: List[ScruTable]):
        if not self.erd:
            return ""
        with tmp_constr() as constr:
            mm_str = get_mermaid(constr)
        return mm_str

    @property
    def _slug(self):
        return self.name.lower().replace(" ", "_")


@dataclass
class ExplorerContext:
    datasets: List[ExplorerDataset]
    remote: Union[S3Remote, LocalRemote]
    registry: str = DEFAULT_REGISTRY
    book_root: Path = field(init=False, default_factory=Path.cwd)
    cc_template: str = CC_DIR

    def load_data(self):
        # to avoid dependency clashes, it is slower but simpler
        # to setup a sandbox one by one
        with sandbox_project(self.registry):
            # TODO: avoid recursive data imports here
            for (pname, pv), ds_gen in groupby(self.datasets, ExplorerDataset.id_):
                self._set_from_project([*ds_gen], pname, pv)

    @classmethod
    def load(cls):
        conf_dic = yaml.safe_load(EXPLORE_CONF_PATH.read_text())
        tdirs = [ExplorerDataset(**tdkw) for tdkw in conf_dic.pop("datasets")]
        remote_raw = conf_dic.pop("remote", "")
        if remote_raw and (not remote_raw.startswith("/")):
            remote = S3Remote(remote_raw)
        else:
            remote = LocalRemote(remote_raw)
        return cls(tdirs, remote, **conf_dic)

    def _set_from_project(
        self, datasets: List[ExplorerDataset], project_name, project_v
    ):
        envs = set([d.env for d in datasets])
        dnss = [*set([d.namespace for d in datasets])]
        test_conf = Config(
            name=SANDBOX_NAME,
            registry=self.registry,
            version="v0.0",
            imported_projects=[ImportedProject(project_name, dnss, version=project_v)],
            envs=[ProjectEnv(env, import_envs={project_name: env}) for env in envs],
        )
        test_conf.dump()
        test_reg = Registry(test_conf, True)
        test_reg.update()
        test_reg.full_build()
        # TODO: cleanup
        runtime = get_runtime(True)
        runtime.load_all_data()
        for dataset in datasets:
            dataset.set_from_project(self.remote, runtime, self.book_root)
        reset_meta_module()
        test_reg.purge()


def build_explorer(cron: str = "0 15 * * *"):
    write_book_actions(cron)
    load_explorer_data()


def load_explorer_data():
    ZimmerAuth().dump_dvc(local=False)
    ctx = ExplorerContext.load()
    ctx.load_data()
    cc_datasets = {"datasets": {"datasets": [ds.cc_context for ds in ctx.datasets]}}
    cc_dic = {"cookiecutter": {"slug": BOOK_DIR.as_posix()}, **cc_datasets}
    with save_edits():
        for ds in ctx.datasets:
            cookiecutter(
                ctx.cc_template,
                no_input=True,
                extra_context={**cc_dic, "main": ds.cc_context},
                overwrite_if_exists=True,
            )
            ds.dump()


@contextmanager
def save_edits():
    outs = []
    paths = [
        *DATASETS.glob("**/*.ipynb"),
        *BOOK_DIR.glob("*.txt"),
        # *BOOK_DIR.glob("*.md"),
        *HOMES.glob("**/*.md"),
    ]
    for _save_path in paths:
        outs.append((_save_path, _save_path.read_text()))
    gen_rmtree(BOOK_DIR)
    yield
    for _path, nbstr in outs:
        if _path.parent.exists():
            _path.write_text(nbstr)


def _shorten(profile):
    soup = BeautifulSoup(profile.to_html(), "html5lib")
    bs_root = "https://cdn.jsdelivr.net/npm/bootstrap@3.3.7/dist"

    # "crossorigin": "anonymous"
    css_link = f"{bs_root}/css/bootstrap.min.css"
    jq_link = "https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"
    slink = Tag(name="link", attrs={"rel": "stylesheet", "href": css_link})
    bs_script = Tag(name="script", attrs={"src": f"{bs_root}/js/bootstrap.min.js"})
    jq_script = Tag(name="script", attrs={"src": jq_link})

    soup.find("style").decompose()
    soup.find("head").append(slink)

    for scrtag in soup.find_all("script"):
        if "/*! jQuery" in "".join(scrtag.contents):
            scrtag.decompose()
            break

    soup.find("body").insert(-1, bs_script)
    soup.find("body").insert(-2, jq_script)
    return str(soup)


def _title(s):
    return camel_to_snake(s).replace("_", " ").title()


def _get_filesize(file_str):
    kb_size = len(file_str.encode("utf-8")) / 2**10
    if kb_size > 1000:
        return f"{kb_size / 2 ** 10:0.2f} MB"
    return f"{kb_size:0.2f} kB"
