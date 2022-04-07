import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from itertools import groupby
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import List, Optional, Union

import boto3
import pandas as pd
import yaml
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup, Tag
from cookiecutter.main import cookiecutter
from jinja2 import Template

from .config_loading import Config, ImportedProject, ProjectEnv, RunConfig
from .get_runtime import get_runtime
from .gh_actions import write_book_actions
from .module_tree import load_scrutable
from .naming import (
    DEFAULT_REGISTRY,
    EXPLORE_AK_ENV,
    EXPLORE_CONF_PATH,
    EXPLORE_SECRET_ENV,
    SANDBOX_NAME,
    repo_link,
)
from .registry import Registry
from .utils import reset_meta_module
from .validation_functions import sandbox_project

CC_DIR = repo_link("explorer-template")
BOOK_DIR = Path("book")
HOME_JINJA = BOOK_DIR / "home.md.jinja"
NB_JINJA = BOOK_DIR / "sneak-peek.ipynb.jinja"
HOMES, TABLES = [BOOK_DIR / sd for sd in ["homes", "tables"]]


@dataclass
class S3Remote:
    bucket: str
    endpoint: Optional[str] = None
    access_key: str = field(init=False)
    secret_key: str = field(init=False)
    s3: boto3.resources.factory.ServiceResource = field(init=False)

    def __post_init__(self):

        self.access_key = os.environ.get(EXPLORE_AK_ENV)
        self.secret_key = os.environ.get(EXPLORE_SECRET_ENV)

        self.s3 = boto3.resource(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
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
        return f"{self.endpoint}/{self.bucket}/{key}"


class LocalRemote:
    def __init__(self, root: Optional[str]) -> None:
        self.root = root or TemporaryDirectory().name
        Path(self.root).mkdir(exist_ok=True)

    def push(self, content, key):
        Path(self.root, key).write_text(content)
        return datetime.now()

    def full_link(self, key):
        return f"file://{self.root}/{key}"


@dataclass
class TableDir:
    name: str
    project: str
    namespace: str
    env: str
    table: str
    description: str = ""
    version: str = ""
    df: pd.DataFrame = field(init=False, default=None)

    def set_df(self, df):
        self.df = df

    def id_(self):
        return (self.project, self.v_str)

    def dump(self, remote: S3Remote, template, nb_template, minimal):
        # slow import...
        from pandas_profiling import ProfileReport

        profile_key = f"{self.slug}-profile.html"
        csv_path = TABLES / self.slug / f"{self.slug}.csv"
        csv_path.parent.mkdir(exist_ok=True)
        remote_csv = remote.full_link(csv_path.name)

        csv_str = self.df.to_csv(index=any(self.df.index.names))
        csv_path.write_text(csv_str)
        remote.push(_shorten(ProfileReport(self.df, minimal=minimal)), profile_key)
        index_md = template.render(
            name=self.name,
            description=self._get_description(),
            profile_url=remote.full_link(profile_key),
            csv_url=remote_csv,
            update_date=remote.push(csv_str, csv_path.name).isoformat(" ", "minutes"),
        )
        (HOMES / f"{self.slug}.md").write_text(index_md)
        nb_str = nb_template.render(csv_filename=csv_path.name)
        (csv_path.parent / "intro.ipynb").write_text(nb_str)

    @property
    def v_str(self):
        return f"=={self.version}" if self.version else ""

    @property
    def slug(self):
        return self.name.lower().replace(" ", "-")

    def _get_description(self):
        return self.description or f"{' × '.join(map(str, self.df.shape))} table"


@dataclass
class ExplorerContext:
    tables: List[TableDir]
    remote: Union[S3Remote, LocalRemote]
    registry: str = DEFAULT_REGISTRY

    def set_dfs(self):
        # to avoid dependency clashes, it is slower but simpler
        # to setup a sandbox one by one
        with sandbox_project():
            # TODO: avoid recursive data imports here
            self._set_in_box()

    def dump_tables(self, minimal=False):
        template = Template(HOME_JINJA.read_text())
        nb_template = Template(NB_JINJA.read_text())
        for tdir in self.tables:
            tdir.dump(self.remote, template, nb_template, minimal)

    @classmethod
    def load(cls):
        conf_dic = yaml.safe_load(EXPLORE_CONF_PATH.read_text())
        tdirs = [TableDir(**tdkw) for tdkw in conf_dic.pop("tables")]
        remote_raw = conf_dic.pop("remote", None)
        if isinstance(remote_raw, dict):
            remote = S3Remote(**remote_raw)
        else:
            remote = LocalRemote(remote_raw)
        return cls(tdirs, remote, **conf_dic)

    @property
    def table_slugs(self):
        return [t.slug for t in self.tables]

    def _set_in_box(self):
        for (aname, av), dirgen in groupby(self.tables, TableDir.id_):
            _dirs = [*dirgen]
            envs = set([d.env for d in _dirs])
            dnss = [*set([d.namespace for d in _dirs])]
            test_conf = Config(
                name=SANDBOX_NAME,
                registry=self.registry,
                version="v0.0",
                imported_projects=[ImportedProject(aname, dnss, version=av)],
                envs=[ProjectEnv(env, import_envs={aname: env}) for env in envs],
            )
            test_conf.dump()
            test_reg = Registry(test_conf, True)
            test_reg.update()
            test_reg.full_build()
            # TODO: cleanup
            get_runtime(True).load_all_data()
            for tdir in _dirs:
                scrutable = load_scrutable(tdir.project, tdir.namespace, tdir.table)
                with RunConfig(read_env=tdir.env):
                    tdir.set_df(scrutable.get_full_df())
            reset_meta_module()


def build_explorer(cron: str = "0 15 * * *", minimal: bool = False):
    write_book_actions(cron)
    load_explorer_data(minimal)


def load_explorer_data(minimal: bool = False):
    ctx = ExplorerContext.load()
    ctx.set_dfs()
    cc_context = {"tables": {"tables": ctx.table_slugs}}
    with save_notebooks():
        cookiecutter(
            CC_DIR,
            no_input=True,
            extra_context={"cookiecutter": {"slug": BOOK_DIR.as_posix()}, **cc_context},
            overwrite_if_exists=False,
            skip_if_file_exists=True,
        )
        for sd in [HOMES, TABLES]:
            sd.mkdir(exist_ok=True, parents=True)
        ctx.dump_tables(minimal)
        HOME_JINJA.unlink()
        NB_JINJA.unlink()


@contextmanager
def save_notebooks():
    outs = []
    for nbp in TABLES.glob("**/*ipynb"):
        outs.append((nbp, nbp.read_text()))
    rmtree(BOOK_DIR, ignore_errors=True)
    yield
    for nbp, nbstr in outs:
        if nbp.parent.exists():
            nbp.write_text(nbstr)


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
