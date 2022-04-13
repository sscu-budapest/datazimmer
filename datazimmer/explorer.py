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
from .full_auth import ZimmerAuth
from .get_runtime import get_runtime
from .gh_actions import write_book_actions
from .module_tree import load_scrutable
from .naming import (
    DEFAULT_ENV_NAME,
    DEFAULT_REGISTRY,
    EXPLORE_CONF_PATH,
    SANDBOX_NAME,
    repo_link,
)
from .registry import Registry
from .utils import reset_meta_module
from .validation_functions import sandbox_project

CC_DIR = repo_link("explorer-template")
BOOK_DIR = Path("book")
HOMES, TABLES, HEADERS = [BOOK_DIR / sd for sd in ["homes", "tables", "headers"]]


class S3Remote:
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
        Path(self.root, key).write_text(content)
        return datetime.now()

    def full_link(self, key):
        return f"file://{self.root}/{key}"


@dataclass
class TableTemplate:
    filepath: str
    out_dir: Path
    nested: bool
    jinja: Template = field(init=False)

    def __post_init__(self):
        t_path = BOOK_DIR / self.filepath
        self.jinja = Template(t_path.read_text())
        t_path.unlink()

    def dump(self, table_obj: "TableDir", full_table_meta):
        out_str = self.jinja.render(**full_table_meta)
        if self.nested:
            out_path = (
                self.out_dir / table_obj.slug / self.filepath.replace(".jinja", "")
            )
        else:
            out_path = self.out_dir / f"{table_obj.slug}.{self.filepath.split('.')[-2]}"
        out_path.parent.mkdir(exist_ok=True, parents=True)
        out_path.write_text(out_str)


@dataclass
class TableDir:
    name: str
    project: str
    namespace: str
    table: str
    env: str = DEFAULT_ENV_NAME
    version: str = ""
    df: pd.DataFrame = field(init=False, default=None)
    profile_url: str = field(init=False, default=None)
    entity: str = field(init=False, default=None)
    project_url: str = field(init=False, default=None)

    def set_meta(self, df, remote: S3Remote, entity, project_url):
        self.df = df
        self.profile_url = remote.full_link(self.profile_key)
        self.entity = entity
        self.project_url = project_url

    def id_(self):
        return (self.project, self.v_str)

    def dump(self, remote: S3Remote, templates: List[TableTemplate], minimal):
        # slow import...
        from pandas_profiling import ProfileReport

        csv_path = TABLES / self.slug / f"{self.slug}.csv"
        csv = self.df.to_csv(index=any(self.df.index.names))
        remote.push(_shorten(ProfileReport(self.df, minimal=minimal)), self.profile_key)
        mod_date = remote.push(csv, csv_path.name)

        meta = self.get_full_meta(mod_date, remote, csv_path.name)
        for template in templates:
            template.dump(self, meta)
        csv_path.write_text(csv)

    def get_full_meta(self, mod_date, remote, csv_filename):
        # to all the templates
        return {
            "name": self.name,
            "slug": self.slug,
            "update_date": mod_date.isoformat(" ", "minutes")[:16],
            "n_cols": self.df.shape[1],
            "n_rows": self.df.shape[0],
            "entity": self.entity,
            "csv_url": remote.full_link(csv_filename),
            "csv_filename": csv_filename,
            "project_url": self.project_url
        }

    @property
    def v_str(self):
        return f"=={self.version}" if self.version else ""

    @property
    def slug(self):
        return self.name.lower().replace(" ", "-")

    @property
    def profile_key(self):
        return f"{self.slug}-profile.html"


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
        templates = [
            TableTemplate("home.md.jinja", HOMES, False),
            TableTemplate("intro.ipynb.jinja", TABLES, True),
            TableTemplate("header.md.jinja", HEADERS, False),
        ]
        for tdir in self.tables:
            tdir.dump(self.remote, templates, minimal)

    @classmethod
    def load(cls):
        conf_dic = yaml.safe_load(EXPLORE_CONF_PATH.read_text())
        tdirs = [TableDir(**tdkw) for tdkw in conf_dic.pop("tables")]
        remote_raw = conf_dic.pop("remote", "")
        if remote_raw and (not remote_raw.startswith("/")):
            remote = S3Remote(remote_raw)
        else:
            remote = LocalRemote(remote_raw)
        return cls(tdirs, remote, **conf_dic)

    @property
    def table_cc_dicts(self):
        return [{"slug": t.slug, "profile_url": t.profile_url} for t in self.tables]

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
            runtime = get_runtime(True)
            runtime.load_all_data()
            for tdir in _dirs:
                scrutable = load_scrutable(tdir.project, tdir.namespace, tdir.table)
                uri = runtime.ext_metas[tdir.project].uri
                with RunConfig(read_env=tdir.env):
                    entity = scrutable.subject.__name__
                    tdir.set_meta(scrutable.get_full_df(), self.remote, entity, uri)
            reset_meta_module()


def build_explorer(cron: str = "0 15 * * *", minimal: bool = False):
    write_book_actions(cron)
    load_explorer_data(minimal)


def load_explorer_data(minimal: bool = False):
    ZimmerAuth().dump_dvc(local=False)
    ctx = ExplorerContext.load()
    ctx.set_dfs()
    cc_tables = {"tables": {"tables": ctx.table_cc_dicts}}
    cc_dic = {"cookiecutter": {"slug": BOOK_DIR.as_posix()}, **cc_tables}
    with save_notebooks():
        cookiecutter(CC_DIR, no_input=True, extra_context=cc_dic)
        ctx.dump_tables(minimal)


@contextmanager
def save_notebooks():
    outs = []
    paths = [
        *TABLES.glob("**/*.ipynb"),
        *BOOK_DIR.glob("*.txt"),
        *HOMES.glob("**/*.md"),
    ]
    for _save_path in paths:
        outs.append((_save_path, _save_path.read_text()))
    rmtree(BOOK_DIR, ignore_errors=True)
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
