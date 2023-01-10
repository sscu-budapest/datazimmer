import base64
import json
import multiprocessing as mp
import re
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
from itertools import groupby
from pathlib import Path
from queue import Queue
from shutil import copytree
from subprocess import check_call
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd
import yaml
from botocore.exceptions import ClientError
from cookiecutter.main import cookiecutter
from sqlmermaid import get_mermaid

from .config_loading import Config, ImportedProject, ProjectEnv, get_full_auth
from .get_runtime import get_runtime
from .gh_actions import write_book_actions
from .metadata.scrutable import ScruTable
from .naming import (
    DEFAULT_ENV_NAME,
    DEFAULT_REGISTRY,
    EXPLORE_CONF_PATH,
    META_MODULE_NAME,
    PACKAGE_NAME,
    REQUIREMENTS_FILE,
    SANDBOX_NAME,
    repo_link,
    to_mod_name,
)
from .nb_generator import get_nb_string
from .registry import Registry
from .sql.loader import SqlFilter, tmp_constr
from .utils import camel_to_snake, gen_rmtree
from .validation_functions import sandbox_project

if TYPE_CHECKING:  # pragma: no cover
    from .project_runtime import ProjectRuntime

CC_DIR = repo_link("explorer-template")
BOOK_DIR = Path("book")
SETUP_DIR = Path("dec-setup")
CC_BASE_FILE = SETUP_DIR / "cc_base.yaml"
DATASETS = BOOK_DIR / "datasets"


class S3Remote:
    def __init__(self, remote_id):

        z_auth = get_full_auth()
        self.bucket = z_auth.get_boto_bucket(remote_id)
        auth = z_auth.get_auth(remote_id)
        self.bucket_name = remote_id
        self.endpoint = auth.endpoint

    def push(self, content: bytes, key):
        new_md5 = md5(content).hexdigest()
        last = self.bucket.Object(key)
        try:
            if last.e_tag[1:-1] == new_md5:
                return last.last_modified
        except ClientError:
            pass
        return self.bucket.put_object(Body=content, Key=key).last_modified

    def full_link(self, key):
        if self.endpoint is None:
            return f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
        return f"{self.endpoint}/{self.bucket_name}/{key}"


class LocalRemote:
    def __init__(self, root: Optional[str]) -> None:
        self.root = root or TemporaryDirectory().name
        Path(self.root).mkdir(exist_ok=True)

    def push(self, content: bytes, key):
        ppath = Path(self.root, key)
        ppath.parent.mkdir(exist_ok=True, parents=True)
        ppath.write_bytes(content)
        return datetime.now()

    def full_link(self, key):
        return f"file://{self.root}/{key}"


@dataclass
class ExplorerDataset:
    name: str
    project: str
    namespace: str
    tables: Optional[list[str]] = None
    env: str = DEFAULT_ENV_NAME
    version: str = ""
    minimal: bool = False
    erd: bool = True
    col_renamer: dict = field(default_factory=dict)
    cc_context: dict = field(init=False, default_factory=dict)
    dfs: dict[str, pd.DataFrame] = field(init=False, default_factory=dict)

    def set_from_project(self, runtime: "ProjectRuntime"):
        meta = runtime.metadata_dic[self.project]
        ns_meta = meta.namespaces[self.namespace]
        scrutable_list = ns_meta.tables
        if self.tables:
            ns_tables = ns_meta.tables
            scrutable_list = [t for t in ns_tables if t.name in self.tables]
            assert all(map(lambda t: t in [_t.name for _t in ns_tables], self.tables))
        self.cc_context = {
            "project_url": meta.uri,
            "slug": self._slug,
            "name": self.name,
            "source_urls": list(map(str, ns_meta.source_urls)),
            "tables": list(map(self._get_table_cc, scrutable_list)),
            "notebooks": [],
            "n_tables": len(scrutable_list),
            "update_str": self._get_cron_desc(meta.cron),
            "erd_mermaid": self._get_erd(scrutable_list),
        }

    def dump(self, remote: LocalRemote):
        _root = SETUP_DIR / self._slug
        _root.mkdir(parents=True, exist_ok=True)
        desc_file = _root / "description.md"
        if not desc_file.exists():
            desc = f"A dataset of {len(self.dfs)} tables"
            desc_file.write_text(desc)
        if not [*_root.glob("*.ipynb")]:
            read_lines = [
                f'df_{tab} = pd.read_csv("{tab}.csv")\n' for tab in self.dfs.keys()
            ]
            nb_str = get_nb_string("Look", (["import pandas as pd"], read_lines))
            (_root / "init.ipynb").write_text(nb_str)
        for table_cc in self.cc_context["tables"]:
            tname = table_cc["slug"]
            df = self.dfs[tname]
            csv_key = f"{self._slug}/{tname}.csv"
            profile_key = f"{self._slug}/{tname}-profile.html"
            csv_blob = df.to_csv(index=None).encode("utf-8")
            (_root / f"{tname}.csv").write_bytes(csv_blob)
            profile_str: str = get_profile_str(df, self.minimal)
            remote.push(profile_str.encode("utf-8"), profile_key)
            mod_date = remote.push(csv_blob, csv_key)
            table_cc.update(
                {
                    "profile_url": remote.full_link(profile_key),
                    "update_date": mod_date.isoformat(" ", "minutes")[:16],
                    "n_cols": df.shape[1],
                    "n_rows": df.shape[0],
                    "csv_url": remote.full_link(csv_key),
                    "csv_filesize": _get_filesize(csv_blob),
                    "head_html": df.head()._repr_html_(),
                }
            )

    def id_(self):
        return (self.project, self.v_str)

    @property
    def v_str(self):
        return f"=={self.version}" if self.version else ""

    def _get_table_cc(self, scrutable: ScruTable):
        name = scrutable.name
        self.dfs[name] = self._get_df(scrutable)
        return {"name": _title(name), "slug": name}

    def _get_cron_desc(self, raw_cron: str):
        from cron_descriptor import ExpressionDescriptor

        if not raw_cron:
            return ""
        exp_dsc = ExpressionDescriptor(
            raw_cron, locale_code="en", throw_exception_on_parse_error=False
        )
        return exp_dsc.get_description().lower()

    def _get_erd(self, scrutables: list[ScruTable]):
        if not self.erd:
            return ""
        names = [st.name for st in scrutables]
        sql_filter = SqlFilter(self.project, self.namespace, names, self.col_renamer)
        with tmp_constr() as constr:
            with sql_filter.filter(constr) as new_constr:
                mm_str = get_mermaid(new_constr)
        return mm_str

    def _get_df(self, scrutable: ScruTable) -> pd.DataFrame:
        has_ind = bool(scrutable.index_cols)
        df_base = scrutable.get_full_df(env=self.env).reset_index(drop=not has_ind)
        renamer_base = {k: v for k, v in self.col_renamer.items() if isinstance(v, str)}
        full_renamer = {**self.col_renamer.get(scrutable.name, {}), **renamer_base}
        return df_base.rename(columns=full_renamer)

    @property
    def _slug(self):
        return self.name.lower().replace(" ", "_")


@dataclass
class ExplorerContext:
    datasets: list[ExplorerDataset]
    remote: Union[S3Remote, LocalRemote]
    registry: str = DEFAULT_REGISTRY
    book_root: Path = field(init=False, default_factory=Path.cwd)
    cc_template: str = CC_DIR
    cc_checkout: Optional[str] = None
    analytics_id: Optional[str] = None

    def load_data(self):
        # to avoid dependency clashes, it is slower but simpler
        # to setup a sandbox one by one
        preps: list[ExplorerDataset] = []
        with sandbox_project(self.registry) as core_path:
            # TODO: avoid recursive data imports here
            for _, ds_iter in groupby(self.datasets, ExplorerDataset.id_):
                self._run_as_proc(list(ds_iter), core_path, preps)

        cc_base = {"analytics_id": self.analytics_id, "datasets": {"datasets": []}}
        for pdset in preps:
            pdset.dump(self.remote)
            cc_base["datasets"]["datasets"].append(pdset.cc_context)

        CC_BASE_FILE.write_text(yaml.safe_dump(cc_base))

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
        self, core_path: Path, ds_list: list[ExplorerDataset], pq: Queue
    ):
        p_name, p_v = ds_list[0].id_()
        names = ", ".join([ds.namespace for ds in ds_list])
        core_path.write_text(
            f"from {META_MODULE_NAME}.{to_mod_name(p_name)} import {names}"
        )
        envs = set([d.env for d in ds_list])
        dnss = list(set([d.namespace for d in ds_list]))
        test_conf = Config(
            name=SANDBOX_NAME,
            registry=self.registry,
            version="v0.0",
            imported_projects=[ImportedProject(p_name, dnss, version=p_v)],
            envs=[ProjectEnv(env, import_envs={p_name: env}) for env in envs],
        )
        test_conf.dump()
        test_reg = Registry(test_conf, True)
        test_reg.update()
        try:
            test_reg.full_build()
            runtime = get_runtime()
            runtime.load_all_data()
            for dataset in ds_list:
                dataset.set_from_project(runtime)
                pq.put(dataset)
        finally:
            test_reg.purge()

    def _run_as_proc(self, lds, core_path, preps: list):
        pq = mp.Queue()
        proc = mp.Process(target=self._set_from_project, args=(core_path, lds, pq))
        proc.start()
        for _ in lds:
            preps.append(pq.get())
        pq.close()
        pq.join_thread()
        proc.join()
        proc.close()


def init_explorer(cron: str = "0 15 * * *"):
    write_book_actions(cron)
    REQUIREMENTS_FILE.write_text(f"{PACKAGE_NAME}[explorer]")
    SETUP_DIR.mkdir(exist_ok=True)
    load_explorer_data()


def load_explorer_data():
    get_full_auth().dump_dvc(local=False)
    ExplorerContext.load().load_data()


def build_explorer():
    gen_rmtree(BOOK_DIR)
    cc_base = yaml.safe_load(CC_BASE_FILE.read_bytes())
    copytree(SETUP_DIR, DATASETS)
    _extend_with_notebooks(cc_base)
    ctx = ExplorerContext.load()
    for ds_cc in cc_base["datasets"]["datasets"]:
        cookiecutter(
            ctx.cc_template,
            checkout=ctx.cc_checkout,
            no_input=True,
            extra_context={**cc_base, "main": ds_cc},
            overwrite_if_exists=True,
        )
    check_call(["jupyter-book", "build", BOOK_DIR.as_posix()])


def _extend_with_notebooks(cc_dic):
    for nb_path in DATASETS.glob("**/*.ipynb"):
        if ".ipynb_checkpoints" in nb_path.parts:
            continue  # pragma: no cover
        dataset_slug = nb_path.parts[-2]
        for ds_dic in cc_dic["datasets"]["datasets"]:
            if ds_dic["slug"] == dataset_slug:
                ds_dic["notebooks"].append(_NBParser(nb_path).cc_dic)
                continue


class _NBParser:
    def __init__(self, nb_path: Path) -> None:
        import nbformat
        from nbconvert.preprocessors import ExecutePreprocessor

        self.ds_root = nb_path.parent
        name = nb_path.name.split(".")[0]
        self.asset_root = self.ds_root / name / "assets"
        self.asset_root.mkdir(exist_ok=True, parents=True)
        self.cc_dic = {
            "name": name,
            "title": _title(name),
            "figures": [],
            "output_html": [],
        }

        nb_text = nb_path.read_bytes().decode("utf-8")
        nb_v = json.loads(nb_text)["nbformat"]
        nb_obj = nbformat.reads(nb_text, as_version=nb_v)
        ep = ExecutePreprocessor(
            timeout=600,
            kernel_name=nb_obj.metadata.kernelspec.name,
            extra_arguments=["--matplotlib=inline"],
        )
        ep.preprocess(nb_obj, {"metadata": {"path": self.ds_root}})
        nb_path.write_bytes(nbformat.writes(nb_obj).encode("utf-8"))
        title_rex = re.compile("^# (.*)")

        for ci, cell in enumerate(nb_obj.cells):
            for out in cell.get("outputs", []):
                data = out.get("data", {})
                self._parse_out(data, "image/png", "figures", base64.decodebytes, ci)
                self._parse_out(data, "text/html", "output_html", lambda x: x, ci)
            if cell.get("cell_type", "") == "markdown":
                source = cell["source"]
                for line in source if isinstance(source, list) else [source]:
                    ptitle = title_rex.findall(line)
                    if ptitle:
                        self.cc_dic["title"] = ptitle[0]

    def _parse_out(self, data, data_key, out_key, decode_wrap, ind):
        file_suffix = data_key.split("/")[-1]
        out_str = data.get(data_key)
        if out_str:
            out_path = self.asset_root / f"out-{ind}.{file_suffix}"
            out_path.write_bytes(decode_wrap(out_str.encode("utf-8")))
            self.cc_dic[out_key].append(out_path.relative_to(self.ds_root).as_posix())


def get_profile_str(df, minimal):

    from bs4 import BeautifulSoup, Tag
    from pandas_profiling import ProfileReport

    profile = ProfileReport(df, minimal=minimal)
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


def _get_filesize(blob: bytes):
    kb_size = len(blob) / 1000
    if kb_size > 1000:  # pragma: no cover
        return f"{kb_size / 1000:0.2f} MB"
    return f"{kb_size:0.2f} kB"
