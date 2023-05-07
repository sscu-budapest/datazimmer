import json
import os
from pathlib import Path

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from datazimmer.config_loading import Config
from datazimmer.naming import meta_version_from_tag
from datazimmer.typer_commands import (
    build_meta,
    deposit_to_zenodo,
    draw,
    import_raw,
    load_external_data,
    publish_data,
    run,
    run_aswan_project,
    set_whoami,
    update,
    validate,
)
from datazimmer.zenodo import ZenApi

from .create_dogshow import DogshowContextCreator, modify_to_version
from .util import dz_ctx, run_in_process


def test_full_dogshow(tmp_path: Path, pytestconfig, proper_env, test_bucket):
    # TODO: turn this into documentation
    mode = pytestconfig.getoption("mode")
    # TODO: make this set temporary
    set_whoami("Endre Márk", "Borza", "0000-0002-8804-4520")
    ds_cc = DogshowContextCreator.load(mode, tmp_path)
    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    if pg_host == "sqlite":  # pragma: no cover
        constr = "sqlite:///_db.sqlite"
    else:
        constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"

    with dz_ctx(ds_cc.ran_dirs):
        for ds in ds_cc.all_contexts:
            run_project_test(ds, constr)
        ds_cc.check_sdists()


def run_project_test(dog_context, constr):
    with dog_context as (name, versions, raw_imports, zen_pattern):
        print("%" * 40, "\n" * 8, name, "\n" * 8, "%" * 40, sep="\n")
        conf = Config.load()
        _complete(constr, raw_imports, zen_pattern)
        for testv in versions:
            modify_to_version(testv)
            if testv == conf.version:
                # should warn and just try install
                run_in_process(build_meta)
                continue
            _complete(constr, reset_aswan=True)
            run_in_process(build_meta)
            # no run or publish, as it will happen once at cron anyway
            # maybe needs changing
        if conf.cron:
            # TODO: warn if same data is tagged differently
            run_in_process(build_meta)
            run_in_process(run_aswan_project)
            run_in_process(run, commit=True, profile=True)
            run_in_process(publish_data)
            run_in_process(deposit_to_zenodo, test=True, publish=True)
        assert_zen(versions, zen_pattern, conf)


def assert_zen(versions, zen_pattern, conf: Config):
    zapi = ZenApi(conf, private=False, tag="z/v0.0/e")
    zid = zapi.zid_from_readme()
    concept = zapi.get(q=rf"10.5072\/zenodo\.{zid}").json()[0]["conceptdoi"]
    resp = zapi.get(q=concept.replace("/", r"\/"), all_versions=1).json()
    assert len(resp) == bool(versions) + bool(conf.cron) + bool(zen_pattern) + 1
    pub_vers = [meta_version_from_tag(r["metadata"]["version"]) for r in resp]
    assert all(map(lambda v: v in pub_vers, versions))
    if zen_pattern:
        assert any(r["metadata"]["access_right"] == "closed" for r in resp)


def _complete(constr, raw_imports=(), zen_pattern="", reset_aswan=False):
    run_in_process(build_meta)
    run_in_process(draw)
    run_in_process(_run_notebooks)
    for imp in raw_imports:
        run_in_process(import_raw, imp, commit=True)
    run_in_process(load_external_data, git_commit=True)
    run_in_process(run_aswan_project)
    run_in_process(run, commit=True, reset_aswan=reset_aswan)
    run_in_process(validate, constr)
    run_in_process(publish_data)
    if zen_pattern:
        key_path = Path("kp.key")
        key_path.write_text(((b"MTEx" * 11)[:-1] + b"=").hex())
        run_in_process(
            deposit_to_zenodo,
            test=True,
            private=True,
            path_filter=zen_pattern,
            publish=True,
            key_path=key_path.as_posix(),
        )
        run_in_process(publish_data)
    run_in_process(deposit_to_zenodo, test=True, publish=True)
    run_in_process(update)


def _run_notebooks():
    for nbp in Path("notebooks").glob("*.ipynb"):
        nb_text = nbp.read_bytes().decode("utf-8")
        nb_v = json.loads(nb_text)["nbformat"]
        nb_obj = nbformat.reads(nb_text, as_version=nb_v)
        ep = ExecutePreprocessor(kernel_name=nb_obj.metadata.kernelspec.name)
        ep.preprocess(nb_obj, {"metadata": {"path": nbp.parent}})
