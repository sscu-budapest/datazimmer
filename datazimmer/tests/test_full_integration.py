import os
from pathlib import Path

from datazimmer.config_loading import Config
from datazimmer.explorer import _NBParser, init_explorer
from datazimmer.typer_commands import (
    build_explorer,
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
        for explorer in [ds_cc.explorer, ds_cc.explorer2]:
            with explorer():
                init_explorer()
                build_explorer()


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
            _complete(constr)
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


def _complete(constr, raw_imports=(), zen_pattern=""):
    run_in_process(build_meta)
    run_in_process(draw)
    run_in_process(_run_notebooks)
    for imp in raw_imports:
        run_in_process(import_raw, imp, commit=True)
    run_in_process(load_external_data, git_commit=True)
    run_in_process(run_aswan_project)
    run_in_process(run, commit=True)
    run_in_process(validate, constr)
    run_in_process(publish_data)
    if zen_pattern:
        run_in_process(
            deposit_to_zenodo,
            test=True,
            private=True,
            path_filter=zen_pattern,
            publish=True,
        )
        run_in_process(publish_data)
    run_in_process(deposit_to_zenodo, test=True, publish=True)
    run_in_process(update)


def _run_notebooks():
    for nbp in Path("notebooks").glob("*.ipynb"):
        _NBParser(nbp)
