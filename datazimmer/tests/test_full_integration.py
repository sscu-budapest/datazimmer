import multiprocessing as mp
import os
from pathlib import Path

from datazimmer.config_loading import Config
from datazimmer.explorer import _NBParser, init_explorer
from datazimmer.naming import BASE_CONF_PATH, MAIN_MODULE_NAME
from datazimmer.typer_commands import (
    build_explorer,
    build_meta,
    cleanup,
    draw,
    import_raw,
    load_external_data,
    publish_data,
    publish_meta,
    run,
    run_aswan_project,
    update,
    validate,
)
from datazimmer.utils import cd_into, git_run

from .create_dogshow import DogshowContextCreator, modify_to_version


def test_full_dogshow(tmp_path: Path, pytestconfig, proper_env, test_bucket):
    # TODO: turn this into documentation
    mode = pytestconfig.getoption("mode")
    ds_cc = DogshowContextCreator.load(mode, tmp_path)

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    if pg_host == "sqlite":  # pragma: no cover
        constr = "sqlite:///_db.sqlite"
    else:
        constr = f"postgresql://postgres:postgres@{pg_host}:5432/postgres"
    try:
        for ds in ds_cc.all_contexts:
            run_project_test(ds, constr)
        ds_cc.check_sdists()
        for explorer in [ds_cc.explorer, ds_cc.explorer2]:
            with explorer():
                init_explorer()
                build_explorer()
    finally:
        for ran_dir in ds_cc.ran_dirs:
            with cd_into(ran_dir):
                cleanup()


def run_project_test(dog_context, constr):
    with dog_context as (name, versions, raw_imports):
        print("%" * 40, "\n" * 8, name, "\n" * 8, "%" * 40, sep="\n")
        conf = Config.load()
        _complete(constr, raw_imports)
        for testv in versions:
            modify_to_version(testv)
            if testv == conf.version:
                # should warn and just try install
                _run(build_meta)
                continue
            _complete(constr)
            _run(build_meta)
        if conf.cron:
            # TODO: warn if same data is tagged differently
            _run(build_meta)
            _run(run_aswan_project)
            _run(run, commit=True, profile=True)
            _run(publish_data)


def _complete(constr, raw_imports=()):
    _run(build_meta)
    _run(draw)
    git_run(add=[MAIN_MODULE_NAME, BASE_CONF_PATH], msg="build", check=True)
    _run_notebooks()
    for imp in raw_imports:
        _run(import_raw, imp)
    _run(load_external_data, git_commit=True)
    _run(run_aswan_project)
    _run(run, commit=True)
    _run(validate, constr)
    _run(publish_meta)
    _run(publish_data)
    _run(update)


def _run(fun, *args, **kwargs):
    print("*" * 20, "RUNNING", fun.__name__, "*" * 20)
    proc = mp.Process(target=fun, args=args, kwargs=kwargs, name=fun.__name__)
    proc.start()
    proc.join()
    assert proc.exitcode == 0
    proc.terminate()
    proc.close()


def _run_notebooks():
    for nbp in Path("notebooks").glob("*.ipynb"):
        _NBParser(nbp)
