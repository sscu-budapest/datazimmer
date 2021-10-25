from importlib import import_module
from pathlib import Path

from invoke import Context

from sscutils.invoke_commands import (
    import_namespaces,
    lint,
    load_external_data,
    push_envs,
    serialize_inscript_metadata,
    set_dvc_remotes,
    update_data,
    write_envs,
)
from sscutils.naming import SRC_PATH
from sscutils.tests.create_dogshow import DogshowContextCreator, csv_path


def import_pipereg():
    return import_module(str(SRC_PATH)).pipereg


def run_step(step):
    pipereg = import_pipereg()
    pipereg.get_step(step).run()


def test_full_dogshow(tmp_path: Path):

    lroot = tmp_path / "root-dir"
    lroot.mkdir()

    ds_cc = DogshowContextCreator(
        local_output_root=lroot,
    )

    c = Context()

    for ds in [ds_cc.dataset_a, ds_cc.dataset_b]:
        with ds as validator:
            import_namespaces(c, git_commit=True)
            lint(c)
            serialize_inscript_metadata(c, git_commit=True)
            update_data(c, (csv_path,))
            set_dvc_remotes(c)
            write_envs(c)
            push_envs(c, git_push=True)
            validator()

    with ds_cc.project_a as validator:
        import_namespaces(c, git_commit=True)
        load_external_data(c, git_commit=True)
        run_step("success")
        c.run("git add *")
        c.run('git commit -m "add step output"')
        c.run("git push")
        validator()

    with ds_cc.project_b as validator:
        pass
        # import_namespaces(c, git_commit=True)
        # load_external_data(c, git_commit=True)
        # pipereg = import_pipereg()
        validator()
