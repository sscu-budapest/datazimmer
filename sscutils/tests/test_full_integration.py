from pathlib import Path

from invoke import Context

from sscutils.invoke_commands import (
    import_namespaces,
    push_envs,
    serialize_inscript_metadata,
    set_dvc_remotes,
    update_data,
    write_envs,
)
from sscutils.tests.create_dogshow import DogshowContextCreator, csv_path


def test_full_dogshow(tmp_path: Path):

    lroot = tmp_path / "root-dir"
    lroot.mkdir()

    ds_cc = DogshowContextCreator(
        local_output_root=lroot,
    )

    c = Context()

    with ds_cc.dataset_a:
        serialize_inscript_metadata(c, git_commit=True)
        update_data(c, (csv_path,))
        set_dvc_remotes(c)
        write_envs(c)
        push_envs(c, git_push=True)

    with ds_cc.dataset_b:
        import_namespaces(c)
        serialize_inscript_metadata(c, git_commit=True)
        update_data(c, (csv_path,))
