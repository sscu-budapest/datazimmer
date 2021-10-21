from pathlib import Path

from invoke import Context

from sscutils.invoke_commands import (
    import_namespaces,
    load_external_data,
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

    for ds in [ds_cc.dataset_a, ds_cc.dataset_b]:
        with ds:
            import_namespaces(c)
            serialize_inscript_metadata(c, git_commit=True)
            update_data(c, (csv_path,))
            set_dvc_remotes(c)
            write_envs(c)
            push_envs(c, git_push=True)

    with ds_cc.project_a:
        import_namespaces(c)
        load_external_data(c)
