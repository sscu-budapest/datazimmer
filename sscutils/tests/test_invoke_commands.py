import importlib
import re

from invoke import Context, MockContext

from sscutils.constants import DATA_PATH
from sscutils.core.invoke_commands import (
    import_data,
    init_dataset,
    push_subsets,
    set_dvc_remotes,
    write_subsets,
)
from sscutils.tests.utils import (
    TemporaryDataset,
    TemporaryDataset2,
    TemporaryProject,
    TemporaryProject2,
)


def test_init_dataset(tmp_path):
    c = MockContext(run={re.compile(".*"): True}, repeat=True)
    with c.cd(tmp_path.as_posix()):
        init_dataset(c)
    assert (tmp_path / DATA_PATH).exists()


def test_write_subsets(tmp_path):
    c = MockContext()
    dspath = tmp_path / "dst"
    with TemporaryDataset(dspath):
        write_subsets(c)
    for prefix in ["complete", "dave_annie", "pete_dave_success"]:
        assert (dspath / DATA_PATH / prefix).exists()
        # TODO: test this better


def test_push_subsets(tmp_path):
    c = Context()
    dspath = tmp_path / "dst"
    with c.cd(dspath.as_posix()), TemporaryDataset(dspath):
        write_subsets(c)
        set_dvc_remotes(c)
        push_subsets(c)


def test_project_commands(tmp_path):
    c = Context()
    ds1_path = tmp_path / "ds1"
    ds2_path = tmp_path / "ds2"
    dp1_path = tmp_path / "dp1"
    dp2_path = tmp_path / "dp2"

    dvc_remote1 = tmp_path / "dvcr1"
    dvc_remote2 = tmp_path / "dvcr2"

    dvc_remote1.mkdir()
    dvc_remote2.mkdir()

    dvcrlist = [*map(str, [dvc_remote1, dvc_remote2])]
    dslist = [*map(str, [ds1_path, ds2_path])]

    with c.cd(ds1_path), TemporaryDataset(ds1_path, dvc_remotes=dvcrlist):
        write_subsets(c)
        set_dvc_remotes(c)
        push_subsets(c, True)

    with c.cd(ds2_path), TemporaryDataset2(ds2_path, dvc_remotes=dvcrlist[:1]):
        write_subsets(c)
        set_dvc_remotes(c)
        push_subsets(c, True)

    with c.cd(dp1_path), TemporaryProject(dp1_path, external_dvc_repos=dslist):
        import_data(c)
        src_module = importlib.import_module("src")
        pipereg = src_module.pipereg
        pipereg.get_step("transform_step").run()

    with TemporaryProject2(dp2_path, external_dvc_repos=[dp1_path.as_posix()]):
        pass
        # TODO: set intermediate import
