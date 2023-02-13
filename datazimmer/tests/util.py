import multiprocessing as mp
from contextlib import contextmanager
from pathlib import Path

from datazimmer.typer_commands import cleanup
from datazimmer.utils import cd_into


@contextmanager
def dz_ctx(cleanup_dirs):
    dvc_conf_path = Path.home() / ".config" / "dvc" / "config"
    glob_conf = ""
    if dvc_conf_path.exists():  # pragma: no cover
        glob_conf = dvc_conf_path.read_text()
        dvc_conf_path.write_text("")
    try:
        yield
    finally:
        for ran_dir in cleanup_dirs:
            with cd_into(ran_dir):
                cleanup()
        if glob_conf:  # pragma: no cover
            dvc_conf_path.write_text(glob_conf)


def run_in_process(fun, *args, **kwargs):
    print("*" * 20, "RUNNING", fun.__name__, "*" * 20)
    proc = mp.Process(target=fun, args=args, kwargs=kwargs, name=fun.__name__)
    proc.start()
    proc.join()
    assert proc.exitcode == 0
    proc.terminate()
    proc.close()
