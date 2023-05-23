from typing import TYPE_CHECKING

from structlog import get_logger

from .exceptions import ProjectRuntimeException

if TYPE_CHECKING:
    from .project_runtime import ProjectRuntime  # pragma: no cover

logger = get_logger()

_GLOBAL_RUNTIME = None


def get_runtime() -> "ProjectRuntime":
    global _GLOBAL_RUNTIME
    import os

    print("GETTING RUNTIME", os.getpid())
    if _GLOBAL_RUNTIME is None:
        from .project_runtime import ProjectRuntime

        try:
            print("LOADING RUNTIME", os.getpid())
            _GLOBAL_RUNTIME = ProjectRuntime()
        except Exception as e:
            logger.info(str(e))
            raise ProjectRuntimeException(f"can't start runtime: {e}")
    return _GLOBAL_RUNTIME
