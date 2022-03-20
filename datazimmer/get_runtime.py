from typing import TYPE_CHECKING

from structlog import get_logger

from .exceptions import ArtifactRuntimeException

if TYPE_CHECKING:
    from .artifact_context import ArtifactContext  # pragma: no cover

logger = get_logger()

_GLOBAL_ARTIFACT_RUNTIME = None


def get_runtime(reset=False) -> "ArtifactContext":
    global _GLOBAL_ARTIFACT_RUNTIME
    if reset:
        _GLOBAL_ARTIFACT_RUNTIME = None
    if _GLOBAL_ARTIFACT_RUNTIME is None:
        from .artifact_context import ArtifactContext

        try:
            _GLOBAL_ARTIFACT_RUNTIME = ArtifactContext()
        except Exception as e:
            logger.exception(e)
            raise ArtifactRuntimeException("can't start runtime")
    return _GLOBAL_ARTIFACT_RUNTIME
