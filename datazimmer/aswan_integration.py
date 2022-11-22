from typing import TYPE_CHECKING

from dvc.repo import Repo
from structlog import get_logger

from .config_loading import Config, RunConfig, get_aswan_leaf_param_id
from .metadata.complete_id import CompleteIdBase
from .naming import BASE_CONF_PATH, get_stage_name
from .utils import get_creation_module_name

logger = get_logger("aswan_integration")

if TYPE_CHECKING:
    from aswan import ANY_HANDLER_T


# TODO: add just checking on and logging external project (depot)
class DzAswan:
    name: str = None
    starters: dict[type["ANY_HANDLER_T"], list[str]] = {}
    cron: str = ""

    def __init__(self, global_run=False) -> None:
        from aswan import Project

        self._module = get_creation_module_name()
        self._ns = global_run or CompleteIdBase.from_module_name(self._module).namespace
        self._conf = Config.load()
        self._project = Project(self.name)
        self._depot = self._project.depot
        self._depot.setup()
        self._unproc_pulled = False
        self._complete_pulled = False

    def run(self):
        self.extend_starters()
        self._depot.pull()
        self._project.cleanup_current_run()
        self._project.run(urls_to_overwrite=self.starters)
        latest = self._depot.save_current()
        self._depot.push()
        self._conf.update_aswan_spec(self.name, latest.name)
        self._depot.current.purge()

    def get_unprocessed_events(self, handler: "ANY_HANDLER_T", only_latest=False):
        from_status = self.get_aswan_status()
        logger.info("getting_unprocessed events", from_status=from_status, ns=self._ns)
        if not self._unproc_pulled:
            if from_status is None:
                self._depot.pull(complete=True)
                self._complete_pulled = True
            else:
                self._depot.pull(post_status=from_status)
            self._unproc_pulled = True
        if from_status:
            runs = self._depot.get_missing_runs(self._depot.get_status(from_status))
        else:
            runs = None
        return self._depot.get_handler_events(
            handler, only_latest=only_latest, past_runs=runs
        )

    def get_all_events(self, handler: "ANY_HANDLER_T"):
        if not self._complete_pulled:
            self._depot.pull(complete=True)
            self._complete_pulled = True
        return self._depot.get_handler_events(handler)

    def extend_starters(self):
        """this runs prior to running the project"""
        pass  # pragma: no cover

    def get_aswan_status(self):
        conf = RunConfig.load()

        stage_name = get_stage_name(self._ns, conf.write_env)
        possible_deps = []
        repo = Repo()

        aswan_id = get_aswan_leaf_param_id(self.name)

        for stage in repo.index.stages:
            if stage.name == stage_name:
                possible_deps = stage.deps

        for dep in possible_deps:
            if dep.def_path == BASE_CONF_PATH.as_posix():
                info = dep.hash_info.value if dep.hash_info else {}
                return info.get(aswan_id)
