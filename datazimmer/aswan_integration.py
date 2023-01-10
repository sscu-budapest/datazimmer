from importlib import import_module
from typing import TYPE_CHECKING

from dvc.repo import Repo
from structlog import get_logger

from .config_loading import Config, RunConfig, get_aswan_leaf_param_id
from .metadata.complete_id import CompleteIdBase
from .naming import BASE_CONF_PATH, get_stage_name
from .utils import get_creation_module_name

logger = get_logger("aswan_integration")

if TYPE_CHECKING:
    from aswan import ANY_HANDLER_T  # pragma: no cover


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
        self.project = Project(self.name)
        self.depot = self.project.depot
        self.depot.setup()
        self._unproc_pulled = False
        self._complete_pulled = False

    def run(self):
        self.prepare_run()
        self._reg_from_module()
        logger.info("pulling")
        self.depot.pull()
        self.depot.current.purge()
        self.project.run(urls_to_overwrite=self.starters)
        logger.info("saving")
        latest = self.depot.save_current()
        logger.info("pushing")
        self.depot.push()
        self._conf.update_aswan_spec(self.name, latest.name)
        self.depot.current.purge()
        logger.info("done")

    def get_unprocessed_events(self, handler: "ANY_HANDLER_T", only_latest=False):
        from_status = self.get_aswan_status()
        logger.info("getting_unprocessed events", from_status=from_status, ns=self._ns)
        if not self._unproc_pulled:
            if from_status is None:
                self.depot.pull(complete=True)
                self._complete_pulled = True
            else:
                self.depot.pull(post_status=from_status)
            self._unproc_pulled = True
        return self.depot.get_handler_events(
            handler, only_latest=only_latest, post_status=from_status
        )

    def get_all_events(self, handler: "ANY_HANDLER_T", only_latest=True):
        if not self._complete_pulled:
            self.depot.pull(complete=True)
            self._complete_pulled = True
        return self.depot.get_handler_events(handler, only_latest=only_latest)

    def prepare_run(self):
        """this runs prior to running the project"""
        pass  # pragma: no cover

    def get_aswan_status(self):
        conf = RunConfig.load()

        stage_name = get_stage_name(self._ns, conf.write_env)
        possible_deps = []
        repo = Repo()

        aswan_id = get_aswan_leaf_param_id(self.name)

        for stage in repo.index.stages:
            # imported stage has no name attribute
            if getattr(stage, "name", "") == stage_name:
                possible_deps = stage.deps

        for dep in possible_deps:
            if dep.def_path == BASE_CONF_PATH.as_posix():
                info = dep.hash_info.value if dep.hash_info else {}
                return info.get(aswan_id)

    def _reg_from_module(self):
        self.project.register_module(import_module(type(self).__module__))
