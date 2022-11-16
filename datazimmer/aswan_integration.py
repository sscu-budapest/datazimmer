from typing import TYPE_CHECKING

from .config_loading import Config
from .metadata.complete_id import CompleteIdBase
from .utils import get_creation_module_name

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
        spec = self._conf.get_aswan_spec(self.name)
        from_status = spec.processed_upto_by_ns.get(self._ns)
        self._depot.pull(post_status=from_status)
        if from_status:
            runs = self._depot.get_missing_runs(self._depot.get_status(from_status))
        else:
            runs = None
        return self._depot.get_handler_events(
            handler, only_latest=only_latest, past_runs=runs
        )

    def get_all_events(self, handler: "ANY_HANDLER_T"):
        self._depot.pull(complete=True)
        return self._depot.get_handler_events(handler)

    def extend_starters(self):
        """this runs prior to running the project"""
        pass  # pragma: no cover
