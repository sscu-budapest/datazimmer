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
        self._project.depot.setup()

    def run(self):
        self.extend_starters()
        self._project.depot.pull()
        self._project.run(urls_to_overwrite=self.starters)
        latest = self._project.depot.save_current()
        self._project.depot.push()
        self._conf.update_aswan_spec(self.name, latest.name)
        self._project.depot.current.purge()

    def get_unprocessed_events(self, handler: "ANY_HANDLER_T"):
        spec = self._conf.get_aswan_spec(self.name)
        from_status = spec.processed_upto_by_ns.get(self._ns)
        self._project.depot.pull(post_status=from_status)
        return self._project.depot.get_handler_events(handler)

    def get_all_events(self, handler: "ANY_HANDLER_T"):
        self._project.depot.pull(complete=True)
        return self._project.depot.get_handler_events(handler)

    def extend_starters(self):
        """this runs prior to running the project"""
        pass  # pragma: no cover
