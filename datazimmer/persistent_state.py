from dataclasses import dataclass

from .config_loading import Config


@dataclass
class PersistentState:
    def save(self):
        self.get_conf().dump_persistent_state(self)

    @classmethod
    def load(cls):
        return cls(**cls.get_conf().persistent_states.get(cls.get_full_name(), {}))

    @classmethod
    def get_full_name(cls):
        return f"{cls.__module__}.{cls.__name__}".replace(".", "-")

    @classmethod
    def get_conf(cls):
        # maybe get it from global runtime
        return Config.load()
