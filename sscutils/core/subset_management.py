from dataclasses import asdict, dataclass, field


@dataclass
class Subset:
    name: str
    branch: str
    sampling_kwargs: dict = field(default_factory=dict)
    default: bool = False

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v and (k != "name")}


@dataclass
class SubsetToImport:
    repo: str
    tag: str
    subset: str
    prefix: str = ""
