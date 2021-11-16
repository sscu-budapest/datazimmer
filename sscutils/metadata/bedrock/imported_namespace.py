from dataclasses import dataclass
from typing import Optional

from ...naming import PIPELINE_STEP_SEPARATOR, ROOT_NS_LOCAL_NAME


@dataclass
class ImportedNamespace:
    prefix: str
    uri: str
    tag: Optional[str] = None

    def __eq__(self, o: "ImportedNamespace") -> bool:
        return (self.uri == o.uri) and (self.tag == o.tag)

    @classmethod
    def from_uri_parts(cls, prefix, uri_root, uri_slug, tag=None):
        return cls(
            prefix, PIPELINE_STEP_SEPARATOR.join([uri_root, uri_slug]), tag
        )

    @property
    def uri_slug(self):
        return self._splitret(1)

    @property
    def uri_root(self):
        return self._splitret(0)

    def _splitret(self, i):
        splitted_id = self.uri.split(PIPELINE_STEP_SEPARATOR)
        try:
            return splitted_id[i]
        except IndexError:
            return ROOT_NS_LOCAL_NAME
