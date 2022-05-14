from dataclasses import dataclass, field
from typing import List

from .atoms import CompositeType, EntityClass
from .scrutable import ScruTable


@dataclass
class NamespaceMetadata:
    """full spec of metadata for a namespace"""

    name: str
    composite_types: List[CompositeType] = field(default_factory=list)
    entity_classes: List[EntityClass] = field(default_factory=list)
    tables: List[ScruTable] = field(default_factory=list)
    source_urls: List[str] = field(default_factory=list)

    def get_table_of_ec(self, ec: EntityClass) -> ScruTable:
        for table in self.tables:
            if table.entity_class == ec:
                return table
