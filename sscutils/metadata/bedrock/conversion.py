from dataclasses import dataclass
from functools import partial
from typing import Callable, List, Optional

from colassigner.constants import PREFIX_SEP

from ...utils import chainmap
from .artifact_metadata import ArtifactMetadata
from .column import Column
from .feature_types import CompositeFeature, ForeignKey, PrimitiveFeature
from .namespace_metadata import NamespaceMetadata
from .namespaced_id import NamespacedId


@dataclass
class FeatConverter:

    ns_meta: NamespaceMetadata
    a_meta: ArtifactMetadata
    wrapper: Callable = lambda x: x
    proc_fk: Optional[Callable] = None

    def feats_to_cols(self, feats) -> List[Column]:
        return chainmap(self.feat_to_cols, feats)

    def feat_to_cols(
        self,
        feat,
        init_prefix=(),
        calling_ns_prefix=None,
        open_to_fk=True,
    ) -> List[Column]:

        new_open_to_fk = True
        fk_to = None
        if isinstance(feat, PrimitiveFeature):
            name = PREFIX_SEP.join([*init_prefix, feat.name])
            return [self.wrapper(Column(name, feat.dtype, feat.nullable))]
        if isinstance(feat, CompositeFeature):
            sub_id = feat.dtype
            subfeats = self._get_atom(sub_id, calling_ns_prefix).features
        elif isinstance(feat, ForeignKey):
            new_open_to_fk = False
            sub_id = feat.table
            fk_to = self._get_id(sub_id, calling_ns_prefix)
            table_obj = self._get_atom(sub_id, calling_ns_prefix)
            subfeats = table_obj.index

        new_ns_prefix = (
            sub_id.ns_prefix if not sub_id.is_local else calling_ns_prefix
        )
        new_feat_prefix = (*init_prefix, feat.prefix)
        new_fun = partial(
            self.feat_to_cols,
            init_prefix=new_feat_prefix,
            calling_ns_prefix=new_ns_prefix,
            open_to_fk=new_open_to_fk,
        )
        out = chainmap(new_fun, subfeats)
        if fk_to is not None and open_to_fk and self.proc_fk:
            self.proc_fk(out, fk_to, new_feat_prefix)

        return out

    def _get_id(self, id_: NamespacedId, calling_namespace=None):
        if not id_.is_local:
            return id_
        if calling_namespace is None:
            calling_namespace = self.ns_meta.local_name
        return NamespacedId(calling_namespace, id_.obj_id)

    def _get_atom(self, id_: NamespacedId, calling_namespace=None):
        return self.a_meta.get_atom(self._get_id(id_, calling_namespace))
