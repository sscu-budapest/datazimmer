from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Callable, List, Optional

from colassigner.constants import PREFIX_SEP

from ...utils import chainmap
from .column import Column, to_dt_map
from .complete_id import CompleteId, CompleteIdBase
from .feature_types import CompositeFeature, ForeignKey, PrimitiveFeature

if TYPE_CHECKING:
    from ...artifact_context import ArtifactContext  # pragma: no cover


@dataclass
class FeatConverter:

    runtime: "ArtifactContext"
    init_base: CompleteIdBase
    wrapper: Callable = lambda x: x
    proc_fk: Optional[Callable] = None

    def feats_to_cols(self, feats) -> List[Column]:
        return chainmap(partial(self.feat_to_cols, calling_base=self.init_base), feats)

    def feat_to_cols(
        self,
        feat,
        calling_base,
        init_prefix=(),
        open_to_fk=True,
    ) -> List[Column]:

        new_open_to_fk = True
        fk_to = None
        if isinstance(feat, PrimitiveFeature):
            name = PREFIX_SEP.join([*init_prefix, feat.name])
            return [self.wrapper(Column(name, feat.dtype, feat.nullable))]
        if isinstance(feat, CompositeFeature):
            sub_id = feat.dtype
            subfeats = self._get_atom(sub_id, calling_base).features
        elif isinstance(feat, ForeignKey):
            new_open_to_fk = False
            sub_id = feat.table
            fk_to = self._get_id(sub_id, calling_base)
            table_obj = self._get_atom(sub_id, calling_base)
            subfeats = table_obj.index

        new_base = sub_id.base if not sub_id.is_local else calling_base
        new_feat_prefix = (*init_prefix, feat.prefix)
        new_fun = partial(
            self.feat_to_cols,
            init_prefix=new_feat_prefix,
            calling_base=new_base,
            open_to_fk=new_open_to_fk,
        )
        out = chainmap(new_fun, subfeats)
        if fk_to is not None and open_to_fk and self.proc_fk:
            self.proc_fk(out, fk_to.absolute_from(calling_base), new_feat_prefix)

        return out

    def _get_id(self, id_: CompleteId, calling_base: CompleteIdBase):
        if id_.is_local:
            return calling_base.to_id(id_.obj_id)
        return id_

    def _get_atom(self, id_: CompleteId, calling_base: CompleteIdBase):
        return self.runtime.get_atom(self._get_id(id_, calling_base))


def table_id_to_dtype_maps(id_: CompleteId, runtime: "ArtifactContext"):
    table = runtime.get_atom(id_)
    convfun = FeatConverter(runtime, id_.base).feats_to_cols
    return map(
        lambda feats: to_dt_map(convfun(feats)),
        [table.features, table.index],
    )


def table_id_to_dtype_map(id_: CompleteId, runtime):
    feat_dic, ind_dic = table_id_to_dtype_maps(id_, runtime)
    return {**feat_dic, **ind_dic}
