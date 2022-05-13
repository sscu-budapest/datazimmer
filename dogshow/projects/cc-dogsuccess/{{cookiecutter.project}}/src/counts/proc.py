import datazimmer as dz

from .. import success  # module import is encouraged
from ..success import SizedDog, sized_dog_table  # should work both ways
from .meta import CountBase, size_count_table, status_count_table


@dz.register(
    dependencies=[sized_dog_table, success.status_table],
    outputs=[size_count_table, status_count_table],
)
def count_proc():

    for src_table, src_ind, count_table in [
        (sized_dog_table, SizedDog.size.dogsize_name, size_count_table),
        (success.status_table, success.Status.status_name, status_count_table),
    ]:

        src_df = src_table.get_full_df()
        target_inds = count_table.index_cols
        assert len(target_inds) == 1
        count_table.replace_all(
            src_df.assign(**{CountBase.elem_count: 1})
            .groupby(src_ind)[CountBase.elem_count]
            .sum()
            .reset_index()
            .rename(columns={src_ind: target_inds[0]})
        )
