from colassigner import get_all_cols

import datazimmer as dz

from .. import success  # module import is encouraged
from ..success import sized_dog_table  # should work both ways
from .meta import CountBase, size_count_table, status_count_table


@dz.register(
    dependencies=[sized_dog_table, success.status_table],
    outputs=[size_count_table, status_count_table],
)
def count_proc():

    for src_table, src_ind, count_table in [
        (sized_dog_table, sized_dog_table.features.size, size_count_table),
        (success.status_table, success.StatusIndex.status_name, status_count_table),
    ]:

        src_df = src_table.get_full_df()
        target_inds = get_all_cols(count_table.index)
        assert len(target_inds) == 1
        (
            src_df.assign(**{CountBase.elem_count: 1})
            .groupby(src_ind)[CountBase.elem_count]
            .sum()
            .reset_index()
            .rename(columns={src_ind: target_inds[0]})
            .pipe(count_table.replace_all)
        )

    # dog_w_size_df = sized_dog_table.get_full_df()
    # dog_w_size_df.assign(**{CountBase.count: 1}).groupby(
    #     SizedDogFeatures.size
    # ).sum().pipe(size_count_table.replace_all)
