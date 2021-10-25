from ..success import sized_dogs_table, DogSizeCalculation
from .report_meta import size_count_table, SizeCountFeatures
from ..pipereg import pipereg


@pipereg.register(dependencies=[sized_dogs_table], outputs=[size_count_table])
def report_step():
    dog_w_size_df = sized_dogs_table.get_full_df()

    dog_w_size_df.assign(**{SizeCountFeatures.count: 1}).groupby(
        DogSizeCalculation.size
    ).sum().pipe(size_count_table.trepo.replace_all)
