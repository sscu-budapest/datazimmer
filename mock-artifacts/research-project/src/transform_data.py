from .pipereg import pipereg
from .data_management.op_trepos import persons_table


# TODO
table_profile_path = ""
combined_table = ""


@pipereg.register(
    dependencies=[persons_table],
    outputs=[combined_table],
    outputs_nocache=[table_profile_path],
)
def transform_step(top_n: int):
    pdf = persons_table.get_full_df()
