from pathlib import Path

from .get_size_counts.meta import SizeCountFeatures, size_count_table
from .pipereg import pipereg
from .sex_matches import SexMatchFeatures, sex_match_table

top_tables_report = Path("reports", "tops.md")


@pipereg.register(
    dependencies=[
        sex_match_table,
        size_count_table,
    ],
    outputs_nocache=[top_tables_report],
)
def calculate_sex_match(show_top):

    sm_df = sex_match_table.get_full_df().sort_values(SexMatchFeatures.count, ascending=False)
    sc_df = size_count_table.get_full_df().sort_values(SizeCountFeatures.count, ascending=False)

    top_tables_report.write_text(
        "\n\n".join(
            [
                "# Top sizes",
                sc_df.head(show_top).to_markdown(),
                "# Top sex matches",
                sm_df.head(show_top).to_markdown(),
            ]
        )
    )
