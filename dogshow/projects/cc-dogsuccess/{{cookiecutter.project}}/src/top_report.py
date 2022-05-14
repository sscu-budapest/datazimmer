import datazimmer as dz

from .counts.meta import SizeCount, size_count_table
from .sex_matches import SexMatch, sex_match_table

top_tables_report = dz.ReportFile("tops.md")
deps = [sex_match_table, size_count_table]


@dz.register(dependencies=deps, outputs_nocache=[top_tables_report])  # remove in v1.0
def calculate_sex_match(show_top):

    sm_df = sex_match_table.get_full_df().sort_values(SexMatch.count, ascending=False)
    sc_df = size_count_table.get_full_df().sort_values(
        SizeCount.elem_count, ascending=False
    )

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
