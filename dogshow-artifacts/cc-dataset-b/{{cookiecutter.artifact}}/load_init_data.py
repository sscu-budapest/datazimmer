import sys

import pandas as pd
from src.columns import DogSizesCols
from src.tables import dog_sizes_table

from sscutils import dump_dfs_to_tables

if __name__ == "__main__":

    droot = sys.argv[1]

    dogsize_df = pd.read_csv(f"{droot}/sizes.csv").set_index(
        DogSizesCols.dogsize_name
    )
    dump_dfs_to_tables(None, [(dogsize_df, dog_sizes_table)])
