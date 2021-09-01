import sys

import pandas as pd

from sscutils import dump_dfs_to_trepos
from src.trepos import rules_table

if __name__ == "__main__":

    droot = sys.argv[1]

    rule_df = pd.read_csv(f"{droot}/rules.csv")
    dump_dfs_to_trepos(None, [(rule_df, rules_table)])
