import sys
print("PATH  ", sys.path)
import pandas as pd

from sscutils import dump_dfs_to_trepos
from src.trepos import persons_table, objects_table, events_table
from src.raw_cols import CommonCols

if __name__ == "__main__":

    droot = sys.argv[1]

    pers_df = pd.read_csv(f"{droot}/persons.csv").set_index(CommonCols.person_id)
    obj_df = pd.read_csv(f"{droot}/objects.csv").set_index(CommonCols.object_id)
    ev_df = pd.read_csv(f"{droot}/events.csv")

    dump_dfs_to_trepos(None, [(pers_df, persons_table), (ev_df, events_table),(obj_df, objects_table)])