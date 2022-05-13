# import metazimmer.dogshowbase.core as dogcore

# import metazimmer.dogsuccess.success as dogsucc
import pandas as pd

import datazimmer as dz

# p1ns1.Success to both dog kinds (from dogshow - dograce)
# based on DotM and race


class DogSuccess(dz.AbstractEntity):

    thing = int
    # success = dogsucc.StatusIndex TODO import data for validation
    # based on dotm  and competitions in ds2


ds_table = dz.ScruTable(DogSuccess)


@dz.register(outputs=[ds_table])  # dependencies=[dogsucc.status_table]
def proc():
    # TODO - give this some logic
    # dogsucc.status_table.get_full_df().index[:3]
    pd.DataFrame({DogSuccess.thing: [1, 2, 3]}).pipe(ds_table.replace_all)
