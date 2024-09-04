from dask import delayed, compute
from dask.distributed import Client
import pandas as pd

import smap_analysis as sa

if __name__ == "__main__":
    client = Client(n_workers = 10, memory_limit = '5GB', threads_per_worker = 1)

    dates = pd.date_range('2015-04-01', '2024-04-01')

    delayed_list = []
    for date in dates:
        delayed_list.append(
            delayed(sa.download_smap)(date, force = False)
        )

    compute(delayed_list)