from dask import delayed, compute
from dask.distributed import Client
import pandas as pd

import argparse

import smap_analysis as sa

def main():
    
    parser = argparse.ArgumentParser(
        '''
        Downloads 9 km SMAP data from Earthdata. 
        SMAP product is SPMP_E v006. Documentation here:
        https://nsidc.org/sites/default/files/documents/user-guide/spl3smp_e-v006-userguide.pdf
        
        Files are downloaded in their native .h5 format to:
        n5eil01u.ecs.nsidc.org/SMAP/SPL3SMP_E.006/
        
        usage:
        
        python download_smap.py --start_date 2021-02-01 --end_date 2021-02-05
        
        '''
    )
    
    parser.add_argument(
        '--start_date',
        required = True,
        type = str,
        description = 'Start date to extract from.'
    )
    
    parser.add_argument(
        '--end_date',
        required = True,
        type = str,
        description = 'End date to extract to.'
    )
    
    parser.add_argument(
        '--redownload',
        type = bool,
        default = False,
        description = 'End date to extract to.'
    )
    
    parser.add_argument(
        '--n_workers',
        type = int,
        default = 3,
        description = 'Number of dask workers.'
    )
    
    parser.add_argument(
        '--memory_limit',
        type = str,
        default = '15GB',
        description = 'Number of dask workers.'
    )
    
    args = parser.parse_args()
    
    start_date = pd.to_datetime(args.start_date)
    end_date = pd.to_datetime(args.end_date)
    redownload = args.redownload
    n_workers = int(args.n_workers)
    memory_limit = str(memory_limit)
    
    client = Client(n_workers = n_workers, memory_limit = memory_limit, threads_per_worker = 1)

    dates = pd.date_range(start_date, end_date)

    delayed_list = []
    for date in dates:
        delayed_list.append(
            delayed(sa.download_smap)(date, force = redownload)
        )

    compute(delayed_list)
    
    
if __name__ == "__main__":
    main()
    