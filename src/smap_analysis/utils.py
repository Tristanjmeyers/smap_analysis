# Copyright 2024 Tristan Jeffrey Meyers

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import xarray as xr
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.combine import concatenate_arrays
from kerchunk.zarr import ZarrToZarr
import kerchunk
import fsspec
import h5py

from ease_grid.ease2_grid import EASE2_grid
import subprocess
import pandas as pd
from dask import delayed, compute
from dask.distributed import Client, as_completed
import numpy as np

import gc
from typing import Tuple, List
import os
from pathlib import Path


CMD = 'wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --no-check-certificate --auth-no-challenge=on -r --reject "index.html*" -np -e robots=off '
SMAP_DIRECTORY = "n5eil01u.ecs.nsidc.org/SMAP/SPL3SMP_E.006"
SMAP_EXTRACT_FIELDS = ['soil_moisture', 'soil_moisture_error', 'soil_moisture_scah', 'soil_moisture_scav', 'soil_moisture_dca_pm', 'soil_moisture_error_pm', 'soil_moisture_scah_pm', 'soil_moisture_scav_pm']
EASE_GRID = EASE2_grid(9000)

def add_smap_coords(ds: xr.Dataset, date : pd.Timestamp ) -> xr.Dataset:
    '''
    SMAP data is read in with "phony_dim_x" coords. We have to populate these dimensions with latitude
    and longitude values from ease2_grids, then assign them as coords. It is also helpful to 
    add the time coordinate.
    '''
    ds = ds.rename({'phony_dim_0':'lat', 'phony_dim_1':'lon'})
    lats = EASE_GRID.latdim[2:-2]
    lons = EASE_GRID.londim[2:]
    ds = ds.assign_coords({'lat':lats, 'lon':lons, 'time': pd.to_datetime(date)}).expand_dims('time')

    ds = ds.transpose('time', 'lat', 'lon')

    return ds

def download_smap(date: pd.Timestamp, force: bool = True) -> Tuple[xr.Dataset,str]:
    '''
    Downloads the smap data using a wget command, then opens it as an xarray dataset.
    '''
    
    url, fname, oname = create_smap_fpaths(date)

    if force or not Path(fname).exists():
        cmd = CMD + url
        p = subprocess.Popen(cmd, shell=True)
        p_status = p.wait()
        
    json = SingleHdf5ToZarr(fname).translate()

    ds = xr.merge([open_kerchunk(json, "Soil_Moisture_Retrieval_Data_AM"),open_kerchunk(json, "Soil_Moisture_Retrieval_Data_PM")])
    ds = ds[SMAP_EXTRACT_FIELDS]

    return ds, oname

def open_kerchunk(json : dict, group : str) -> xr.Dataset :
    '''
    opens a json file created by kerchunk as an xarray dataset.
    '''
    return xr.open_dataset(json, engine="kerchunk", chunks = {}, group = group)

def create_smap_fpaths(date : pd.Timestamp) -> Tuple[str,str,str] :
    '''
    Generates 3 path names:
    1. Url of SMAP on the NASA
    2. File name of the .h5 file on disc (this created automatically via wget)
    3. Name of the output zarr file. 
    '''
    date = pd.to_datetime(date)
    datestr_fname = date.strftime('%Y%m%d')
    datestr_dname = date.strftime('%Y.%m.%d')
    
    url = f'https://{SMAP_DIRECTORY}/{datestr_dname}/SMAP_L3_SM_P_E_{datestr_fname}_R19240_001.h5'
    fname = f'{SMAP_DIRECTORY}/{datestr_dname}/SMAP_L3_SM_P_E_{datestr_fname}_R19240_001.h5'
    oname = f'{SMAP_DIRECTORY}/zarr/SMAP_L3_SM_P_E_{datestr_fname}_R19240_001.zarr'

    return url, fname, oname

def extract_smap(date : pd.Timestamp, force : bool = True, save : bool = False, extract_kwargs : dict = None) -> xr.Dataset:

    ds, oname = download_smap(date, force)

    ds = add_smap_coords(ds, date)
    
    if extract_kwargs is not None:
        ds = create_daily_soil_moisture_and_subset(ds, **extract_kwargs)
        

    if save:
        oname = Path(oname)
        oname.parents[0].mkdir(exist_ok = True, parents = True)
        ds.to_zarr(oname)
        ds.close()
        gc.collect()
    else:
        return ds
    
def extract_timestamp_from_fpath(f: Path) -> pd.Timestamp:
    f = Path(f)
    return pd.to_datetime(f.parent.name)



def create_daily_soil_moisture_and_subset(ds: xr.Dataset, domain : Tuple[slice, slice] = None) -> xr.Dataset :
    '''
    Creates a daily soil moisture dataset by averaging different soil mositure product channels.
    
    As per smap documentation for the L3 SPM_E product:
    Daily global composite of the estimated soil moisture at 9 km grid posting, as returned by
    the L2_SM_P_E processing software. The generic soil_moisture field is internally linked
    to the output produced by the baseline algorithm (DCA currently). 
    
    We have extracted soil_msoiture in the am, and we will use soil_moisture_dca_pm for the pm. 
    
    '''
    
    ds_am  = ds[['soil_moisture']].assign_coords({'update':'am'}).expand_dims('update')
    
    ds_pm  = ds[['soil_moisture_dca_pm']].rename(
        {'soil_moisture_dca_pm':'soil_moisture'}).assign_coords({'update':'pm'}).expand_dims('update')
    
    if domain is None:
        return xr.concat([ds_am, ds_pm], dim = 'update').mean('update') 

    else:
        return xr.concat([ds_am, ds_pm], dim = 'update').mean('update').sel(lat = domain[0], lon = domain[1])