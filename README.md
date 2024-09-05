# SMAP 9km real-time processing 

The code in this repository downloads NASA's Soil Moisture Active Passive (SMAP) in it's native `.h5` format, converts it to a .`zarr` file, and saves to disc. Data can be opened in an `xarray` dataset. SMAP data is being explored to understand the effects of atmospheric rivers (ARs) in New Zealand, as well as a way to monitor drought. <br>

## How to run the code
There are many scientific software dependencies for processing SMAP data. Because of this, `conda` or `mamba` are the recommended ways to create the software environment. This environment can be created from the file `env.yml`. If you have conda installed, create it by running:
```
conda create -v -n SMAP -f env.yml
```

Install this package via pip: 
```
pip install smap_analysis
```
## Download SMAP

To download 9km real-time SMAP data, run `download_smap.py` from the `scripts` directory:

```
conda activate SMAP

python download_smap.py --start_date '2024-01-01' --end_date '2024-02-01'
```