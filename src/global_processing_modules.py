# Packages
import os

import xarray as xr
import numpy as np
import dask
import sys


from subprocess import run, PIPE
from pathlib import Path

import logging

from helper_modules import run_cmd


@dask.delayed
def gauss_to_regular(global_config, year, month, variable):
    
    month_str = str(month).zfill(2)
    
    flenme_grb = f"{global_config['raw_directory']}/{year}/{month_str}/{global_config['raw_forecasts']['prefix']}_daily_{variable}_{year}{month_str}.grb"
    flenme_nc  = f"{global_config['raw_directory']}/{year}/{month_str}/{global_config['raw_forecasts']['prefix']}_daily_{variable}_{year}{month_str}.nc"
    
    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'setgridtype,regular', str(flenme_grb), str(flenme_nc))
    
    run_cmd(cmd)
    