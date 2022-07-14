import xarray as xr
import numpy as np
import pandas as pd
import json

#from dask.distributed import Client

#client = Client() 


import multiprocessing as mp

from cdo import *
cdo = Cdo()


# Import the parameter file
with open('src/bcsd_parameter.json') as json_file:
    parameter = json.load(json_file)

# Get the current month
month   = parameter['issue_date']['month']
# ...and year
year    = parameter['issue_date']['year']

# Get the directory of the global data
glbldir = parameter['directories']['glbldir']

# Set the directories
fullglbldir = glbldir + 'daily' + '/' + format(year, '4') + '/' + format(month, '02') + '/'

regroot = parameter['directories']['regroot']


# Define a small function for running the sellonlatbox-command in parallel
def sellonlatbox(inpt):
    fle_in  = inpt[0]
    fle_out = inpt[1]
    bbox    = inpt[2]

    cdo.sellonlatbox(bbox, input=fle_in, output=fle_out, options = "-f nc4 -k grid -z zip_6")


# Count the number of domains
ndoms = len(parameter['domains'])



# Loop over the domains
for i in range(0, ndoms):

    domain_name = parameter['domains'][i]['name']
    domain_bbox = parameter['domains'][i]['bbox']


    fullregdir     = regroot + domain_name + '/' + 'daily' + '/' 
    flenme_raw     = fullregdir + 'seas5' + '/' + 'SEAS5_daily_' + format(year, '04') + format(month, '02') + '_O320_' + domain_name + '.nc'  # Filename for the raw forecasts
    flenme_interp  = fullregdir + 'seas5_h' + '/' + 'SEAS5_daily_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '.nc'
    flenme_lnchnks = fullregdir + 'linechunks' + '/' +  'SEAS5_daily_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '_lns.nc'


    ## 1. Apply sellonlatbox to the global files for truncating the global data to the current domain

    # For each domain, create a list which holds all the mandatory parameter for the sellonlatbox-call

    # Create an empty multiprocessing-pool for running a parallel for-loop 
    
    inpt_lst = list()
    for ens in range (0, 51):
        flenme_glbl    = fullglbldir + 'ECMWF_SEAS5_' + format(ens, '02') + '_' + format(year, '04') + format(month, '02') + '.nc' 
        tmp_out        = fullregdir + 'temp' + '/' + domain_name + '_reg_' + format(ens, '02') + format(year, '04') + format(month, '02') + '.nc'
        inpt_lst.append([flenme_glbl, tmp_out, domain_bbox])
       
    # Run sellonlatbox in parallel for speeding up the process
    pool = mp.Pool(processes=10)
    pool.map(sellonlatbox, inpt_lst)
    pool.close()

    # Load the first ensemble member for getting the latitudes an dlongitudes
    ens0        = fullregdir + 'temp' + '/' + domain_name + '_reg_00' + format(year, '04') + format(month, '02') + '.nc'
    temp_handle = xr.open_dataset(ens0)

    # ...and get the length of the dimensions
    nlat = temp_handle.dims['lat']
    nlon = temp_handle.dims['lon']
    nts  = temp_handle.dims['time']
    nens = 51
    
    ## 2. Put all ensemble member in a single file

     # Load all files in one handle
    wldcrd      = fullregdir + 'temp' + '/' + domain_name + '_reg_*' + format(year, '04') + format(month, '02') + '.nc'
    temp_handle = xr.open_mfdataset(wldcrd, concat_dim='ens', combine='nested', chunks={'time': 1, 'lat': nlat, 'lon': nlon}).persist()
    temp_handle = temp_handle.transpose("time", "ens", "lat", "lon")
    
    # Write the output variables to a single file
    temp_handle.tp.to_netcdf(path=flenme_raw, mode='w', format='NETCDF4_CLASSIC', encoding={'tp': {'dtype': 'int32', 'scale_factor': 0.001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    temp_handle.t2m.to_netcdf(path=flenme_raw, mode='a', format='NETCDF4_CLASSIC', encoding={'t2m': {'dtype': 'int32', 'scale_factor': 0.0001, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    temp_handle.t2min.to_netcdf(path=flenme_raw, mode='a', format='NETCDF4_CLASSIC', encoding={'t2min': {'dtype': 'int32', 'scale_factor': 0.0001, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    temp_handle.t2max.to_netcdf(path=flenme_raw, mode='a', format='NETCDF4_CLASSIC', encoding={'t2max': {'dtype': 'int32', 'scale_factor': 0.0001, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    temp_handle.ssrd.to_netcdf(path=flenme_raw, mode='a', format='NETCDF4_CLASSIC', encoding={'ssrd': {'dtype': 'int32', 'scale_factor': 0.001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}}) 

    ## 3. Do the interpolation
    # Set a reference grid for the re-mapping
    grd    = '/pd/home/lorenz-c/Projects/bias-correction-of-seas5/src/masks' + '/' + 'Landsea_mask_0.1_' + domain_name + '.nc'
    
    # Use CDO for doing the re-mapping
    cdo.remapbil(grd, input=flenme_raw, output = flenme_interp, options = "-f nc4 -k grid -z zip_6 -P 10")

    ## 4. Compute the temperature ranges for the bias correction
    # Load temperature data
    high_hndle = xr.open_dataset(flenme_interp)
    
    # Get number of latitutes and longitudes
    nlat = high_hndle.dims['lat']
    nlon = high_hndle.dims['lon']
    
    # Load the temperature data
    da_tmax  = high_hndle.t2max.load()
    da_tmin  = high_hndle.t2min.load()
    da_tmean = high_hndle.t2m.load()
    
    # Create an empty xarray-Dataset
    out = xr.Dataset()
    
    # Compute the temperature ranges
    out['dtr']     = da_tmax - da_tmin
    out['t2plus']  = da_tmax - da_tmean
    out['t2minus'] = da_tmean - da_tmin
    
    # Close the file-handle so that we don't mess with the open NetCDF-file
    high_hndle.close()
    
    # Set the attributes for the additional variables
    out.dtr.attrs = {'long_name': 'diurnal_temperature_range', 'units': 'K'}
    out.t2plus.attrs = {'long_name': 'tmax_minus_tmean', 'units': 'K'}
    out.t2minus.attrs = {'long_name': 'tmean_minus_tmin', 'units': 'K'}
    
    # Write the temperature ranges to the output file
    out.dtr.to_netcdf(path=flenme_interp, mode='a', format='NETCDF4_CLASSIC', encoding={'dtr': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    out.t2plus.to_netcdf(path=flenme_interp, mode='a', format='NETCDF4_CLASSIC', encoding={'t2plus': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    out.t2minus.to_netcdf(path=flenme_interp, mode='a', format='NETCDF4_CLASSIC', encoding={'t2minus': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}}) 

    high_hndle = xr.open_dataset(flenme_interp)

    # 5. Do some re-chunking for speeding up the process
    high_hndle.tp.to_netcdf(path=flenme_lnchnks, mode='w', format='NETCDF4_CLASSIC', encoding={'tp': {'dtype': 'int32', 'scale_factor': 0.001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}})
    high_hndle.t2m.to_netcdf(path=flenme_lnchnks, mode='a', format='NETCDF4_CLASSIC', encoding={'t2m': {'dtype': 'int32', 'scale_factor': 0.0001, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}})
    high_hndle.dtr.to_netcdf(path=flenme_lnchnks, mode='a', format='NETCDF4_CLASSIC', encoding={'dtr': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}})
    high_hndle.t2plus.to_netcdf(path=flenme_lnchnks, mode='a', format='NETCDF4_CLASSIC', encoding={'t2plus': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}})
    high_hndle.t2minus.to_netcdf(path=flenme_lnchnks, mode='a', format='NETCDF4_CLASSIC', encoding={'t2minus': {'dtype': 'int32', 'scale_factor': 0.0001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}})
    high_hndle.ssrd.to_netcdf(path=flenme_lnchnks, mode='a', format='NETCDF4_CLASSIC', encoding={'ssrd': {'dtype': 'int32', 'scale_factor': 0.001, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [nts, nens, 1, 1]}}) 
   

  
