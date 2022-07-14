import xarray as xr
import numpy as np
import pandas as pd
import json

with open('src/bcsd_parameter.json') as json_file:
    parameter = json.load(json_file)

version = parameter['version']
month   = parameter['issue_date']['month']
year    = parameter['issue_date']['year']

# Set the directory of the regional data
regdir  = parameter['directories']['regroot']

# Count the number of domains
ndoms = len(parameter['domains'])

# Loop over the domains
for i in range(0, ndoms):
    domain_name = parameter['domains'][i]['name']

    domaindir   = regdir + domain_name + '/' + 'daily' + '/' 
    lnchk_fle   = domaindir + 'linechunks' + '/' + 'SEAS5_BCSD_v' + version + '_daily_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '_lns.nc'
    mapchnk_fle = domaindir + 'seas5_bcsd' + '/' + 'SEAS5_BCSD_v' + version + '_daily_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '.nc'

    lnchnk_hndle = xr.open_dataset(lnchk_fle)
    
    nlat = lnchnk_hndle.dims['lat']
    nlon = lnchnk_hndle.dims['lon']
    
    # Load the temperature data
    da_t2m     = lnchnk_hndle.t2m.load()
    da_t2plus  = lnchnk_hndle.t2plus.load()
    da_t2minus = lnchnk_hndle.t2minus.load()
    
    # Create an empty xarray-Dataset
    out = xr.Dataset()
    
    # Compute the temperature ranges
    out['t2max'] = da_t2m + da_t2plus
    out['t2min'] = da_t2m - da_t2minus
    
    # Set the attributes for the additional variables
    out.t2max.attrs = {'long_name': 'maximum_daily_temperature_at_2m', 'units': 'K', 'standard_name': 'air_temperature'}
    out.t2min.attrs = {'long_name': 'minimum_daily_temperature_at_2m', 'units': 'K', 'standard_name': 'air_temperature'}
    
    lnchnk_hndle.tp.to_netcdf(path=mapchnk_fle, mode='w', format='NETCDF4_CLASSIC', encoding={'tp': {'dtype': 'int32', 'scale_factor': 0.01, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})
    lnchnk_hndle.t2m.to_netcdf(path=mapchnk_fle, mode='a', format='NETCDF4_CLASSIC', encoding={'t2m': {'dtype': 'int32', 'scale_factor': 0.01, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})    
    out.t2max.to_netcdf(path=mapchnk_fle, mode='a', format='NETCDF4_CLASSIC', encoding={'t2max': {'dtype': 'int32', 'scale_factor': 0.01, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}})    
    out.t2min.to_netcdf(path=mapchnk_fle, mode='a', format='NETCDF4_CLASSIC', encoding={'t2min': {'dtype': 'int32', 'scale_factor': 0.01, 'add_offset': 273.15, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}}) 
    lnchnk_hndle.ssrd.to_netcdf(path=mapchnk_fle, mode='a', format='NETCDF4_CLASSIC', encoding={'ssrd': {'dtype': 'int32', 'scale_factor': 0.01, 'zlib': True, 'complevel': 6, '_FillValue': -9999, 'chunksizes': [1, 1, nlat, nlon]}}) 
