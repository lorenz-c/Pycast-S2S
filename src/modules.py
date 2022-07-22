# import packages
import os
from os.path import exists
import datetime as dt
import numpy as np
import netCDF4
import xarray as xr


def set_metadata(window_obs, window_mdl):
    # Empty Dictionary
    now = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    global_attributes = {
        'title': 'Bias-corrected SEAS5-forecasts - version 2.1',
        'Conventions': 'CF-1.8',
        'references': 'TBA',
        'institution': 'Karlsruhe Institute of Technology - Institute of Meteorology and Climate Research',
        'comment': '',
        'history': f"{now}: BCSD applied to SEAS5 data",
        'Contact_person': 'Christof Lorenz (Christof.Lorenz@kit.edu)',
        'Author': 'Christof Lorenz (Christof.Lorenz@kit.edu)',
        'License': 'CC-BY 4.0',
        'date_created': f"{now}"
    }

    variable_attributes = {
        'tp': {'long_name': 'total_precipitation', 'standard_name': 'precipitation_flux', 'units': 'mm/day', 'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.01, 'add_offset': 0.0},
        't2m': {'long_name': '2m_temperature', 'standard_name': 'air_temperature', 'units': 'K', 'dtype': 'int16', '_FillValue': -9999, 'scale_factor': 0.01, 'add_offset': 237.15},
        't2plus': {'long_name': 'tmax_minus_tmean', 'standard_name': 'air_temperature', 'units': 'K', 'dtype': 'int16', '_FillValue': -9999, 'scale_factor': 0.01, 'add_offset': 0.0},
        't2minus': {'long_name': 'tmean_minus_tmin', 'standard_name': 'air_temperature', 'units': 'K', 'dtype': 'int16', '_FillValue': -9999, 'scale_factor': 0.01, 'add_offset': 0.0},
        'ssrd': {'long_name': 'surface_solar_radiation', 'standard_name': 'surface_solar_radiation', 'units': 'W m-2', 'dtype': 'int32', '_FillValue': -9999, 'scale_factor': 0.01, 'add_offset': 0.0},
    }
    
    return global_attributes, variable_attributes


def set_filenames(month = None, year = None, domain = None, regroot = None, version = None, *args, **kwargs):

    yr_str   = str(year)
    mnth_str = str(month)
    mnth_str = mnth_str.zfill(2)
    basedir  = f"{regroot}{domain}/daily/"

    raw_lnechnks = f"{basedir}linechunks/SEAS5_daily_{yr_str}{mnth_str}_0.1_{domain}_lns.nc"
    bc_out_lns   = f"{basedir}linechunks/SEAS5_BCSD_v{version}_daily_{yr_str}{mnth_str}_0.1_{domain}_lns.nc"


    # Set the number of ensemble member
    syr_calib=1981
    eyr_calib=2016

    obs_dict = {'tp': basedir+'linechunks/ERA5_Land_daily_tp_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
                  't2m': basedir+'linechunks/ERA5_Land_daily_t2m_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
                  't2plus': basedir+'linechunks/ERA5_Land_daily_t2plus_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
                  't2minus': basedir+'linechunks/ERA5_Land_daily_t2minus_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
                  'ssrd': basedir+'linechunks/ERA5_Land_daily_ssrd_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc'}

    mdl_dict = {'tp': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
                  't2m': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
                  't2plus': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
                  't2minus': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
                  'ssrd': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc'}

    pred_dict = {'tp': raw_lnechnks,
                   't2m': raw_lnechnks,
                   't2plus': raw_lnechnks,
                   't2minus': raw_lnechnks,
                   'ssrd': raw_lnechnks}

    return obs_dict, mdl_dict, pred_dict, month, bc_out_lns


def create_4d_netcdf(fnme, global_attributes, variable_attributes, coordinates):

    da_dict = {}
    
    for variable in variable_attributes:
        da_dict[variable] = xr.DataArray(
            None, 
            dims = ['time', 'ens', 'lat', 'lon'], 
            coords = {
                'time': ('time', coordinates['time'], {'standard_name': 'time', 'long_name': 'time'}),
                'ens': ('ens', coordinates['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                'lat': ('lat', coordinates['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_east'}),
                'lon': ('lon', coordinates['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_north'})
            },
            attrs = {
                'standard_name': variable_attributes[variable]['standard_name'],
                'long_name': variable_attributes[variable]['long_name'],
                'units': variable_attributes[variable]['units'],
            } 
        )

    ds = xr.Dataset(
        data_vars = da_dict,
        coords = {
                'time': ('time', coordinates['time'], {'standard_name': 'time', 'long_name': 'time'}),
                'ens': ('ens', coordinates['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                'lat': ('lat', coordinates['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_east'}),
                'lon': ('lon', coordinates['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_north'})
            },
        attrs = global_attributes
    )

    encoding = {}

    for variable in variable_attributes:
        encoding[variable] = {
            'zlib': True,
            'complevel': 4,
            '_FillValue': variable_attributes[variable]['_FillValue'],
            'scale_factor': variable_attributes[variable]['scale_factor'],
            'add_offset': variable_attributes[variable]['scale_factor'],
            'dtype': variable_attributes[variable]['dtype'],
            'chunksizes': [1, len(coordinates['ens']), len(coordinates['lat']), len(coordinates['lon'])]
        }

    ds.to_netcdf(fnme, mode='w', format='NETCDF4_CLASSIC', encoding=encoding)

    return ds


def get_coords_from_files(filename):
    ds = xr.open_dataset(filename)

    return {
        'time': ds['time'].values,
        'lat': ds['lat'].values,
        'lon': ds['lon'].values,
        'ens': ds['ens'].values
    }


