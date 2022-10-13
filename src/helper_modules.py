# import packages
import os
from os.path import exists
import datetime as dt
import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da

import dask
from dask_jobqueue import SLURMCluster
from dask.distributed import Client

from subprocess import run, PIPE
from pathlib import Path

import sys
#from bc_module import bc_module
#from write_output import write_output


def update_global_attributes(global_config, bc_params, coords, domain):
    # Update the global attributes with some run-specific parameters
    now = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    global_config['comment'] = f'Domain: {domain}, BCSD-Parameter: {str(bc_params)}'
    global_config['creation_date'] = f"{now}"

    global_config['geospatial_lon_min'] = min(coords['lon'])
    global_config['geospatial_lon_max'] = max(coords['lon'])
    global_config['geospatial_lat_min'] = min(coords['lat'])
    global_config['geospatial_lat_max'] = max(coords['lat'])
    global_config['StartTime']          = pd.to_datetime(coords['time'][0]).strftime("%Y-%m-%dT%H:%M:%S")
    global_config['StopTime']           = pd.to_datetime(coords['time'][-1]).strftime("%Y-%m-%dT%H:%M:%S")

    return global_config

def set_filenames(year, month, domain_config, variable_config, forecast_linechunks):

    # Integrate check for every parameter...
    yr_str   = str(year)
    mnth_str = str(month)
    mnth_str = mnth_str.zfill(2)
    basedir  = f"{domain_config['regroot']}/daily/"

    raw_dict       = {}
    bcsd_dict      = {}
    ref_hist_dict  = {}
    mdl_hist_dict  = {}

    for variable in variable_config:

        if domain_config['raw_forecasts']['merged_variables'] == True:
            if forecast_linechunks == True:
                raw_dict[variable]  = f"{basedir}linechunks/{domain_config['raw_forecasts']['prefix']}_daily_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
            else:
                raw_dict[variable]  = f"{basedir}input_target_res/{domain_config['raw_forecasts']['prefix']}_daily_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
        else:
            if forecast_linechunks == True:
                raw_dict[variable]  = f"{basedir}linechunks/{domain_config['raw_forecasts']['prefix']}_daily_{variable}_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
            else:
                raw_dict[variable]  = f"{basedir}input_target_res/{domain_config['raw_forecasts']['prefix']}_daily_{variable}_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
                
                
        if domain_config["bcsd_forecasts"]['merged_variables'] == True:
            if forecast_linechunks == True:
                bcsd_dict[variable] = f"{basedir}linechunks/{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
            else:
                bcsd_dict[variable] = f"{basedir}output_target_res/{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
                
        else:
            if forecast_linechunks == True:
                bcsd_dict[variable] = f"{basedir}linechunks/{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{variable}_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
            else:
                bcsd_dict[variable] = f"{basedir}output_target_res/{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{variable}_{yr_str}{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"

        if domain_config['reference_history']['merged_variables'] == True:
            ref_hist_dict[variable]  = f"{basedir}linechunks/{domain_config['reference_history']['prefix']}_daily_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
        else:
            ref_hist_dict[variable]  = f"{basedir}linechunks/{domain_config['reference_history']['prefix']}_daily_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"


        if domain_config['model_history']['merged_variables'] == True:
            mdl_hist_dict[variable]  = f"{basedir}linechunks/{domain_config['model_history']['prefix']}_daily_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{mnth_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
        else:
            mdl_hist_dict[variable]  = f"{basedir}linechunks/{domain_config['model_history']['prefix']}_daily_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}__{mnth_str}{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"

    return raw_dict, bcsd_dict, ref_hist_dict, mdl_hist_dict


def set_encoding(variable_config, coordinates, type='maps'):
    encoding = {}
    
    if type == 'maps':
        if 'ens' in coordinates:
            chunksizes = [20, len(coordinates['ens']), len(coordinates['lat']), len(coordinates['lon'])]
        else:
            chunksizes =  [20, len(coordinates['lat']), len(coordinates['lon'])]
    elif type == 'lines':
        if 'ens' in coordinates:
            chunksizes = [len(coordinates['time']), len(coordinates['ens']), 1, 1]
        else:
            chunksizes = [len(coordinates['time']), 1, 1]
            
            

    for variable in variable_config:        
        encoding[variable] =  {
            #'zlib': True,
            #'complevel': 4,
            '_FillValue': variable_config[variable]['_FillValue'],
            'scale_factor': variable_config[variable]['scale_factor'],
            'add_offset': variable_config[variable]['scale_factor'],
            'dtype':variable_config[variable]['dtype'],
            'chunksizes': chunksizes            
        }
        
    return encoding

def create_4d_netcdf(bcsd_dict, global_config, domain_config, variable_config, coordinates):

    da_dict  = {}

    encoding = set_encoding(variable_config, coordinates)
 

    # This dataarray has all variables, that are included in variable_config
    for variable in variable_config:
        da_dict[variable] = xr.DataArray(
            None, 
            dims = ['time', 'ens', 'lat', 'lon'], 
            coords = {
                'time': ('time', coordinates['time'], {'standard_name': 'time', 'long_name': 'time'}),
                'ens': ('ens', coordinates['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                'lat': ('lat', coordinates['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_north'}),
                'lon': ('lon', coordinates['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_east'})
            },
            attrs = {
                'standard_name': variable_config[variable]['standard_name'],
                'long_name': variable_config[variable]['long_name'],
                'units': variable_config[variable]['units'],
            },
        )


    if domain_config["bcsd_forecasts"]['merged_variables'] == True:

        ds = xr.Dataset(
            data_vars = da_dict,
            coords = {
                    'time': ('time', coordinates['time'], {'standard_name': 'time', 'long_name': 'time'}),
                    'ens': ('ens', coordinates['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                    'lat': ('lat', coordinates['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_north'}),
                    'lon': ('lon', coordinates['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_east'})
                },
            attrs = global_config
        )

        ds.to_netcdf(list(bcsd_dict.values())[0], mode='w', format='NETCDF4_CLASSIC', engine='netcdf4', encoding = encoding)

    else:

        for variable in variable_config:
            ds = xr.Dataset(
                data_vars = {variable: da_dict[variable]},
                coords = {
                    'time': ('time', coordinates['time'], {'standard_name': 'time', 'long_name': 'time'}),
                    'ens': ('ens', coordinates['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                    'lat': ('lat', coordinates['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_north'}),
                    'lon': ('lon', coordinates['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_east'})
                },
            attrs = global_config
        )

        ds.to_netcdf(bcsd_dict[variable], mode='w', format='NETCDF4_CLASSIC', engine='netcdf4', encoding = {variable: encoding[variable]})

    return ds


def get_coords_from_files(filename):
    ds = xr.open_dataset(filename)

    #return {
    #    'time': ds['time'].values,
    #    'lat': ds['lat'].values.astype(np.float32),
    #    'lon': ds['lon'].values.astype(np.float32),
    #    'ens': ds['ens'].values
    #}
    
    return {
        'time': ds['time'].values,
        'lat': ds['lat'].values,
        'lon': ds['lon'].values,
        'ens': ds['ens'].values
    }


def preprocess_mdl_hist(filename, month, variable):
    # Open data
    ds = xr.open_mfdataset(filename, chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 10, 'lon': 10}, parallel=True, engine='netcdf4')

    ds = ds[variable]

    # Define time range
    year_start = ds.year.values[0].astype(int)
    year_end = ds.year.values[-1].astype(int)
    nday = len(ds.time.values)

    # Create new time based on day and year
    da_date = da.empty(shape=0, dtype=np.datetime64)
    for yr in range(year_start, year_end + 1):
        date = np.asarray(pd.date_range(f'{yr}-{month}-01 00:00:00', freq="D", periods=nday))
        da_date = da.append(da_date, date)

    # Assign Datetime-Object to new Date coordinate and rename it to "time"
    ds = ds.stack(date=('year', 'time')).assign_coords(date=da_date).rename(date="time")

    return ds



def getCluster(queue, nodes, jobs_per_node):
    
    workers = nodes * jobs_per_node
    
    # cluster options
    if queue == 'rome':
        cores, memory, walltime = (62, '220GB', '0-15:00:00')
    elif queue == 'ivyshort':
        cores, memory, walltime = (40, '60GB', '08:00:00')
    elif queue == 'ccgp' or queue == 'cclake':
        cores, memory, walltime = (38, '170GB', '04:00:00')
    elif queue == 'haswell':
        cores, memory, walltime = (40, '120GB', '08:00:00')
    elif queue == 'ivy':
        cores, memory, walltime = (40, '60GB', '04:00:00')
    elif queue == 'fat':
        cores, memory, walltime = (96, '800GB', '48:00:00')
    
    # cluster if only single cluster is needed!
    cluster = SLURMCluster(
         cores = cores,             
         memory = memory,       
         processes = jobs_per_node,
         local_directory = '/bg/data/NCZarr/temp',  
         queue = queue,
         project = 'dask_test',
         walltime = walltime
    )
    
    client = Client(cluster)
    
    cluster.scale(n=workers)

    return client, cluster


def run_cmd(cmd, path_extra=Path(sys.exec_prefix)/'bin'):
    
    #'''Run a bash command.'''
    env_extra = os.environ.copy()
    env_extra['PATH'] = str(path_extra) + ':' + env_extra['PATH']
    status = run(cmd, check=False, stderr=PIPE, stdout=PIPE, env=env_extra)
    if status.returncode != 0:
        error = f'''{' '.join(cmd)}: {status.stderr.decode('utf-8')}'''
        raise RuntimeError(f'{error}')
    return status.stdout.decode('utf-8')
