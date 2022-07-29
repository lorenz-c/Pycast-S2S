# import packages
import os
from os.path import exists
import datetime as dt
import numpy as np
import netCDF4
import xarray as xr
import pandas as pd
import dask.array as da

import dask
from dask_jobqueue import SLURMCluster
from dask.distributed import Client, LocalCluster



from bc_module import bc_module
from write_output import write_output



def set_metadata(coords, bc_params):
    # Empty Dictionary
    now = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),

    global_attributes = {
        'title': 'Bias-corrected and spatially disaggregated SEAS5-forecasts - version 2.2',
        'references': 'Lorenz, C. et al. (2021): Bias-corrected and spatially disaggregated seasonal forecasts: a long-term reference forecast product for the water sector in semi-arid regions, doi: https://doi.org/10.5194/essd-2020-177',
        'institution': 'Karlsruhe Institute of Technology - Institute of Meteorology and Climate Research',
        'institution-ID': 'https://ror.org/04t3en479',
        'PI': 'Christof Lorenz',
        'originator': 'Christof Lorenz',
        'contact': 'https://imk-ifu.kit.edu',
        'contact_email': 'Christof.Lorenz@kit.edu',
        'operator': 'Clemens Borkenhagen',
        'contributor': 'Jan Weber, Tanja Portele, Harald Kunstmann',
        'comment': f'BCSD-Parameter: {str(bc_params)}',
        'Contact_person': 'Christof Lorenz (Christof.Lorenz@kit.edu)',
        'Author': 'Christof Lorenz (Christof.Lorenz@kit.edu)',
        'Conventions': 'CF-1.8',
        'license': 'CC-BY 4.0',
        'creation_date': f"{now}",
        'source': 'ECMWF SEAS5, ECMWF ERA5-Land',
        'crs': 'EPSG:4326',
        'geospatial_lon_min': min(coords['lon']),
        'geospatial_lon_max': max(coords['lon']),
        'geospatial_lat_min': min(coords['lat']),
        'geospatial_lat_max': max(coords['lat']),
        'StartTime': pd.to_datetime(coords['time'][0]).strftime("%Y-%m-%dT%H:%M:%S"),
        'StopTime': pd.to_datetime(coords['time'][-1]).strftime("%Y-%m-%dT%H:%M:%S")
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
        'lat': ds['lat'].values.astype(np.float32),
        'lon': ds['lon'].values.astype(np.float32),
        'ens': ds['ens'].values
    }


def preprocess_mdl_hist(filename, month):
    # Open data
    ds = xr.open_mfdataset(filename, chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 10, 'lon': 10}, parallel=True, engine='h5netcdf')

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



def slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl, ds_pred, coordinates, window_obs, dry_thresh, precip, low_extrapol, up_extrapol, extremes, intermittency, k):
    #queue_out["time_step"] = k
    # Fill in np.nan for the data
    #queue_out['data'] = np.full([len(coordinates['nens']), len(coordinates['lat']), len(coordinates['lon'])], np.nan)

    day = dayofyear_mdl[k]  # initial day for Month 04

    # create range +- pool
    day_range = (np.arange(day - window_obs, day + window_obs + 1) + 365) % 365 + 1

    # Find days in day_range in dayofyear from obs
    intersection_day_obs = np.in1d(dayofyear_obs, day_range)
    intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)

    # Cut out obs, which correspond to intersected days
    ds_obs_sub = ds_obs.loc[dict(time=intersection_day_obs)]

    # Silence warning of slicing dask array with chunk size
    dask.config.set({"array.slicing.split_large_chunks": False})

    # Cut out mdl, which correspond to intersected days
    ds_mdl_sub = ds_mdl.loc[dict(time=intersection_day_mdl)]
    # Stack "ens" and "time" dimension
    ds_mdl_sub = ds_mdl_sub.stack(ens_time=("ens", "time"))
    ds_mdl_sub = ds_mdl_sub.drop('time')

    # Select pred
    ds_pred_sub = ds_pred.isel(time=k)

    # This is where the magic happens:
    ## apply_u_func apply the function bc_module over each Lat/Lon-Cell, processing the whole time period
    pred_corr_test = xr.apply_ufunc(bc_module, ds_pred_sub, ds_obs_sub, ds_mdl_sub, extremes, low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"], ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize=True, dask="parallelized", output_dtypes=[np.float64])  # , 
    pred_corr_test = pred_corr_test.chunk(chunks={'ens': len(coordinates['nens']), 'lat': len(coordinates['lat']), 'lon':len(coordinates['lon'])})
    
    
    #exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51}) #,  exclude_dims=set(("quantile",)))
    # pred_corr_test = xr.apply_ufunc(bc_module, ds_pred.load(), ds_obs_sub.load(), ds_mdl_sub.load(), extremes, low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"], ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize = True, output_dtypes=[np.float64]).compute() # , exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51}) #,  exclude_dims=set(("quantile",)))

    # Fill NaN-Values with corresponding varfill, varscale and varoffset
    #if varoffset[v] != []:
    #    pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v] + varoffset[v])  # this is needed, because otherwise the conversion of np.NAN to int does not work properly
   # else:
    #   pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v])
    #    queue_out['data'] = pred_corr_test.values

    # Run write_output.ipynb
    #write_output(queue_out)

    return pred_corr_test





def slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl, ds_pred, coordinates, window_obs, dry_thresh, precip, low_extrapol, up_extrapol, extremes, intermittency, k):

    day = dayofyear_mdl[k]  # initial day for Month 04

    # create range +- pool
    day_range = (np.arange(day - window_obs, day + window_obs + 1) + 365) % 365 + 1

    # Find days in day_range in dayofyear from obs
    intersection_day_obs = np.in1d(dayofyear_obs, day_range)
    intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)

    # Cut out obs, which correspond to intersected days
    ds_obs_sub = ds_obs.loc[dict(time=intersection_day_obs)]

    # Silence warning of slicing dask array with chunk size
    dask.config.set({"array.slicing.split_large_chunks": False})

    # Cut out mdl, which correspond to intersected days
    ds_mdl_sub = ds_mdl.loc[dict(time=intersection_day_mdl)]
    # Stack "ens" and "time" dimension
    ds_mdl_sub = ds_mdl_sub.stack(ens_time=("ens", "time"))
    ds_mdl_sub = ds_mdl_sub.drop('time')

    # Select pred
    ds_pred_sub = ds_pred.isel(time=k)

    # This is where the magic happens:
    ## apply_u_func apply the function bc_module over each Lat/Lon-Cell, processing the whole time period
    pred_corr = xr.apply_ufunc(bc_module, ds_pred_sub, ds_obs_sub, ds_mdl_sub, extremes, low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"], ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize=True, dask="parallelized", output_dtypes=[np.float64])  # , 
    pred_corr = pred_corr.chunk(chunks={'ens': len(coordinates['nens']), 'lat': len(coordinates['lat']), 'lon':len(coordinates['lon'])})
    
   
    return pred_corr_test


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
        cores, memory, walltime = (40, '120GB', '01:00:00')
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