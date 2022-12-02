# In this script, the historical SEAS5- and ERA5-Land-Data are processed for each domain

# Packages
import chunk
import os
# from cdo import *
# cdo = Cdo()
import xarray as xr
import numpy as np
# import zarrimpo
import dask
import sys
import pandas as pd

from subprocess import run, PIPE
from pathlib import Path

import logging

from helper_modules import run_cmd, set_encoding
import dir_fnme

# Open Points
# 1. Paths are local, change them (pd/data)
# 2. Get information out of the parameter-file (which has to be changed, according to Christof's draft)
# 3. Global attributes for nc-Files --> check, how the historic raw seas5-files for other domains have been built and rebuilt for new domains
#  --> Change overall settings for nc-Files (global attributes, vars, etc.) within the module.py, so that it can be used for all cases within the BCSD


###### SEAS5 #######
# Steps:
# 1. Load global SEAS5-Dataset
# 2. Cut out domain of interest
# 3. Remap to local grid
# 4. Store as high resolution dataset for the specific domain

global bbox


def create_grd_file(domain_config: dict, dir_dict: dict, fnme_dict: dict) -> str:
    """Creates a grid description file that is used for remapping the forecasts to the final resolution."""
    min_lon = domain_config["bbox"][0]
    max_lon = domain_config["bbox"][1]
    min_lat = domain_config["bbox"][2]
    max_lat = domain_config["bbox"][3]

    # Create regional mask with desired resolution
    grd_res = domain_config['target_resolution']
    lat_range = int((max_lat - min_lat) / grd_res) + 1
    lon_range = int((max_lon - min_lon) / grd_res) + 1
    grd_size = lat_range * lon_range

    grd_flne = f"{dir_dict['grd_dir']}/{fnme_dict['grd_dir']}"

    # if file does not exist, create regional text file for domain with desired resolution
    # --> Can be implemented and outsourced as function !!!!!
    content = [
        f"# regional grid\n",
        f"# domain: {domain_config['prefix']}\n",
        f"# grid resolution: {str(grd_res)}\n",
        f"gridtype = lonlat\n",
        f"gridsize = {str(grd_size)}\n",
        f"xsize = {str(lon_range)}\n",
        f"ysize = {str(lat_range)}\n",
        f"xname = lon\n",
        f"xlongname = Longitude\n",
        f"xunits = degrees_east\n",
        f"yname = lat\n",
        f"ylongname = Latitude\n",
        f"yunits = degrees_north\n",
        f"xfirst = {str(min_lon)}\n",
        f"xinc = {str(grd_res)}\n",
        f"yfirst = {str(min_lat)}\n",
        f"yinc = {str(grd_res)}\n"
    ]

    if not os.path.isfile(grd_flne):
        with open(grd_flne, mode="w") as f:
            f.writelines(content)
    else:
        print("File for regional grid already exists")

    return grd_flne


def preprocess(ds):
    # ADD SOME CHECKS HERE THAT THIS STUFF IS ONLY APPLIED WHEN LATITUDES ARE REVERSED AND LONGITUDES GO FROM 0 TO 360
    if 'longitude' in ds.variables:
        ds = ds.rename({'longitude': 'lon'})

    if 'latitude' in ds.variables:
        ds = ds.rename({'latitude': 'lat'})

    ds = ds.sortby(ds.lat)
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds = ds.sortby(ds.lon)

    ds['lon'].attrs = {'standard_name': 'longitude', 'units': 'degrees_east'}
    ds['lat'].attrs = {'standard_name': 'latitude', 'units': 'degrees_north'}

    return ds


@dask.delayed
def truncate_forecasts(domain_config: dict, variable_config: dict, dir_dict: dict, year: int, month: int):
    
    month_str = str(month).zfill(2)
    bbox = domain_config['bbox']

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1

    # Update Filenames
    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"])

    fle_string = f"{dir_dict['frcst_low_glob_dir']}/{year}/{month_str}/{fnme_dict['frcst_low_glob_dir']}"

    print(fle_string)
    
    # ds = xr.open_mfdataset(fle_string, concat_dim = 'ens', combine = 'nested', parallel = True, chunks = {'time': 50}, engine='netcdf4', preprocess=preprocess, autoclose=True)
    ds = xr.open_mfdataset(fle_string, concat_dim='ens', combine='nested', parallel=True, chunks={'time': 50},
                           preprocess=preprocess)

    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon)).persist()

    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32),
              'lon': ds['lon'].values.astype(np.float32), 'ens': ds['ens'].values}

    ds = ds.transpose("time", "ens", "lat", "lon")

    encoding = set_encoding(variable_config, coords)

    # Write all variables in one file, or write out the individual variables in sepperate files
    if domain_config["raw_forecasts"]["merged_variables"] == True:
        try:
            # Update Filenames
            fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"])

            ds.to_netcdf(f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}", encoding=encoding)
            logging.info(f"Slicing for month {month_str} and year {year} successful")
        except:
            logging.error(f"Something went wrong during slicing for month {month_str} and year {year}")
    else:
        for variable in variable_config:
            try:
                # Update Filenames
                fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"], variable)

                ds[variable].to_netcdf(f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}",
                                       encoding={variable: encoding[variable]})
                logging.info(f"Slicing for month {month_str} and year {year} successful")
            except:
                logging.error(f"Something went wrong during slicing for month {month_str} and year {year}")


@dask.delayed
def remap_forecasts(domain_config: dict, variable_config: dict, dir_dict: dict, year: int, month: int, grd_fle: str, variable: str):
    import logging

    month_str = str(month).zfill(2)
    
    #if domain_config['reference_history']['merged_variables'] == True:
    #    # Update Filenames
    #    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"])

    #    coarse_file = f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}"
    #    hires_file = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

    #    try:
    #        os.path.isfile(coarse_file)
    #    except:
    #        logging.error(f"Remap_forecast: file {coarse_file} not available")

        # try:
        # cdo.remapbil(grd_fle, input=coarse_file, output=hires_file, options="-f nc4 -k grid -z zip_6")
    #    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(coarse_file), str(hires_file))
    #    run_cmd(cmd)
        #    logging.info(f"Remap_forecast: Remapping for year {year} and month {month} successful")
        # except:
        #    logging.error(f"Remap_forecast: Something went wrong during remapping for month {month} and year {year}")

    #else:
    #    for variable in variable_config:
            # Update Filenames
    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"], variable)
    coarse_file = f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}"
    hires_file = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"
            # print(coarse_file)
            # print(hires_file)

    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(coarse_file), str(hires_file))
    
    try:
        os.path.isfile(coarse_file)
        run_cmd(cmd)
    except:
        logging.error(f"Remap_forecast: file {coarse_file} not available")

            # try:
            # cdo.remapbil(grd_fle, input=coarse_file, output=hires_file, options="-f nc4 -k grid -z zip_6")

            
            #    logging.info(f"Remap_forecast: Remapping for year {year} and month {month} successful")
            # except:
            #    logging.error(f"Remap_forecast: Something went wrong during remapping for month {month} and year {year}")


@dask.delayed
def rechunk_forecasts(domain_config: dict, variable_config: dict, dir_dict: dict, year: int, month: int, variable: str):
    
    month_str = str(month).zfill(2)
    
    #if domain_config['reference_history']['merged_variables'] == True:
    #    # Update Filenames
    #    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"])

    #    fle_string = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

    #    ds = xr.open_mfdataset(fle_string, parallel=True, engine='netcdf4', autoclose=True, chunks={'time': 50})

    #    coords = {'time': ds['time'].values, 'ens': ds['ens'].values, 'lat': ds['lat'].values.astype(np.float32),
    #              'lon': ds['lon'].values.astype(np.float32)}

    #    encoding = set_encoding(variable_config, coords, 'lines')

    #    final_file = f"{dir_dict['frcst_high_reg_lnch_dir']}/{fnme_dict['frcst_high_reg_lnch_dir']}"

    #    try:
    #        ds.to_netcdf(final_file, encoding=encoding)
    #        logging.info(f"Rechunking forecast for {month_str} successful")
    #    except:
    #        logging.error(f"Something went wrong during writing of forecast linechunks")
    #else:
    #    for variable in variable_config:
            # Update Filenames
    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"], variable)

    fle_string = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

    ds = xr.open_mfdataset(fle_string, parallel=True, engine='netcdf4', autoclose=True, chunks={'time': 50})
    # ds = xr.open_mfdataset(fle_string, parallel =True, engine='netcdf4', autoclose=True, chunks={'time': 215, 'ens': 25, 'lat': 1, 'lon': 1})
    # ds = xr.open_mfdataset(fle_string)
    coords = {'time': ds['time'].values, 'ens': ds['ens'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}
    # chunks = {"time": 1}
    encoding = set_encoding(variable_config, coords, 'lines')

    # ds = ds.chunk(chunks={'time': len(ds['time'].values), 'ens': len(ds['ens'].values), 'lat': 1, 'lon': 1})

    # chunksizes = [len(coords['time']), len(coords['ens']), 1, 1]

    final_file = f"{dir_dict['frcst_high_reg_lnch_dir']}/{fnme_dict['frcst_high_reg_lnch_dir']}"

    # ds.to_netcdf(final_file, encoding={variable: encoding[variable]})
    # ds.close()
    # ds.to_netcdf(final_file)


    try:
        ds.to_netcdf(final_file, encoding={variable: encoding[variable]})
        logging.info(f"Rechunking forecast for {month_str} successful")
        ds.close()
    except:
        logging.error(f"Something went wrong during writing of forecast linechunks")


def calib_forecasts(domain_config, variable_config, dir_dict, month_str):
    file_list = []
    syr =  domain_config['syr_calib']
    eyr =  domain_config['eyr_calib']

    if domain_config['reference_history']['merged_variables'] == True:
        for year in range(syr, eyr + 1):
            # Update Filenames
            fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"])

            file_list.append(f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}")

        ds = xr.open_mfdataset(file_list, parallel=True, engine='netcdf4', autoclose=True, chunks={'time': 50})

        coords = {'time': ds['time'].values, 'ens': ds['ens'].values, 'lat': ds['lat'].values.astype(np.float32),
                  'lon': ds['lon'].values.astype(np.float32)}

        encoding = set_encoding(variable_config, coords, 'lines')

        final_file = f"{dir_dict['frcst_high_reg_lnch_calib_dir']}/{fnme_dict['frcst_high_reg_lnch_calib_dir']}"

        try:
            ds.to_netcdf(final_file, encoding=encoding)
            logging.info(f"Rechunking forecast for {month_str} successful")
        except:
            logging.error(f"Something went wrong during writing of forecast linechunks")
    else:
        for variable in variable_config:
            for year in range(syr, eyr + 1):
                # Update Filenames
                fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"], variable)

                file_list.append(f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}")

            ds = xr.open_mfdataset(file_list, parallel=True, engine='netcdf4', autoclose=True, chunks={'time': 50})

            coords = {'time': ds['time'].values, 'ens': ds['ens'].values, 'lat': ds['lat'].values.astype(np.float32),
                      'lon': ds['lon'].values.astype(np.float32)}

            encoding = set_encoding(variable_config, coords, 'lines')

            final_file = f"{dir_dict['frcst_high_reg_lnch_calib_dir']}/{fnme_dict['frcst_high_reg_lnch_calib_dir']}"

            try:
                ds.to_netcdf(final_file, encoding={variable: encoding[variable]})
                logging.info(f"Rechunking forecast for {month_str} successful")
            except:
                logging.error(f"Something went wrong during writing of forecast linechunks")


def preprocess_reference(ds):
    # Create new and unique time values
    # set year
    year = ds.time.dt.year.values[0]
    # save attributes
    time_attr = ds.time.attrs

    # Create time values
    time_values = pd.date_range(f"{year}-01-01 12:00:00", f"{year}-12-31 12:00:00")

    # Set time values
    ds = ds.assign_coords({"time": time_values})
    ds.time.attrs = {"standard_name": time_attr['standard_name'], 'long_name': time_attr['long_name'],
                     'axis': time_attr['axis']}

    # some other preprocessing
    if 'longitude' in ds.variables:
        ds = ds.rename({'longitude': 'lon'})

    if 'latitude' in ds.variables:
        ds = ds.rename({'latitude': 'lat'})

    ds = ds.sortby(ds.lat)
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds = ds.sortby(ds.lon)

    ds['lon'].attrs = {'standard_name': 'longitude', 'units': 'degrees_east'}
    ds['lat'].attrs = {'standard_name': 'latitude', 'units': 'degrees_north'}

    # Drop var "time bounds" if necessary, otherwise cant be merged
    try:
        ds = ds.drop_dims("bnds")
    except:
        print(f"No bnds dimension available")

    return ds


@dask.delayed
def truncate_reference(domain_config: dict, variable_config: dict, dir_dict: dict, fnme_dict: dict, fle_string: str, variable: str):
    bbox = domain_config['bbox']

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1

    ds = xr.open_mfdataset(fle_string, parallel=True, chunks={'time': 50}, engine='netcdf4',
                           preprocess=preprocess_reference, autoclose=True)

    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))

    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32),
              'lon': ds['lon'].values.astype(np.float32)}

    encoding = set_encoding(variable_config, coords)

    try:
        if domain_config['reference_history']['merged_variables'] == True:
            ds.to_netcdf(f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}", encoding=encoding)
        else:
            ds.to_netcdf(f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}",
                         encoding={variable: encoding[variable]})
    #    logging.info(f"Truncate reference: Slicing for variable {variable} successful")
    except:
        logging.error(f"Truncate reference: Something went wrong during truncation for variable {variable}!")

@dask.delayed
def remap_reference(domain_config: dict, variable_config: dict, dir_dict: dict, year: int, month: int, grd_fle: str, variable: str):
    import logging
    
    month_str = str(month).zfill(2)

    #if domain_config['reference_history']['merged_variables'] == True:
        # Update Filenames
    #    fnme_dict   = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['reference_history']['merged_variables'])
    #    coarse_file = f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}"
    #    hires_file  = f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"
        
    #    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(coarse_file), str(hires_file))

    #    try:
    #        os.path.isfile(coarse_file)
    #    except:
    #        logging.error(f"Remap_forecast: file {coarse_file} not available")

        

    #    run_cmd(cmd)
    #else:
    #    for variable in variable_config:
            # Update Filenames
            
    fnme_dict   = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['reference_history']['merged_variables'], variable)
    coarse_file = f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}"
    hires_file  = f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"
             
    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(coarse_file), str(hires_file))

    try:
        os.path.isfile(coarse_file)
        run_cmd(cmd)
    except:
        logging.error(f"Remap_forecast: file {coarse_file} not available")
  

@dask.delayed
def rechunk_reference(domain_config: dict, variable_config: dict, dir_dict: dict, year: int, month: int, variable: str):
    
    month_str = str(month).zfill(2)
    
    #if domain_config['reference_history']['merged_variables'] == True:
        # Update Filenames:
    #    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str,
    #                                       domain_config['reference_history']['merged_variables'])

    #    fle_string = f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"

        # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    #    ds = xr.open_mfdataset(fle_string, parallel=True, chunks={'time': 50}, engine='netcdf4', preprocess=preprocess, axcbvxcbvcxbvbvnbvnbvnutoclose=True)

    #    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}

    #    encoding = set_encoding(variable_config, coords, 'lines')

     ##   try:
    #        ds.to_netcdf(f"{dir_dict['ref_high_reg_daily_lnch_dir']}/{fnme_dict['ref_high_reg_daily_lnch_dir']}", encoding=encoding)
    #    except:
    #        logging.error(f"Rechunk reference: Rechunking of reference data failed!")
    #else:
    #    for variable in variable_config:
        # Update Filenames:
    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['reference_history']['merged_variables'], variable)

    fle_string = f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"

        # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    ds = xr.open_mfdataset(fle_string, parallel=True, chunks={'time': 50}, engine='netcdf4',
                                   preprocess=preprocess, autoclose=True)

    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32),
                      'lon': ds['lon'].values.astype(np.float32)}

    encoding = set_encoding(variable_config, coords, 'lines')

    try:
        ds.to_netcdf(f"{dir_dict['ref_high_reg_daily_lnch_dir']}/{fnme_dict['ref_high_reg_daily_lnch_dir']}",
                             encoding={variable: encoding[variable]})
    except:
        logging.error(f"Rechunk reference: Rechunking of reference data failed for variable {variable}!")

@dask.delayed
def calib_reference(domain_config: dict, variable_config: dict, dir_dict: dict, syr: int, eyr: int, variable: str):
    
    fle_list = []
    
    #if domain_config['reference_history']['merged_variables'] == True:
    #    for year in range(syr, eyr + 1):
    #        # Update filenames
    #        month_str = "01"  # dummy
    #        fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['reference_history']['merged_variables'])#

    #        fle_list.append(f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}")

    #    # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    #    ds = xr.open_mfdataset(fle_list, parallel=True, chunks={'time': 50}, engine='netcdf4', autoclose=True)

    #    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32),
    #              'lon': ds['lon'].values.astype(np.float32)}

    #    encoding = set_encoding(variable_config, coords, 'lines')

    #    try:
    #        ds.to_netcdf(
    #            f"{dir_dict['ref_high_reg_daily_lnch_calib_dir']}/{fnme_dict['ref_high_reg_daily_lnch_calib_dir']}",
    #            encoding=encoding)
    #    except:
    #        logging.error(f"Rechunk reference: Rechunking of reference data failed for variable!")
            
    #else:
        
    #    for variable in variable_config:
            
    for year in range(syr, eyr + 1):
        # Update filenames
        month_str = "01"  # dummy
        fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['reference_history']['merged_variables'], variable)

        fle_list.append(f"{dir_dict['ref_high_reg_daily_lnch_dir']}/{fnme_dict['ref_high_reg_daily_lnch_dir']}")

            # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
            
    ds = xr.open_mfdataset(fle_list, parallel=True, engine='netcdf4', autoclose=True)

    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}

    encoding = set_encoding(variable_config, coords, 'lines')

    try:
        ds.to_netcdf(f"{dir_dict['ref_high_reg_daily_lnch_calib_dir']}/{fnme_dict['ref_high_reg_daily_lnch_calib_dir']}",
                    encoding={variable: encoding[variable]})
    except:
        logging.error(f"Rechunk reference: Rechunking of reference data failed for variable {variable}!")


def create_climatology(dataset, domain_config, variable_config, dir_dict, syr_calib, eyr_calib, month_str):
    # Open Points:
    # --> Prefix and Directories does not match (era5_land / ERA5_Land) --> Issue
    # --> Alles einheitlich bennen für die Funktionen --> Issue
    # --> Encoding while writing netcdf????
    # --> Loop over months for SEAS5???
    # --> Write one function and choose ref or seas5 product???
    # --> Choose baseline period with start and end year (up to now, all files in the folder are selected and opened)
    # set up encoding for netcdf-file later

    # SEAS5
    if dataset == "seas5":
        fle_list = []
        if domain_config['reference_history']['merged_variables'] == True:
            # Select period of time
            for year in range(syr_calib, eyr_calib + 1):
                # Update filenames
                fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str,
                                                   domain_config['reference_history']['merged_variables'])

                fle_list.append(f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}")

            # load nc-Files for each month over all years
            ds = xr.open_mfdataset(fle_list, parallel=True, engine='netcdf4')
            # Calculate climatogloy (mean) for each lead month
            ds_clim = ds.groupby('time.month').mean('time')
            ds_clim = ds_clim.rename({'month': 'time'})
            # set encoding
            coords = {'time': ds_clim['time'].values, 'ens': ds_clim['ens'].values,
                      'lat': ds_clim['lat'].values.astype(np.float32), 'lon': ds_clim['lon'].values.astype(np.float32)}
            encoding = set_encoding(variable_config, coords, 'lines')

            # Save NC-File
            try:
                ds_clim.to_netcdf(f"{dir_dict['frcst_climatology']}/{fnme_dict['frcst_climatology']}",
                                  encoding=encoding)
            except:
                logging.error(f"Calculate climatology of SEAS5: Climatology for month {month_str} failed!")
        else:
            for variable in variable_config:
                # Select period of time
                for year in range(syr_calib, eyr_calib + 1):
                    # Update filenames
                    fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str,
                                                       domain_config['reference_history']['merged_variables'], variable)

                    fle_list.append(f"{dir_dict['frcst_low_reg_dir']}/{fnme_dict['frcst_low_reg_dir']}")

                # load nc-Files for each month over all years
                ds = xr.open_mfdataset(fle_list, parallel=True, engine='netcdf4')
                # Calculate climatogloy (mean) for each lead month
                ds_clim = ds.groupby('time.month').mean('time')
                ds_clim = ds_clim.rename({'month': 'time'})
                # set encoding
                coords = {'time': ds_clim['time'].values, 'ens': ds_clim['ens'].values,
                          'lat': ds_clim['lat'].values.astype(np.float32),
                          'lon': ds_clim['lon'].values.astype(np.float32)}
                encoding = set_encoding(variable_config, coords, 'lines')

                # Save NC-File
                try:
                    ds_clim.to_netcdf(f"{dir_dict['frcst_climatology']}/{fnme_dict['frcst_climatology']}",
                                      encoding={variable: encoding[variable]})
                except:
                    logging.error(f"Calculate climatology of SEAS5: Climatology for month {month_str} failed!")


    #### Ref - ERA5
    else:
        # Select period of time
        fle_list = []
        if domain_config['reference_history']['merged_variables'] == True:
            for year in range(syr_calib, eyr_calib + 1):
                # Update filenames
                fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str,
                                                   domain_config['reference_history']['merged_variables'])

                fle_list.append(f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}")

            # Open dataset
            ds = xr.open_mfdataset(fle_list, parallel=True, engine='netcdf4')
            # Calculate climatogloy (mean)
            ds_clim = ds.groupby('time.month').mean('time')
            ds_clim = ds_clim.rename({'month': 'time'})
            # set encoding
            coords = {'time': ds_clim['time'].values, 'lat': ds_clim['lat'].values.astype(np.float32),
                      'lon': ds_clim['lon'].values.astype(np.float32)}
            encoding = set_encoding(variable_config, coords, 'lines')
            # Save NC-File
            try:
                ds_clim.to_netcdf(f"{dir_dict['ref_climatology']}/{fnme_dict['ref_climatology']}", encoding=encoding)
            except:
                logging.error(f"Calculate climatology of Ref: Climatology for variable failed!")
        else:
            for variable in variable_config:
                for year in range(syr_calib, eyr_calib + 1):
                    # Update filenames
                    fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str,
                                                       domain_config['reference_history']['merged_variables'], variable)

                    fle_list.append(f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}")

                # Open dataset
                ds = xr.open_mfdataset(fle_list, parallel=True, engine='netcdf4')
                # Calculate climatogloy (mean)
                ds_clim = ds.groupby('time.month').mean('time')
                ds_clim = ds_clim.rename({'month': 'time'})
                # set encoding
                coords = {'time': ds_clim['time'].values, 'lat': ds_clim['lat'].values.astype(np.float32),
                          'lon': ds_clim['lon'].values.astype(np.float32)}
                encoding = set_encoding(variable_config, coords, 'lines')
                # Save NC-File
                try:
                    ds_clim.to_netcdf(f"{dir_dict['ref_climatology']}/{fnme_dict['ref_climatology']}",
                                      encoding={variable: encoding[variable]})
                except:
                    logging.error(f"Calculate climatology of Ref: Climatology for variable {variable} failed!")


def calc_quantile_thresh(domain_config, dir_dict, syr_calib, eyr_calib, month_str):
    # Create empty file list
    file_lst = []
    # Load all years of specific months in one file
    for year in range(syr_calib, eyr_calib + 1):
        # Update Filenames
        fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str)

        ##### Monthly Filename = .../domain/monthly/SEAS5_BCSD/SEAS5_BCSD_V3.0_monthly_198101_0.1_Chira.nc"
        file_lst.append(
            f"{dir_dict['monthly_bcsd']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc")

    # Load file list at once
    ds = xr.open_mfdataset(file_lst, parallel=True, engine='netcdf4')
    # This step depends on how the files will look like and if they have the variable "time_bnds"
    ds = ds.drop_vars("time_bnds")

    # Necessary otherwise error
    ds = ds.chunk(dict(time=-1))

    # Calculate quantile, tercile and extremes on a monthly basis
    ds_quintiles = ds.groupby('time.month').quantile(q=[0.2, 0.4, 0.6, 0.8], dim=['time', 'ens'])
    ds_tercile = ds.groupby('time.month').quantile(q=[0.33, 0.66], dim=['time', 'ens'])
    ds_extreme = ds.groupby('time.month').quantile(q=[0.1, 0.9], dim=['time', 'ens'])

    # Save NC-File
    ### ENCODING?!
    try:
        ds_quintiles.to_netcdf(
            f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_quintiles_{syr_calib}_{eyr_calib}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc")
    except:
        logging.error(f"Error: Create NC-File for quantiles")

    try:
        ds_tercile.to_netcdf(
            f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_tercile_{syr_calib}_{eyr_calib}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc")
    except:
        logging.error(f"Error: Create NC-File for tercile")

    try:
        ds_extreme.to_netcdf(
            f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_extreme_{syr_calib}_{eyr_calib}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc")
    except:
        logging.error(f"Error: Create NC-File for extreme")