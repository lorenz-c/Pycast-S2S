# In this script, the historical SEAS5- and ERA5-Land-Data are processed for each domain

# Packages
import chunk
import os
#from cdo import *
#cdo = Cdo()
import xarray as xr
import numpy as np
import zarr
import dask
import sys

from subprocess import run, PIPE
from pathlib import Path

import logging

from helper_modules import run_cmd, set_encoding

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

def set_and_make_dirs(domain_config):

    #if data_set  == "seas5":
        # Global directory of Domain
        # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'

    # List of Directories
    dir_dict = {
        "seas5_raw_dir":  "/pd/data/regclim_data/gridded_data/seasonal_predictions/seas5/daily",
        "ref_dir":        "/pd/data/regclim_data/gridded_data/reanalyses/era5_land/daily",
        "raw_reg_dir":    f"{domain_config['regroot']}/daily/{domain_config['raw_forecasts']['prefix']}",
        "ref_reg_dir":    f"{domain_config['regroot']}/daily/{domain_config['reference_history']['prefix']}",
        "grd_dir":        f"{domain_config['regroot']}/static",
        "hr_reg_dir":     f"{domain_config['regroot']}/daily/{domain_config['raw_forecasts']['prefix']}_h",
        "lnch_dir":       f"{domain_config['regroot']}/daily/linechunks",
        "climatology":    f"{domain_config['regroot']}/climatology",
        # CAUTION!!! : Prefix and name of directories of existing domains differ (e.g. era5_land (dir) and ERA5_Land (prefix and filenames)!!!!
        "raw_clim":     f"{domain_config['regroot']}/climatology/{domain_config['raw_forecasts']['prefix']}",
        "ref_clim":       f"{domain_config['regroot']}/climatology/{domain_config['reference_history']['prefix']}"
    }


    # Directory of raw SEAS5-Data for each Domain
    

    # Directory of regional grid-File
    

    # Directory of raw, high-resoluted SEAS5-Data
    

    # Directory for raw, high-resoluted SEAS5-Data for the whole time period
    

    # Check if Domain-Directory exist, otherwise create important directories
    # --> Problem: Cannot write into pd/data/...

    if not os.path.isdir(domain_config["regroot"]):
        os.makedirs(domain_config["regroot"])
    if not os.path.isdir(f"{domain_config['regroot']}/daily"):
        os.makedirs(f"{domain_config['regroot']}/daily")

    if not os.path.isdir(dir_dict["raw_reg_dir"]):
        os.makedirs(dir_dict["raw_reg_dir"])
    if not os.path.isdir(dir_dict["ref_reg_dir"]):
        os.makedirs(dir_dict["ref_reg_dir"])
    if not os.path.isdir(dir_dict["hr_reg_dir"]):
        os.makedirs(dir_dict["hr_reg_dir"])
    if not os.path.isdir(dir_dict["grd_dir"]):
        os.makedirs(dir_dict["grd_dir"])
    if not os.path.isdir(dir_dict["lnch_dir"]):
        os.makedirs(dir_dict["lnch_dir"])
    if not os.path.isdir(dir_dict["climatology"]):
        os.makedirs(dir_dict["climatology"])
    if not os.path.isdir(dir_dict["raw_clim"]):
        os.makedirs(dir_dict["raw_clim"])
    if not os.path.isdir(dir_dict["ref_clim"]):
        os.makedirs(dir_dict["ref_clim"])

    return dir_dict



    
def create_grd_file(domain_config, dir_dict):

    min_lon = domain_config["bbox"][0]
    max_lon = domain_config["bbox"][1]
    min_lat = domain_config["bbox"][2]
    max_lat = domain_config["bbox"][3]

    # Create regional mask with desired resolution
    grd_res = domain_config['target_resolution']
    lat_range = int((max_lat - min_lat) / grd_res) + 1
    lon_range = int((max_lon - min_lon) / grd_res) + 1
    grd_size = lat_range * lon_range

    grd_flne = f"{dir_dict['grd_dir']}/{domain_config['prefix']}_{domain_config['target_resolution']}_grd.txt"
    
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

    if not os.path.exists(grd_flne):
        with open(grd_flne, mode = "w") as f:
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
        
    ds               = ds.sortby(ds.lat)
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds               = ds.sortby(ds.lon)
    
    ds['lon'].attrs  = {'standard_name': 'longitude', 'units': 'degrees_east'}
    ds['lat'].attrs  = {'standard_name': 'latitude', 'units': 'degrees_north'}
    
    return ds



            
@dask.delayed
def truncate_forecasts(domain_config, variable_config, dir_dict, year, month_str):

    bbox = domain_config['bbox']

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1
    
    fle_string = f"{dir_dict['seas5_raw_dir']}/{year}/{month_str}/ECMWF_SEAS5_*_{year}{month_str}.nc"
    
    ds = xr.open_mfdataset(fle_string, concat_dim = 'ens', combine = 'nested', parallel = True, chunks = {'time': 50}, engine='netcdf4', preprocess=preprocess, autoclose=True)
    
    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon)).persist()
    
    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32), 'ens': ds['ens'].values}
    
    ds = ds.transpose("time", "ens", "lat", "lon")
    
    encoding = set_encoding(variable_config, coords)
    
    try:
        ds.to_netcdf(f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month_str}_O320_{domain_config['prefix']}.nc", encoding=encoding)
        logging.info(f"Slicing for month {month_str} and year {year} successful")             
    except:
        logging.error(f"Something went wrong during slicing for month {month_str} and year {year}")      


#@dask.delayed
def rechunk_forecasts(domain_config, variable_config, dir_dict, syr_calib, eyr_calib, month):
    
    file_list = []
    
    for year in range(syr_calib, eyr_calib + 1):
        
        file_list.append(f"{dir_dict['hr_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc")
        
        
    ds = xr.open_mfdataset(file_list, parallel = True, engine='netcdf4', autoclose=True, chunks = {'time': 50})
    
    coords = {'time': ds['time'].values, 'ens': ds['ens'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}
    
    encoding = set_encoding(variable_config, coords, 'lines')
    
    final_file = f"{dir_dict['lnch_dir']}/{domain_config['raw_forecasts']['prefix']}_daily__{syr_calib}_{eyr_calib}_{month}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
    
    
    try:
        ds.to_netcdf(final_file, encoding = encoding)
    except:
        logging.error(f"Something went wrong during writing of forecast linechunks")   
    
    
        

def truncate_reference(domain_config, variable_config, dir_dict, syr_calib, eyr_calib):

    bbox = domain_config['bbox']

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1
    
    for variable in variable_config:
            
        fle_list = []
            
        for year in range(syr_calib, eyr_calib + 1):
        
            fle_list.append(f"{dir_dict['ref_dir']}/ERA5_Land_daily_{variable}_{year}.nc")
           
        ds = xr.open_mfdataset(fle_list, parallel = True, chunks = {'time': 50}, engine='netcdf4', preprocess=preprocess, autoclose=True)
            
        ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
            
        coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}
        
        encoding = set_encoding(variable_config, coords)
            
        try:
            ds.to_netcdf(f"{dir_dict['ref_reg_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}_{domain_config['prefix']}_temp.nc", encoding= {variable: encoding[variable]})
        #    ogging.info(f"Truncate reference: Slicing for variable {variable} successful")             
        except:
            logging.error(f"Truncate reference: Something went wrong during truncation for variable {variable}!")    
        
            
def remap_reference(domain_config, variable_config, dir_dict, syr_calib, eyr_calib, grd_fle):  
 #    
    for variable in variable_config:
         
        input_file = f"{dir_dict['ref_reg_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}_{domain_config['prefix']}_temp.nc"
        final_file = f"{dir_dict['ref_reg_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
         
        cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(input_file), str(final_file))
        
        run_cmd(cmd)

def rechunk_reference(domain_config, variable_config, dir_dict, syr_calib, eyr_calib):  
 #    
    for variable in variable_config:
         
        input_file = f"{dir_dict['ref_reg_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
        final_file = f"{dir_dict['lnch_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc"
        
        ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
        
        coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32)}
        
        encoding = set_encoding(variable_config, coords, 'lines')
        
        try:
            ds.to_netcdf(final_file, encoding = {variable: encoding[variable]})
        except:
            logging.error(f"Rechunk reference: Rechunking of reference data failed for variable {variable}!")    
             
        
@dask.delayed
def remap_forecasts(domain_config, dir_dict, year, month, grd_fle):
    
    import logging
    
    coarse_file = f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month}_O320_{domain_config['prefix']}.nc"
    hires_file  = f"{dir_dict['hr_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
    
    print(coarse_file)
    print(hires_file)
    
    try:
        os.path.isfile(coarse_file) 
    except:
        logging.error(f"Remap_forecast: file {coarse_file} not available")
        
        
    #try:
    #cdo.remapbil(grd_fle, input=coarse_file, output=hires_file, options="-f nc4 -k grid -z zip_6")
    cmd = ('cdo', '-O', '-f', 'nc4c', '-z', 'zip_6', f'remapbil,{grd_fle}', str(coarse_file), str(hires_file))
    run_cmd(cmd)
    #    logging.info(f"Remap_forecast: Remapping for year {year} and month {month} successful")
    #except: 
    #    logging.error(f"Remap_forecast: Something went wrong during remapping for month {month} and year {year}")      
        

def create_climatology(domain_config, variable_config, dir_dict, syr_calib, eyr_calib):
    # Open Points:
    # --> Prefix and Directories does not match (era5_land / ERA5_Land) --> Issue
    # --> Alles einheitlich bennen für die Funktionen --> Issue
    # --> Encoding while writing netcdf????
    # --> Loop over months for SEAS5???
    # --> Write one function and choose ref or seas5 product???
    # --> Choose baseline period with start and end year (up to now, all files in the folder are selected and opened)
    # set up encoding for netcdf-file later



    #### SEAS5
    # loop over month
    for month in range(1,13):
        # Select period of time
        fle_list = []
        for year in range(syr_calib, eyr_calib + 1):
            fle_list.append(f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{str(month).zfill(2)}_O320_{domain_config['prefix']}.nc")

        # load nc-Files for each month over all years
        ds = xr.open_mfdataset(fle_list, parallel = True, engine='netcdf4')
        # Calculate climatogloy (mean) for each lead month
        ds_clim = ds.groupby('time.month').mean('time')
        ds_clim = ds_clim.rename({'month': 'time'})
        # set encoding
        coords = {'time': ds_clim['time'].values, 'ens': ds_clim['ens'].values, 'lat': ds_clim['lat'].values.astype(np.float32), 'lon': ds_clim['lon'].values.astype(np.float32)}
        encoding = set_encoding(variable_config, coords, 'lines')

        # Save NC-File
        try:
            ds_clim.to_netcdf(f"{dir_dict['seas5_clim']}/{domain_config['raw_forecasts']['prefix']}_climatology_{syr_calib}_{eyr_calib}_{str(month).zfill(2)}_0320_{domain_config['prefix']}.nc", encoding = encoding)
        except:
            logging.error(f"Calculate climatology of SEAS5: Climatology for month {str(month).zfill(2)} failed!")


    #### Ref - ERA5
    # loop over variables
    for variable in domain_config['variables']:
        # Select period of time
        fle_list = []
        for year in range(syr_calib, eyr_calib + 1):
            fle_list.append(f"{dir_dict['ref_reg_dir']}/{domain_config['reference_history']['prefix']}_daily_{variable}_{year}_{domain_config['prefix']}.nc")

        # Open dataset
        ds = xr.open_mfdataset(fle_list, parallel = True, engine='netcdf4')
        # Calculate climatogloy (mean)
        ds_clim  = ds.groupby('time.month').mean('time')
        ds_clim = ds_clim.rename({'month': 'time'})
        # set encoding
        coords = {'time': ds_clim['time'].values, 'lat': ds_clim['lat'].values.astype(np.float32), 'lon': ds_clim['lon'].values.astype(np.float32)}
        encoding = set_encoding(variable_config, coords, 'lines')
        # Save NC-File
        try:
            ds_clim.to_netcdf(f"{dir_dict['ref_clim']}/{domain_config['reference_history']['prefix']}_climatology_{variable}_{syr_calib}_{eyr_calib}_{domain_config['prefix']}.nc", encoding = {variable: encoding[variable]})
        except:
            logging.error(f"Calculate climatology of Ref: Climatology for variable {variable} failed!")





