# In this script, the historical SEAS5- and ERA5-Land-Data are processed for each domain

# Packages
import chunk
import os
from cdo import *
cdo = Cdo()
import xarray as xr
import numpy as np
import modules
import zarr
import dask

import logging

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

    # Directory of raw SEAS5-Data
    dir_dict = {
        "seas5_raw_dir":  "/pd/data/regclim_data/gridded_data/seasonal_predictions/seas5/daily",
        "raw_reg_dir":    f"{domain_config['regroot']}/daily/{domain_config['raw_forecasts']['prefix']}",
        "grd_dir":        f"{domain_config['regroot']}/static",
        "hr_reg_dir":     f"{domain_config['regroot']}/daily/{domain_config['raw_forecasts']['prefix']}_h",
        "lnch_dir":       f"{domain_config['regroot']}/daily/linechunks"
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
    if not os.path.isdir(dir_dict["hr_reg_dir"]):
        os.makedirs(dir_dict["hr_reg_dir"])
    if not os.path.isdir(dir_dict["grd_dir"]):
        os.makedirs(dir_dict["grd_dir"])
    if not os.path.isdir(dir_dict["lnch_dir"]):
        os.makedirs(dir_dict["lnch_dir"])

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


#def setup_domain(domain_config, global_config, variable_config, ref_hist_dict, mdl_hist_dict):




    #smonth_calib = 1
    #emonth_claib = 2

    # Set number of ensembles
    #number_ens = 2

def preprocess(ds):
    # ADD SOME CHECKS HERE THAT THIS STUFF IS ONLY APPLIED WHEN LATITUDES ARE REVERSED AND LONGITUDES GO FROM 0 TO 360   
    ds               = ds.sortby(ds.lat)
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds               = ds.sortby(ds.lon)
    
    return ds


def prepare_forecasts(domain_config, variable_config, dir_dict):

    
    bbox = domain_config['bbox']

    min_lon = bbox[0]
    max_lon = bbox[1]
    min_lat = bbox[2]
    max_lat = bbox[3]


    # Set calibration time (year, month)
    syr_calib = domain_config["syr_calib"]
    eyr_calib = domain_config["eyr_calib"]

    for year in range(syr_calib, eyr_calib + 1):

        for month in range(1, 13):
            month_str = str(month).zfill(2)

            print(f"{year}{month_str}")
            fle_list = []
            for ens in range(0, 25):
                ens_str = str(ens).zfill(2)
                fle_list.append(f"{dir_dict['seas5_raw_dir']}/{year}/{month_str}/ECMWF_SEAS5_{ens_str}_{year}{month_str}.nc")



            ds = xr.open_mfdataset(fle_list, concat_dim = 'ens', combine = 'nested', chunks = {'time': 10}, parallel = True, engine='netcdf4', preprocess=preprocess)

            ds = ds[domain_config['variables']]

            #ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon)).persist()

            coords = {
                'time': ds['time'].values,
                'lat': ds['lat'].values.astype(np.float32),
                'lon': ds['lon'].values.astype(np.float32),
                'ens': ds['ens'].values
            }

            ds = ds.transpose("time", "ens", "lat", "lon")

            encoding = modules.set_encoding(variable_config, coords)

            #for var in encoding:
            #    encoding[var]['compressor'] = zarr.Blosc(cname="zstd", clevel=5, shuffle=2)
#            try:
#                ds.to_netcdf(f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month_str}_O320_{domain_config['prefix']}.nc", encoding=encoding)
#            except:
#                print(f"Writing of file for {year}{month})


            
@dask.delayed
def prepare_forecast_dask(domain_config, variable_config, dir_dict, year, month_str):

    bbox = domain_config['bbox']

    min_lon = bbox[0]
    max_lon = bbox[1]
    min_lat = bbox[2]
    max_lat = bbox[3]
    
    fle_string = f"{dir_dict['seas5_raw_dir']}/{year}/{month_str}/ECMWF_SEAS5_*_{year}{month_str}.nc"
    
    ds = xr.open_mfdataset(fle_string, concat_dim = 'ens', combine = 'nested', parallel = True, chunks = {'time': 50}, engine='netcdf4', preprocess=preprocess, autoclose=True)
    
    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
    
    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon': ds['lon'].values.astype(np.float32), 'ens': ds['ens'].values}
    
    ds = ds.transpose("time", "ens", "lat", "lon")
    
    encoding = modules.set_encoding(variable_config, coords)
    
    log = logging.getLogger(__name__)
    
    try:
        ds.to_netcdf(f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year}{month_str}_O320_{domain_config['prefix']}.nc", encoding=encoding)
        log.info(f"Slicing for month {month_str} and year {year} successful")             
    except:
        log.error(f"Something went wrong during slicing for month {month_str} and year {year}")      

    #fle_list = []
    #        or ens in range(0, 25):
    #            ens_str = str(ens).zfill(2)
     #           fle_list.append(f"{dir_dict['seas5_raw_dir']}/{year}/{month_str}/ECMWF_SEAS5_{ens_str}_{year}{month_str}.nc")


     

     # loop over all years
    #for month in range (1, 3):

    #    month_str = str(month).zfill(2)

    #    ens_list = []

    #    for year in range(syr_calib, eyr_calib+1):

    #        tmp = xr.open_mfdataset(f"{dir_dict['seas5_raw_dir']}/{year}/{month_str}/ECMWF_SEAS5_*_{year}{month_str}.nc", concat_dim = 'ens', combine = 'nested', chunks = {'time': 1}, parallel = True, preprocess=preprocess)
    #        tmp = tmp[domain_config['variables']]

    #        ens_list.append(tmp)


   #    ds = xr.concat(ens_list, dim='time')

    #    ds_reg = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon)


    #f#or year in range(syr_calib, eyr_calib+1):
     #   year_str = str(year)
     #   # loop over all months
       # for month in range (1, 3):
            
     #       print(f"{year_str}, {month_str}")
            # Open raw SEAS5-data and merge all ensemble member in one file

          #  ds = xr.open_mfdataset(f"{dir_dict['seas5_raw_dir']}/{year_str}/{month_str}/*.nc", concat_dim = "ens", combine = "nested", chunks={'time': 20}, parallel = True, preprocess=preprocess)

           # ds = ds[domain_config['variables']].persist()

        
            
            # Cut out domain
       #     ds_reg = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))


        #    coords = {
        #        'time': ds_reg['time'].values,
        #        'lat': ds_reg['lat'].values.astype(np.float32),
        #        'lon': ds_reg['lon'].values.astype(np.float32),
        #        'ens': ds_reg['ens'].values
        #    }

         #   ds_reg = ds_reg.transpose("time", "ens", "lat", "lon")

        #    encoding = modules.set_encoding(variable_config, coords)

        #    ds_reg.to_netcdf(f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year_str}{month_str}_O320_{domain_config['prefix']}.nc", encoding=encoding)



def remap_forecasts(domain_config, grd_fle):

    for year in range(syr_calibdomain_config["syr_calib"], domain_config["eyr_calib"]+1):

        year_str = str(year)

        for month in range (1, 3):

            month_str = str(month).zfill(2)

            seas5_raw_flne  = f"{dir_dict['raw_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year_str}{month_str}_O320_{domain_config['prefix']}.nc"
            seas5_high_flne = f"{dir_dict['hr_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_daily_{year_str}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
            cdo.remapbil(grd_flne, input=seas5_raw_flne, output=seas5_high_flne, options="-f nc4 -k grid -z zip_6 -P 10")

## Domain Name
#domain = 'Germany'

## Set domain limits
#bbox =  [5.25,15.15,45.55,55.45]



    # Set directories
    # --> HARDCODED!!!!! Change directories to pd/data!!!

    # which dataset
   # data_set = "era5_land"






    # Create text-file for regional grid
    # filename of regional grid





                # set coordinates
                #coordinates = {"time": ds_reg.time.values,
                #                "ens": ds_reg.ens.values,
                #                "lat": ds_reg.lat.values,
                #                "lon": ds_reg.lon.values}

                # Setup meta-data
                #sinfo = {"domain": domain,
                 #    "  resolution": 0.3}

            #glb_attr, var_attr = modules.set_metadata(sinfo, 15, 15)

            # create empty netcdf-File
            #modules.create_4d_netcdf(reg_dir + 'SEAS5_daily_' + year_str + month_str + '_O320_' + domain + '.nc', glb_attr, var_attr, coordinates)

            # Save regional xarray to existing netcdf4-File
            # --> Save each variable in one seperate file?
            #ncid = netCDF4.Dataset(reg_dir + 'SEAS5_daily_' + year_str + month_str + '_O320_' + domain + '.nc', mode = "a")

            # Write variables to existing netcdf4-File
            # Auswahl an Variablen implementieren
            #ncid.variables['tp'][:, :, :, :] = ds_reg.tp.values
            #ncid.variables['t2m'][:, :, :, :] = ds_reg.t2m.values
            #ncid.variables['t2min'][:, :, :, :] = ds_reg.t2min.values
            #ncid.variables['t2max'][:, :, :, :] = ds_reg.t2max.values
            #ncid.variables['ssrd'][:, :, :, :] = ds_reg.ssrd.values#

            #ncid.close()

            # ds_reg.to_netcdf(reg_dir + '/SEAS5_daily_' + year_str + month_str + '_O320_' + domain + '.nc')

            # Remap to regional grid and store output as sepperate file


                #    Update global attributes of high-resoluted SEAS5-File
            #sinfo = {"domain": domain,
             #        "resolution": 0.1}

            #glb_attr, var_attr = modules.set_metadata(sinfo, 15, 15)
            #modules.update_glb_attributes(seas5_high_flne, glb_attr)

  #  print()

    # Merge by time for every month, all years
    #for month in range(smonth_calib,emonth_claib+1):
    #    month_str = str(month).zfill(2)
    #    ds = xr.open_mfdataset(seas5_high_dir + 'SEAS5_daily_*' + month_str + '_0.1_' + domain + '.nc') # linechunks festlegen wenn möglich
    #    ds.to_netcdf(lnch_dir + "SEAS5_daily_" + str(syr_calib) + "_" + str(eyr_calib) + "_" + month_str + "_0.1_" + domain + "_lns.nc")


    # ERA5-Land
def some_other_crap():
    # Global directory of Domain
    # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'
    glb_dir = '/Users/borkenhagen-c/KIT_Master/BC_routine/historic_data/'

    # Directory of global ERA5-Land
    era5_raw_dir = '/Users/borkenhagen-c/temp_files/'

    # Directory of regional ERA5-Land for each domain
    reg_dir = glb_dir + domain + '/era5_land/'

    # Directory of temporal ERA5-Land files during process
    temp_dir = glb_dir + domain + '/era5_land/temp/'

    # Directory of regional grid
    grd_dir = glb_dir  + domain + '/masks/'

    if not os.path.isdir(reg_dir):
        os.makedirs(reg_dir)
    if not os.path.isdir(temp_dir):
        os.makedirs(temp_dir)

    # List of variables
    #vars = ['tp', 't2m', 't2min', 't2max', 'ssrd']


    # Loop over variables
    for variable in variable_config:
        # loop over all years
        for year in range(syr_calib, eyr_calib+1):
            year_str = str(year)
            # Open dataset
            ds = xr.open_mfdataset(f"{era5_raw_dir}/ERA5_Land_daily_{variable}_{year_str}.nc", parallel = True)
            # Rename longitude and latitude
            ds = ds.rename({'longitude': 'lon', 'latitude': 'lat'})
            # Order of lat/lon matters!!! Maybe switch order?
            # Sort latitude in ascending order
            ds = ds.sortby('lat')
            # Cut out domain
            #ds_reg = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
            # Interpolate to regional grid
            #ds_reg.to_netcdf(temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw.nc")

            # fill missing value
            # cdo.fillmiss(input = temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw.nc", output = temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw_fill.nc")



            # Map Era5-Land to same grid as SEAS5
            # Grid File
            #grd_flne = grd_dir + domain + "_grd.txt"
            era5_input  = temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw.nc"
            era5_output = reg_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + ".nc"
            cdo.remapbil(grd_flne, input=era5_input, output=era5_output, options="-f nc4 -k grid -z zip_6 -P 10")

print()