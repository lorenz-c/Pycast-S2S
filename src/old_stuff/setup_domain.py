# In this script, the historical SEAS5- and ERA5-Land-Data are processed for each domain

# Packages
import os

from cdo import *

cdo = Cdo()
import modules
import netCDF4
import numpy as np
import xarray as xr

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


# Domain Name
domain = "Germany"

# Set domain limits
bbox = [5.25, 15.15, 45.55, 55.45]

min_lon = 5.25
max_lon = 15.15
min_lat = 45.55
max_lat = 55.45

# Set calibration time (year, month)
syr_calib = 1981
eyr_calib = 1982
smonth_calib = 1
emonth_claib = 2

# Set number of ensembles
number_ens = 2

# Set directories
# --> HARDCODED!!!!! Change directories to pd/data!!!

# which dataset
data_set = "era5_land"

if data_set == "seas5":
    # Global directory of Domain
    # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'
    glb_dir = "/Users/borkenhagen-c/KIT_Master/BC_routine/historic_data/"

    # Directory of raw SEAS5-Data
    seas5_raw_dir = "/Users/borkenhagen-c/KIT_Master/BC_routine/seas5_raw/"

    # Directory of raw SEAS5-Data for each Domain
    reg_dir = glb_dir + domain + "/seas5/"

    # Directory of regional grid-File
    grd_dir = glb_dir + domain + "/masks/"

    # Directory of raw, high-resoluted SEAS5-Data
    seas5_high_dir = glb_dir + domain + "/seas5_h/"

    # Directory for raw, high-resoluted SEAS5-Data for the whole time period
    lnch_dir = glb_dir + domain + "/linechunks/"

    # Check if Domain-Directory exist, otherwise create important directories
    # --> Problem: Cannot write into pd/data/...

    if not os.path.isdir(glb_dir + domain):
        os.makedirs(glb_dir + domain)
    if not os.path.isdir(reg_dir):
        os.makedirs(reg_dir)
    if not os.path.isdir(seas5_high_dir):
        os.makedirs(seas5_high_dir)
    if not os.path.isdir(grd_dir):
        os.makedirs(grd_dir)
    if not os.path.isdir(lnch_dir):
        os.makedirs(lnch_dir)

    # Create regional mask with desired resolution
    grd_res = 0.1
    lat_range = int((max_lat - min_lat) / grd_res) + 1
    lon_range = int((max_lon - min_lon) / grd_res) + 1
    grd_size = lat_range * lon_range

    # Create text-file for regional grid
    # filename of regional grid
    grd_flne = grd_dir + domain + "_grd.txt"

    # if file does not exist, create regional text file for domain with desired resolution
    # --> Can be implemented and outsourced as function !!!!!!
    if not os.path.exists(grd_flne):
        with open(grd_flne, mode="w") as f:
            f.write(
                "# regional grid \n"
                "# domain: " + domain + "\n"
                "# grid resolution: " + str(grd_res) + "\n"
                "gridtype = lonlat \n"
                "gridsize = " + str(grd_size) + "\n"
                "xsize = " + str(lon_range) + "\n"
                "ysize = " + str(lat_range) + "\n"
                "xname = lon \n"
                'xlongname = "Longitude" \n'
                'xunits = "degrees_east" \n'
                "yname = lat \n"
                'ylongname = "Latitude" \n'
                'yunits = "degrees_north" \n'
                "xfirst = " + str(min_lon) + "\n"
                "xinc = " + str(grd_res) + "\n"
                "yfirst = " + str(min_lat) + "\n"
                "yinc = " + str(grd_res) + "\n"
            )
    else:
        print("File for regional grid already exists")

    # loop over all years
    for year in range(syr_calib, eyr_calib + 1):
        year_str = str(year)
        # loop over all months
        for month in range(smonth_calib, emonth_claib + 1):
            month_str = str(month).zfill(2)
            # Open raw SEAS5-data and merge all ensemble member in one file
            ds = xr.open_mfdataset(
                seas5_raw_dir + year_str + "/" + month_str + "/*.nc",
                concat_dim="ens",
                combine="nested",
                parallel=True,
            )
            # Create range for ensemble coordinate
            # ens_nr = np.arange(0,number_ens)
            # Assign ensemble coordinate
            # ds = ds.assign_coords(ens = ens_nr)
            # Drop variable ens
            # ds = ds.drop_vars("ens")
            # Reorder coordinates

            #### Doe not slice, do the direct interpolation onto regional grid

            ds = ds.transpose("time", "ens", "lat", "lon")
            # Order of lat/lon matters!!! Maybe switch order?
            # Sort latitude in ascending order
            ds = ds.sortby("lat")
            # Cut out domain
            ds_reg = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))

            # set coordinates
            coordinates = {
                "time": ds_reg.time.values,
                "ens": ds_reg.ens.values,
                "lat": ds_reg.lat.values,
                "lon": ds_reg.lon.values,
            }

            # Setup meta-data
            sinfo = {"domain": domain, "resolution": 0.3}

            glb_attr, var_attr = modules.set_metadata(sinfo, 15, 15)

            # create empty netcdf-File
            modules.create_4d_netcdf(
                reg_dir
                + "SEAS5_daily_"
                + year_str
                + month_str
                + "_O320_"
                + domain
                + ".nc",
                glb_attr,
                var_attr,
                coordinates,
            )

            # Save regional xarray to existing netcdf4-File
            # --> Save each variable in one seperate file?
            ncid = netCDF4.Dataset(
                reg_dir
                + "SEAS5_daily_"
                + year_str
                + month_str
                + "_O320_"
                + domain
                + ".nc",
                mode="a",
            )

            # Write variables to existing netcdf4-File
            # Auswahl an Variablen implementieren
            ncid.variables["tp"][:, :, :, :] = ds_reg.tp.values
            ncid.variables["t2m"][:, :, :, :] = ds_reg.t2m.values
            ncid.variables["t2min"][:, :, :, :] = ds_reg.t2min.values
            ncid.variables["t2max"][:, :, :, :] = ds_reg.t2max.values
            ncid.variables["ssrd"][:, :, :, :] = ds_reg.ssrd.values

            ncid.close()

            # ds_reg.to_netcdf(reg_dir + '/SEAS5_daily_' + year_str + month_str + '_O320_' + domain + '.nc')

            # Remap to regional grid and store output as sepperate file
            seas5_raw_flne = (
                reg_dir
                + "SEAS5_daily_"
                + year_str
                + month_str
                + "_O320_"
                + domain
                + ".nc"
            )
            seas5_high_flne = (
                seas5_high_dir
                + "SEAS5_daily_"
                + year_str
                + month_str
                + "_0.1_"
                + domain
                + ".nc"
            )
            cdo.remapbil(
                grd_flne,
                input=seas5_raw_flne,
                output=seas5_high_flne,
                options="-f nc4 -k grid -z zip_6 -P 10",
            )

            # Update global attributes of high-resoluted SEAS5-File
            sinfo = {"domain": domain, "resolution": 0.1}

            glb_attr, var_attr = modules.set_metadata(sinfo, 15, 15)
            modules.update_glb_attributes(seas5_high_flne, glb_attr)

    print()

    # Merge by time for every month, all years
    for month in range(smonth_calib, emonth_claib + 1):
        month_str = str(month).zfill(2)
        ds = xr.open_mfdataset(
            seas5_high_dir + "SEAS5_daily_*" + month_str + "_0.1_" + domain + ".nc"
        )  # linechunks festlegen wenn möglich
        ds.to_netcdf(
            lnch_dir
            + "SEAS5_daily_"
            + str(syr_calib)
            + "_"
            + str(eyr_calib)
            + "_"
            + month_str
            + "_0.1_"
            + domain
            + "_lns.nc"
        )

elif data_set == "era5_land":
    # ERA5-Land

    # Global directory of Domain
    # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'
    glb_dir = "/Users/borkenhagen-c/KIT_Master/BC_routine/historic_data/"

    # Directory of global ERA5-Land
    era5_raw_dir = "/Users/borkenhagen-c/temp_files/"

    # Directory of regional ERA5-Land for each domain
    reg_dir = glb_dir + domain + "/era5_land/"

    # Directory of temporal ERA5-Land files during process
    temp_dir = glb_dir + domain + "/era5_land/temp/"

    # Directory of regional grid
    grd_dir = glb_dir + domain + "/masks/"

    if not os.path.isdir(reg_dir):
        os.makedirs(reg_dir)
    if not os.path.isdir(temp_dir):
        os.makedirs(temp_dir)

    # List of variables
    vars = ["tp", "t2m", "t2min", "t2max", "ssrd"]

    # Loop over variables
    for var in vars:
        # loop over all years
        for year in range(syr_calib, eyr_calib + 1):
            year_str = str(year)
            # Open dataset
            ds = xr.open_mfdataset(
                era5_raw_dir + "ERA5_Land_daily_" + var + "_" + year_str + ".nc"
            )
            # Rename longitude and latitude
            ds = ds.rename({"longitude": "lon", "latitude": "lat"})
            # Order of lat/lon matters!!! Maybe switch order?
            # Sort latitude in ascending order
            ds = ds.sortby("lat")
            # Cut out domain
            ds_reg = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
            # Interpolate to regional grid
            ds_reg.to_netcdf(
                temp_dir
                + "ERA5_Land_daily_"
                + var
                + "_"
                + year_str
                + "_"
                + domain
                + "_raw.nc"
            )

            # fill missing value
            # cdo.fillmiss(input = temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw.nc", output = temp_dir + "ERA5_Land_daily_" + var + "_" + year_str + "_" + domain + "_raw_fill.nc")

            # Map Era5-Land to same grid as SEAS5
            # Grid File
            grd_flne = grd_dir + domain + "_grd.txt"
            era5_input = (
                temp_dir
                + "ERA5_Land_daily_"
                + var
                + "_"
                + year_str
                + "_"
                + domain
                + "_raw.nc"
            )
            era5_output = (
                reg_dir
                + "ERA5_Land_daily_"
                + var
                + "_"
                + year_str
                + "_"
                + domain
                + ".nc"
            )
            cdo.remapbil(
                grd_flne,
                input=era5_input,
                output=era5_output,
                options="-f nc4 -k grid -z zip_6 -P 10",
            )

print()
