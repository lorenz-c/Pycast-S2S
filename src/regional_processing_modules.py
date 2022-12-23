# In this script, the historical SEAS5- and ERA5-Land-Data are processed for each domain

# Packages
import logging
import os
from os.path import exists

# import zarrimpo
import dask
import numpy as np
import pandas as pd

# from cdo import *
# cdo = Cdo()
import xarray as xr
import zarr
from rechunker import rechunk

# import dir_fnme
from helper_modules import run_cmd, set_encoding

# Open Points
# 1. Paths are local, change them (pd/data)
# 2. Get information out of the parameter-file (which has to be changed, according to
#       Christof's draft)
# 3. Global attributes for nc-Files --> check, how the historic raw seas5-files for other
#       domains have been built and rebuilt for new domains
#  --> Change overall settings for nc-Files (global attributes, vars, etc.) within the module.py,
#       so that it can be used for all cases within the BCSD


# SEAS5 #
# Steps:
# 1. Load global SEAS5-Dataset
# 2. Cut out domain of interest
# 3. Remap to local grid
# 4. Store as high resolution dataset for the specific domain

global bbox


def create_grd_file(domain_config: dict, grid_file: str) -> str:
    """Creates a grid description file that is used for remapping the forecasts to the final resolution."""
    min_lon = domain_config["bbox"][0]
    max_lon = domain_config["bbox"][1]
    min_lat = domain_config["bbox"][2]
    max_lat = domain_config["bbox"][3]

    # Create regional mask with desired resolution
    grd_res = domain_config["target_resolution"]
    lat_range = int((max_lat - min_lat) / grd_res) + 1
    lon_range = int((max_lon - min_lon) / grd_res) + 1
    grd_size = lat_range * lon_range

    # grd_flne = f"{dir_dict['grd_dir']}/{fnme_dict['grd_dir']}"

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
        f"yinc = {str(grd_res)}\n",
    ]

    if not os.path.isfile(grid_file):
        with open(grid_file, mode="w") as f:
            f.writelines(content)
    else:
        print("File for regional grid already exists")

    return grid_file

### SEAS5 ###

def preprocess(ds):
    # ADD SOME CHECKS HERE THAT THIS STUFF IS ONLY APPLIED WHEN LATITUDES ARE REVERSED AND LONGITUDES GO FROM 0 TO 360
    if "longitude" in ds.variables:
        ds = ds.rename({"longitude": "lon"})

    if "latitude" in ds.variables:
        ds = ds.rename({"latitude": "lat"})

    ds = ds.sortby(ds.lat)
    ds.coords["lon"] = (ds.coords["lon"] + 180) % 360 - 180
    ds = ds.sortby(ds.lon)

    ds["lon"].attrs = {"standard_name": "longitude", "units": "degrees_east"}
    ds["lat"].attrs = {"standard_name": "latitude", "units": "degrees_north"}

    return ds


@dask.delayed
def truncate_forecasts(
    domain_config: dict,
    variable_config: dict,
    reg_dir_dict: dict,
    glob_dir_dict: str,
    year: int,
    month: int,
    month_range: list
):

    bbox = domain_config["bbox"]

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1

    fle_string = f"{glob_dir_dict['global_forecasts']}/{year}/{month:02d}/ECMWF_SEAS5_*_{year}{month:02d}.nc"  # TBA...

    ds = xr.open_mfdataset(
        fle_string,
        concat_dim="ens",
        combine="nested",
        parallel=True,
        chunks={"time": 50},
        preprocess=preprocess,
    )
    # Select Lat/Lon-Box
    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
    # Select only 7 Month and exlude the first day of 8th month, because that couse troubles, when some SEAS5-Forecast does not include this day
    # ds = ds.sel(time=ds.time.dt.month.isin(month_range))

    coords = {
        "time": ds["time"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
        "ens": ds["ens"].values,
    }

    ds = ds.transpose("time", "ens", "lat", "lon")

    encoding = set_encoding(variable_config, coords)

    for variable in variable_config:

        fle_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}.nc"
        full_out = f"{reg_dir_dict['raw_forecasts_initial_resolution_dir']}/{fle_out}"

        try:
            ds[variable].to_netcdf(full_out, encoding={variable: encoding[variable]})
            logging.info(
                f"Truncate forecasts: succesful for month {month:02d} and year {year}"
            )
        except:
            logging.error(
                f"Something went wrong during slicing for month {month:02d} and year {year}"
            )


@dask.delayed
def remap_forecasts(
    domain_config: dict,
    reg_dir_dict: dict,
    year: int,
    month: int,
    grd_fle: str,
    variable: str,
):
    print("test")
    fle_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}.nc"
    full_in = f"{reg_dir_dict['raw_forecasts_initial_resolution_dir']}{fle_in}"

    print(full_in)

    fle_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['raw_forecasts_target_resolution_dir']}{fle_out}"

    cmd = (
        "cdo",
        "-O",
        "-f",
        "nc4c",
        "-z",
        "zip_6",
        f"remapbil,{grd_fle}",
        str(full_in),
        str(full_out),
    )

    try:
        os.path.isfile(full_in)
        run_cmd(cmd)
    except:
        logging.error(f"Remap_forecast: file {full_in} not available")

### REF ####

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
    ds.time.attrs = {
        "standard_name": time_attr["standard_name"],
        "long_name": time_attr["long_name"],
        "axis": time_attr["axis"],
    }

    # some other preprocessing
    if "longitude" in ds.variables:
        ds = ds.rename({"longitude": "lon"})

    if "latitude" in ds.variables:
        ds = ds.rename({"latitude": "lat"})

    ds = ds.sortby(ds.lat)
    ds.coords["lon"] = (ds.coords["lon"] + 180) % 360 - 180
    ds = ds.sortby(ds.lon)

    ds["lon"].attrs = {"standard_name": "longitude", "units": "degrees_east"}
    ds["lat"].attrs = {"standard_name": "latitude", "units": "degrees_north"}

    # Drop var "time bounds" if necessary, otherwise cant be merged
    try:
        ds = ds.drop_dims("bnds")
    except:
        print("No bnds dimension available")

    return ds


@dask.delayed
def truncate_reference(
    domain_config: dict,
    variable_config: dict,
    reg_dir_dict: dict,
    glob_dir_dict: dict,
    year: int,
    variable: str,
):
    bbox = domain_config["bbox"]

    # Add one degree in each direction to avoid NaNs at the boarder after remapping.
    min_lon = bbox[0] - 1
    max_lon = bbox[1] + 1
    min_lat = bbox[2] - 1
    max_lat = bbox[3] + 1

    file_in = (
        f"{glob_dir_dict['global_reference']}/ERA5_Land_daily_{variable}_{year}.nc"
    )
    file_out = f"{domain_config['reference_history']['prefix']}_{variable}_{year}.nc"
    full_out = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_out}"

    ds = xr.open_mfdataset(
        file_in,
        parallel=True,
        chunks={"time": 50},
        engine="netcdf4",
        preprocess=preprocess_reference,
        autoclose=True,
    )

    ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))

    coords = {
        "time": ds["time"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = set_encoding(variable_config, coords)

    try:
        ds.to_netcdf(full_out, encoding={variable: encoding[variable]})
        logging.info(
            f"Truncate reference: succesful for variable {variable} and year {year}"
        )
    except:
        logging.info(
            f"Truncate reference: something went wrong for variable {variable} and year {year}"
        )
    #            f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}",
    #            encoding=encoding,

    # try:
    #    if domain_config["reference_history"]["merged_variables"]:
    #        ds.to_netcdf(
    #            f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}",
    #            encoding=encoding,
    #        )
    #    else:
    #        ds.to_netcdf(
    #            f"{dir_dict['ref_low_reg_dir']}/{fnme_dict['ref_low_reg_dir']}",
    #            encoding={variable: encoding[variable]},
    #        )

    # except:
    #    logging.error(
    #        f"Truncate reference: Something went wrong during truncation for variable {variable}!"
    #    )


@dask.delayed
def remap_reference(
    domain_config: dict,
    reg_dir_dict: dict,
    year: int,
    grd_fle: str,
    variable: str,
):

    file_in = f"{domain_config['reference_history']['prefix']}_{variable}_{year}.nc"
    full_in = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_in}"

    file_out = f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['reference_target_resolution_dir']}/{file_out}"

    cmd = (
        "cdo",
        "-O",
        "-f",
        "nc4c",
        "-z",
        "zip_6",
        f"remapbil,{grd_fle}",
        str(full_in),
        str(full_out),
    )

    try:
        os.path.isfile(full_in)
        run_cmd(cmd)
    except:
        logging.error(f"Remap_forecast: file {full_in} not available")

######### Old Crap #########

def rechunker_forecasts(
    domain_config: dict,
    variable_config: dict,
    dir_dict: dict,
    year: int,
    month: int,
    variable: str,
):

    fnme_dict = dir_fnme.set_filenames(
        domain_config,
        year,
        month,
        domain_config["raw_forecasts"]["merged_variables"],
        variable,
    )

    fle_string = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

    return fle_string


@dask.delayed
def rechunk_forecasts(
    domain_config: dict,
    variable_config: dict,
    dir_dict: dict,
    year: int,
    month: int,
    variable: str,
):

    month_str = str(month).zfill(2)

    # if domain_config['reference_history']['merged_variables'] == True:
    #    # Update Filenames
    #    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str,
    # domain_config['raw_forecasts']["merged_variables"])

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
    # else:
    #    for variable in variable_config:
    # Update Filenames
    fnme_dict = dir_fnme.set_filenames(
        domain_config,
        year,
        month,
        domain_config["raw_forecasts"]["merged_variables"],
        variable,
    )

    fle_string = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

    ds = xr.open_mfdataset(
        fle_string,
        parallel=True,
        # chunks={"time": 'auto', 'ens': 'auto', 'lat': -1, 'lon': -1},
        chunks={"time": 50},
        engine="netcdf4",
        autoclose=True,
    )

    # ds = ds.chunk({"time": -1, 'ens': -1, 'lat': 1, 'lon': 1})

    coords = {
        "time": ds["time"].values,
        "ens": ds["ens"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = set_encoding(variable_config, coords, "lines")

    # Delete the chunksizes-attribute as we want to keep the chunks from above..
    # del encoding[variable]["chunksizes"]
    # del encoding[variable]["zlib"]
    # del encoding[variable]["complevel"]

    # compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

    # encoding[variable]["compressor"] = compressor

    final_file = (
        f"{dir_dict['frcst_high_reg_lnch_dir']}/{fnme_dict['frcst_high_reg_lnch_dir']}"
    )

    try:
        ds.to_netcdf(final_file, encoding={variable: encoding[variable]})
        logging.info(
            f"rechunk_forecasts: Rechunking forecast for {year}-{month:02d} successful"
        )
    except:
        logging.info(f"rechunk_forecasts: Something went wrong for {year}-{month:02d}")

    # if exists(f"/bg/data/NCZarr/temp/SEAS5_{month:02d}.zarr"):
    #    if exists(f"/bg/data/NCZarr/temp/SEAS5_{month:02d}.zarr/{variable}"):
    #        out = ds.to_zarr(f"/bg/data/NCZarr/temp/SEAS5_{month:02d}.zarr", mode='a', append_dim="time")
    #    else:
    #        out = ds.to_zarr(f"/bg/data/NCZarr/temp/SEAS5_{month:02d}.zarr", mode='a', append_dim="time", encoding={variable: encoding[variable]})
    # else:
    #    out = ds.to_zarr(f"/bg/data/NCZarr/temp/SEAS5_{month:02d}.zarr", mode='w', encoding={variable: encoding[variable]})
    #
    # return ds, encoding

    #
    # except:
    #    logging.error(f"Something went wrong during writing of forecast linechunks")



def calib_forecasts(domain_config, variable_config, dir_dict, syr, eyr, month_str):
    file_list = []
    if domain_config["reference_history"]["merged_variables"]:
        for year in range(syr, eyr + 1):
            # Update Filenames
            fnme_dict = dir_fnme.set_filenames(
                domain_config,
                syr,
                eyr,
                year,
                month_str,
                domain_config["raw_forecasts"]["merged_variables"],
            )

            file_list.append(
                f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"
            )

        ds = xr.open_mfdataset(
            file_list,
            parallel=True,
            engine="netcdf4",
            autoclose=True,
            chunks={"time": 50},
        )

        coords = {
            "time": ds["time"].values,
            "ens": ds["ens"].values,
            "lat": ds["lat"].values.astype(np.float32),
            "lon": ds["lon"].values.astype(np.float32),
        }

        encoding = set_encoding(variable_config, coords, "lines")

        final_file = f"{dir_dict['frcst_high_reg_lnch_calib_dir']}/{fnme_dict['frcst_high_reg_lnch_calib_dir']}"

        try:
            ds.to_netcdf(final_file, encoding=encoding)
            logging.info("Calibrate forecast: successful")
        except:
            logging.error("Calibrate forecast: Something went wrong")
    else:
        for variable in variable_config:
            for year in range(syr, eyr + 1):
                # Update Filenames
                fnme_dict = dir_fnme.set_filenames(
                    domain_config,
                    syr,
                    eyr,
                    year,
                    month_str,
                    domain_config["raw_forecasts"]["merged_variables"],
                    variable,
                )

                file_list.append(
                    f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"
                )

            ds = xr.open_mfdataset(
                file_list,
                parallel=True,
                engine="netcdf4",
                autoclose=True,
                chunks={"time": 50},
            )

            coords = {
                "time": ds["time"].values,
                "ens": ds["ens"].values,
                "lat": ds["lat"].values.astype(np.float32),
                "lon": ds["lon"].values.astype(np.float32),
            }

            encoding = set_encoding(variable_config, coords, "lines")

            final_file = f"{dir_dict['frcst_high_reg_lnch_calib_dir']}/{fnme_dict['frcst_high_reg_lnch_calib_dir']}"

            try:
                ds.to_netcdf(final_file, encoding={variable: encoding[variable]})
                logging.info("Calibrate forecast: successful")
            except:
                logging.error("Calibrate forecast: Something went wrong")









@dask.delayed
def rechunk_reference(
    domain_config: dict,
    variable_config: dict,
    dir_dict: dict,
    year: int,
    month: int,
    variable: str,
):

    month_str = str(month).zfill(2)

    # if domain_config['reference_history']['merged_variables'] == True:
    # Update Filenames:
    #    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str,
    #                                       domain_config['reference_history']['merged_variables'])

    #    fle_string = f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"

    # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    #    ds = xr.open_mfdataset(fle_string, parallel=True, chunks={'time': 50}, engine='netcdf4', preprocess=preprocess,

    #    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32), 'lon':
    # ds['lon'].values.astype(np.float32)}

    #    encoding = set_encoding(variable_config, coords, 'lines')

    ##   try:
    #        ds.to_netcdf(f"{dir_dict['ref_high_reg_daily_lnch_dir']}/
    # {fnme_dict['ref_high_reg_daily_lnch_dir']}", encoding=encoding)
    #    except:
    #        logging.error(f"Rechunk reference: Rechunking of reference data failed!")
    # else:
    #    for variable in variable_config:
    # Update Filenames:
    fnme_dict = dir_fnme.set_filenames(
        domain_config,
        year,
        month_str,
        domain_config["reference_history"]["merged_variables"],
        variable,
    )

    fle_string = (
        f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}"
    )

    # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    ds = xr.open_mfdataset(
        fle_string,
        parallel=True,
        chunks={"time": 50},
        engine="netcdf4",
        preprocess=preprocess,
        autoclose=True,
    )

    coords = {
        "time": ds["time"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = set_encoding(variable_config, coords, "lines")

    try:
        ds.to_netcdf(
            f"{dir_dict['ref_high_reg_daily_lnch_dir']}/{fnme_dict['ref_high_reg_daily_lnch_dir']}",
            encoding={variable: encoding[variable]},
        )
    except:
        logging.error(
            f"Rechunk reference: Rechunking of reference data failed for variable {variable}!"
        )


@dask.delayed
def calib_reference(
    domain_config: dict,
    variable_config: dict,
    dir_dict: dict,
    syr: int,
    eyr: int,
    variable: str,
):

    fle_list = []

    # if domain_config['reference_history']['merged_variables'] == True:
    #    for year in range(syr, eyr + 1):
    #        # Update filenames
    #        month_str = "01"  # dummy
    #        fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config
    # ['reference_history']['merged_variables'])#

    #        fle_list.append(f"{dir_dict['ref_high_reg_daily_dir']}/{fnme_dict['ref_high_reg_daily_dir']}")

    #    # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})
    #    ds = xr.open_mfdataset(fle_list, parallel=True, chunks={'time': 50}, engine='netcdf4', autoclose=True)

    #    coords = {'time': ds['time'].values, 'lat': ds['lat'].values.astype(np.float32),
    #              'lon': ds['lon'].values.astype(np.float32)}

    #    encoding = set_encoding(variable_config, coords, 'lines')

    #    try:
    #        ds.to_netcdf(
    #            f"{dir_dict['ref_high_reg_daily_lnch_calib_dir']}/{fnme_dic
    # t['ref_high_reg_daily_lnch_calib_dir']}",
    #            encoding=encoding)
    #    except:
    #        logging.error(f"Rechunk reference: Rechunking of reference data failed for variable!")

    # else:

    #    for variable in variable_config:

    for year in range(syr, eyr + 1):
        # Update filenames
        month_str = "01"  # dummy
        fnme_dict = dir_fnme.set_filenames(
            domain_config,
            year,
            month_str,
            domain_config["reference_history"]["merged_variables"],
            variable,
        )

        fle_list.append(
            f"{dir_dict['ref_high_reg_daily_lnch_dir']}/{fnme_dict['ref_high_reg_daily_lnch_dir']}"
        )

        # ds = xr.open_mfdataset(input_file, parallel = True, chunks = {'time': 100})

    ds = xr.open_mfdataset(fle_list, parallel=True, engine="netcdf4", autoclose=True)

    coords = {
        "time": ds["time"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = set_encoding(variable_config, coords, "lines")

    try:
        ds.to_netcdf(
            f"{dir_dict['ref_high_reg_daily_lnch_calib_dir']}/{fnme_dict['ref_high_reg_daily_lnch_calib_dir']}",
            encoding={variable: encoding[variable]},
        )
    except:
        logging.error(
            f"Rechunk reference: Rechunking of reference data failed for variable {variable}!"
        )

