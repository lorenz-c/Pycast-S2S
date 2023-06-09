# import packages
import datetime as dt
import os
import sys
from pathlib import Path
from subprocess import PIPE, run
import logging
import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from dask import config
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
import dask
import dir_fnme

# from bc_module import bc_module
# from write_output import write_output


def set_and_make_dirs(domain_config: dict) -> dict:

    # List of Directories
    reg_dir_dict = {}
    glob_dir_dict = {}

    # Set first the root directory
    reg_dir_dict["domain_dir"] = f"{domain_config['regroot']}"

    # Then the level 1 directories
    reg_dir_dict["static_dir"] = f"{reg_dir_dict['domain_dir']}/00_static/"
    reg_dir_dict[
        "raw_forecasts_dir"
    ] = f"{reg_dir_dict['domain_dir']}/01_raw_forecasts/"
    reg_dir_dict["reference_dir"] = f"{reg_dir_dict['domain_dir']}/02_reference/"
    reg_dir_dict["processed_dir"] = f"{reg_dir_dict['domain_dir']}/03_processed/"
    reg_dir_dict["aggregated_dir"] = f"{reg_dir_dict['domain_dir']}/04_aggregated/"
    reg_dir_dict[
        "forecast_measure_dir"
    ] = f"{reg_dir_dict['domain_dir']}/05_forecast_measures/"

    # Then the level 2 directories
    reg_dir_dict[
        "raw_forecasts_initial_resolution_dir"
    ] = f"{reg_dir_dict['raw_forecasts_dir']}initial_resolution/"
    reg_dir_dict[
        "raw_forecasts_target_resolution_dir"
    ] = f"{reg_dir_dict['raw_forecasts_dir']}target_resolution/"
    reg_dir_dict[
        "raw_forecasts_zarr_dir"
    ] = f"{reg_dir_dict['raw_forecasts_dir']}zarr_stores/"
    reg_dir_dict[
        "reference_initial_resolution_dir"
    ] = f"{reg_dir_dict['reference_dir']}initial_resolution"
    reg_dir_dict[
        "bcsd_forecast_zarr_dir"
    ] = f"{reg_dir_dict['processed_dir']}zarr_stores/"
    reg_dir_dict[
        "reference_target_resolution_dir"
    ] = f"{reg_dir_dict['reference_dir']}target_resolution/"
    reg_dir_dict["reference_zarr_dir"] = f"{reg_dir_dict['reference_dir']}zarr_stores/"

    reg_dir_dict["climatology_dir"] = f"{reg_dir_dict['aggregated_dir']}/climatology/"
    reg_dir_dict["monthly_dir"] = f"{reg_dir_dict['aggregated_dir']}/monthly/"
    reg_dir_dict["statistic_dir"] = f"{reg_dir_dict['aggregated_dir']}/statistic/"


    # Then the level 3 directories
    reg_dir_dict[
        "bcsd_forecast_mon_zarr_dir"
    ] = f"{reg_dir_dict['monthly_dir']}zarr_stores_bcsd/"
    reg_dir_dict[
        "ref_forecast_mon_zarr_dir"
    ] = f"{reg_dir_dict['monthly_dir']}zarr_stores_ref/"
    reg_dir_dict[
        "seas_forecast_mon_zarr_dir"
    ] = f"{reg_dir_dict['monthly_dir']}zarr_stores_seas/"



    for key in reg_dir_dict:
        if not os.path.isdir(reg_dir_dict[key]):
            print(f"Creating directory {reg_dir_dict[key]}")
            os.makedirs(reg_dir_dict[key])

    glob_dir_dict[
        "global_forecasts"
    ] = "/pd/data/regclim_data/gridded_data/seasonal_predictions/seas5/daily"  # We need to make this more flexible...
    glob_dir_dict[
        "global_reference"
    ] = "/pd/data/regclim_data/gridded_data/reanalyses/era5_land/daily"  # We need to make this more flexible...

    return reg_dir_dict, glob_dir_dict


def update_global_attributes(global_config, bc_params, coords, domain):
    # Update the global attributes with some run-specific parameters
    now = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    global_config["comment"] = f"Domain: {domain}, BCSD-Parameter: {str(bc_params)}"
    global_config["creation_date"] = f"{now}"

    global_config["geospatial_lon_min"] = min(coords["lon"])
    global_config["geospatial_lon_max"] = max(coords["lon"])
    global_config["geospatial_lat_min"] = min(coords["lat"])
    global_config["geospatial_lat_max"] = max(coords["lat"])
    global_config["StartTime"] = pd.to_datetime(coords["time"][0]).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    global_config["StopTime"] = pd.to_datetime(coords["time"][-1]).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    return global_config


def set_input_files(
    domain_config: dict, reg_dir_dict: dict, month: int, year: int, variable: str
):

    raw_file = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    raw_full = f"{reg_dir_dict['raw_forecasts_target_resolution_dir']}{raw_file}"

    pp_file = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    pp_full = f"{reg_dir_dict['processed_dir']}{pp_file}"

    refrcst_zarr = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{month:02d}_{domain_config['target_resolution']}_calib_linechunks.zarr"
    refrcst_full = f"{reg_dir_dict['raw_forecasts_zarr_dir']}{refrcst_zarr}"

    ref_zarr = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
    ref_full = f"{reg_dir_dict['reference_zarr_dir']}{ref_zarr}"

    return raw_full, pp_full, refrcst_full, ref_full


def set_filenames(
    domain_config,
    variable_config,
    dir_dict,
    syr_calib,
    eyr_calib,
    year,
    month_str,
    forecast_linechunks,
):
    raw_dict = {}
    bcsd_dict = {}
    ref_hist_dict = {}
    mdl_hist_dict = {}

    for variable in variable_config:

        # Update Filename
        fnme_dict = dir_fnme.set_filenames(
            domain_config,
            syr_calib,
            eyr_calib,
            year,
            month_str,
            domain_config["bcsd_forecasts"]["merged_variables"],
            variable,
        )

        if forecast_linechunks:
            raw_dict[
                variable
            ] = f"{dir_dict['frcst_high_reg_lnch_dir']}/{fnme_dict['frcst_high_reg_lnch_dir']}"
        else:
            raw_dict[
                variable
            ] = f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}"

        if forecast_linechunks:
            bcsd_dict[
                variable
            ] = f"{dir_dict['frcst_high_reg_bcsd_daily_lnch_dir']}/{fnme_dict['frcst_high_reg_bcsd_daily_lnch_dir']}"
        else:
            bcsd_dict[
                variable
            ] = f"{dir_dict['frcst_high_reg_bcsd_daily_dir']}/{fnme_dict['frcst_high_reg_bcsd_daily_dir']}"

        ref_hist_dict[
            variable
        ] = f"{dir_dict['ref_high_reg_daily_lnch_calib_dir']}/{fnme_dict['ref_high_reg_daily_lnch_calib_dir']}"

        mdl_hist_dict[
            variable
        ] = f"{dir_dict['frcst_high_reg_lnch_calib_dir']}/{fnme_dict['frcst_high_reg_lnch_calib_dir']}"

    return raw_dict, bcsd_dict, ref_hist_dict, mdl_hist_dict


def set_encoding(variable_config, coordinates, type="maps"):

    encoding = {}

    if type == "maps":
        if "ens" in coordinates:
            chunksizes = [
                20,
                len(coordinates["ens"]),
                len(coordinates["lat"]),
                len(coordinates["lon"]),
            ]
        else:
            chunksizes = [20, len(coordinates["lat"]), len(coordinates["lon"])]
    elif type == "lines":
        if "ens" in coordinates:
            chunksizes = [len(coordinates["time"]), len(coordinates["ens"]), 1, 1]
        else:
            chunksizes = [len(coordinates["time"]), 1, 1]

    for variable in variable_config:
        encoding[variable] = {
            "zlib": True,
            "complevel": 1,
            "_FillValue": variable_config[variable]["_FillValue"],
            "scale_factor": variable_config[variable]["scale_factor"],
            "add_offset": variable_config[variable]["add_offset"],
            "dtype": variable_config[variable]["dtype"],
            "chunksizes": chunksizes,
        }

    encoding['lat'] = {"_FillValue": None, "dtype": "float"}
    encoding['lon'] = {"_FillValue": None, "dtype": "float"}
    encoding['time'] = {"_FillValue": None, "units": 'days since 1950-01-01 00:00:00', "dtype": "int32"}

    if "ens" in coordinates:
        encoding['ens'] = {"dtype": "int16"}

    return encoding


def set_zarr_encoding(variable_config):

    encoding = {}

    for variable in variable_config:

        encoding[variable] = {
            "compressor": zarr.Blosc(cname="zstd", clevel=3, shuffle=2),
            "_FillValue": variable_config[variable]["_FillValue"],
            "scale_factor": variable_config[variable]["scale_factor"],
            "add_offset": variable_config[variable]["add_offset"],
            "dtype": variable_config[variable]["dtype"],
        }
        
        encoding['lat'] = {"_FillValue": None, "dtype": "float"}
        encoding['lon'] = {"_FillValue": None, "dtype": "float"}
        encoding['time'] = {"_FillValue": None, "units": 'days since 1950-01-01 00:00:00', "dtype": "float"}

    return encoding


def create_4d_netcdf(
    file_out, global_config, domain_config, variable_config, coordinates, variable
):
    da_dict = {}

    encoding = set_encoding(variable_config, coordinates)

    da_dict[variable] = xr.DataArray(
        None,
        dims=["time", "ens", "lat", "lon"],
        coords={
            "time": (
                "time",
                coordinates["time"],
                {"standard_name": "time", "long_name": "time"},
            ),
            "ens": (
                "ens",
                coordinates["ens"],
                {"standard_name": "realization", "long_name": "ensemble_member"},
            ),
            "lat": (
                "lat",
                coordinates["lat"],
                {
                    "standard_name": "latitude",
                    "long_name": "latitude",
                    "units": "degrees_north",
                },
            ),
            "lon": (
                "lon",
                coordinates["lon"],
                {
                    "standard_name": "longitude",
                    "long_name": "longitude",
                    "units": "degrees_east",
                },
            ),
        },
        attrs={
            "standard_name": variable_config[variable]["standard_name"],
            "long_name": variable_config[variable]["long_name"],
            "units": variable_config[variable]["units"],
        },
    )

    ds = xr.Dataset(
        data_vars={variable: da_dict[variable]},
        coords={
            "time": (
                "time",
                coordinates["time"],
                {"standard_name": "time", "long_name": "time"},
            ),
            "ens": (
                "ens",
                coordinates["ens"],
                {"standard_name": "realization", "long_name": "ensemble_member"},
            ),
            "lat": (
                "lat",
                coordinates["lat"],
                {
                    "standard_name": "latitude",
                    "long_name": "latitude",
                    "units": "degrees_north",
                },
            ),
            "lon": (
                "lon",
                coordinates["lon"],
                {
                    "standard_name": "longitude",
                    "long_name": "longitude",
                    "units": "degrees_east",
                },
            ),
        },
        attrs=global_config,
    )

    # ds.to_netcdf(
    #    file_out,
    #    mode="w",
    #    engine="netcdf4",
    #    encoding={variable: encoding[variable]},
    # )

    return ds


def get_coords_from_frcst(filename):

    ds = xr.open_dataset(filename, engine = "netcdf4")

    # return {
    #    'time': ds['time'].values,
    #    'lat': ds['lat'].values.astype(np.float32),
    #    'lon': ds['lon'].values.astype(np.float32),
    #    'ens': ds['ens'].values
    # }

    return {
        "time": ds["time"].values,
        "lat": ds["lat"].values,
        "lon": ds["lon"].values,
        "ens": ds["ens"].values,
    }


def get_coords_from_ref(filename):
    ds = xr.open_dataset(filename)

    # return {
    #    'time': ds['time'].values,
    #    'lat': ds['lat'].values.astype(np.float32),
    #    'lon': ds['lon'].values.astype(np.float32),
    #    'ens': ds['ens'].values
    # }

    return {
        "time": ds["time"].values,
        "lat": ds["lat"].values,
        "lon": ds["lon"].values,
    }


def preprocess_mdl_hist(filename, month, variable):
    # Open data
    ds = xr.open_mfdataset(
        filename,
        chunks={"time": 215, "year": 36, "ens": 25, "lat": 10, "lon": 10},
        parallel=True,
        engine="netcdf4",
    )

    ds = ds[variable]

    # Define time range
    year_start = ds.year.values[0].astype(int)
    year_end = ds.year.values[-1].astype(int)
    nday = len(ds.time.values)

    # Create new time based on day and year
    da_date = da.empty(shape=0, dtype=np.datetime64)
    for yr in range(year_start, year_end + 1):
        date = np.asarray(
            pd.date_range(f"{yr}-{month}-01 00:00:00", freq="D", periods=nday)
        )
        da_date = da.append(da_date, date)

    # Assign Datetime-Object to new Date coordinate and rename it to "time"
    ds = ds.stack(date=("year", "time")).assign_coords(date=da_date).rename(date="time")

    return ds


def getCluster(queue, nodes, jobs_per_node):
    workers = nodes * jobs_per_node

    # cluster options
    if queue == "rome":
        cores, memory, walltime = (62, "220GB", "08:00:00")
    elif queue == "ivyshort":
        cores, memory, walltime = (40, "60GB", "48:00:00")
    elif queue == "ccgp" or queue == "cclake":
        cores, memory, walltime = (38, "170GB", "48:00:00")
    elif queue == "haswell":
        cores, memory, walltime = (40, "120GB", "48:00:00")
    elif queue == "ivy":
        cores, memory, walltime = (40, "60GB", "48:00:00")
    elif queue == "fat":
        cores, memory, walltime = (96, "800GB", "48:00:00")
    elif queue == "milan":
        cores, memory, walltime = (40, "120GB", "12:00:00")

    # cluster if only single cluster is needed!
    cluster = SLURMCluster(
        cores=cores,
        memory=memory,
        processes=jobs_per_node,
        queue=queue,
        walltime=walltime,
    )

    # config.set({"interface": "lo"})

    client = Client(cluster)

    cluster.scale(n=workers)

    return client, cluster


def run_cmd(cmd, path_extra=Path(sys.exec_prefix) / "bin"):
    # '''Run a bash command.'''
    env_extra = os.environ.copy()
    env_extra["PATH"] = str(path_extra) + ":" + env_extra["PATH"]
    status = run(cmd, check=False, stderr=PIPE, stdout=PIPE, env=env_extra)
    if status.returncode != 0:
        error = f"""{' '.join(cmd)}: {status.stderr.decode('utf-8')}"""
        raise RuntimeError(f"{error}")
    return status.stdout.decode("utf-8")


def decode_processing_years(years_string):

    year_list = [int(item) for item in years_string.split(",")]

    if len(year_list) == 1:
        # We want to run the routine for a single year
        years = [year_list[0]]
    elif len(year_list) == 2:
        years = year_list
    elif len(year_list) == 3:
        if year_list[1] == 0:
            years = range(
                year_list[0], year_list[2] + 1
            )  # We want the last year to be included in the list...
        else:
            years = year_list
    elif len(year_list) > 3:
        years = year_list

    return years


def decode_processing_months(months_string):

    month_list = [int(item) for item in months_string.split(",")]

    if len(month_list) == 1:
        # We want to run the routine for a single year
        months = [month_list[0]]
    elif len(month_list) == 2:
        months = month_list
    elif len(month_list) == 3:
        if month_list[1] == 0:
            months = range(
                month_list[0], month_list[2] + 1
            )  # We want the last month to be included in the list...
        else:
            months = month_list
    elif len(month_list) > 3:
        months = month_list

    return months

def s3_init():
    s3 = s3fs.S3FileSystem(
        key="YGJDL7QJF6FW6AM1UNEX",
        secret="EqdyNK0u2aIsxGotQjA0iNdlSlWXSVzC2K8Wvjo6",
        client_kwargs=dict(
            region_name="us-east-1",
            endpoint_url="https://s3.imk-ifu.kit.edu:8082",
            verify=False,
        ),
    )

    return s3
