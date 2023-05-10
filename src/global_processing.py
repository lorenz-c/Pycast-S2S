import json
import multiprocessing as mp
import os

import numpy as np
import pandas as pd
import xarray as xr


def reformatarray(inpt):

    fulloutdir = inpt[0]
    ens = inpt[1]
    year = inpt[2]
    month = inpt[3]
    glbl_attrs = inpt[4]
    tp_hndle = inpt[5]
    t2m_hndle = inpt[6]
    t2min_hndle = inpt[7]
    t2max_hndle = inpt[8]
    ssrd_hndle = inpt[9]

    issue_date = format(year, "4") + "-" + format(month, "02") + "-" + "01"

    flenme_out = (
        fulloutdir
        + "ECMWF_SEAS5_"
        + format(ens, "02")
        + "_"
        + format(year, "04")
        + format(month, "02")
        + ".nc"
    )

    ids = np.arange(ens, 10965, 51)

    ds_tp = tp_hndle.isel(time=ids).load()
    ds_t2m = t2m_hndle.isel(time=ids)
    ds_t2min = t2min_hndle.isel(time=ids)
    ds_t2max = t2max_hndle.isel(time=ids)
    ds_ssrd = ssrd_hndle.isel(time=ids).load()

    # Transform prec from m/day in mm/day
    ds_tp = ds_tp * 1000

    # Transform radiation from XY to W/m^2
    ds_ssrd = ds_ssrd / (24 * 60 * 60)

    # Write global attributes to the prec dataarray
    ds_tp.attrs = glbl_attrs

    ds_tp["time"] = pd.date_range(issue_date, periods=215)
    ds_tp.time.encoding["units"] = "days since 1950-01-01 00:00:00"

    ds_t2m["time"] = pd.date_range(issue_date, periods=215)
    ds_t2m.time.encoding["units"] = "days since 1950-01-01 00:00:00"

    ds_t2min["time"] = pd.date_range(issue_date, periods=215)
    ds_t2min.time.encoding["units"] = "days since 1950-01-01 00:00:00"

    ds_t2max["time"] = pd.date_range(issue_date, periods=215)
    ds_t2max.time.encoding["units"] = "days since 1950-01-01 00:00:00"

    ds_ssrd["time"] = pd.date_range(issue_date, periods=215)
    ds_ssrd.time.encoding["units"] = "days since 1950-01-01 00:00:00"

    da_tp_diff = ds_tp.tp.diff(dim="time")
    da_tp_full = xr.concat([ds_tp["tp"][0, :, :], da_tp_diff], dim="time")

    ds_tp["tp"] = da_tp_full.transpose("time", "lat", "lon")

    da_ssrd_diff = ds_ssrd.ssrd.diff(dim="time")
    da_ssrd_full = xr.concat([ds_ssrd["ssrd"][0, :, :], da_ssrd_diff], dim="time")

    ds_ssrd["ssrd"] = da_ssrd_full.transpose("time", "lat", "lon")

    ds_tp.time.attrs["standard_name"] = "time"

    ds_t2m["t2m"] = ds_t2m["mean2t24"]
    ds_t2min["t2min"] = ds_t2min["mn2t24"]
    ds_t2max["t2max"] = ds_t2max["mx2t24"]

    ds_tp.tp.attrs = {"long_name": "total_precipitation", "units": "mm/day"}
    ds_t2m.t2m.attrs = {
        "long_name": "2m_temperature",
        "units": "K",
        "standard_name": "air_temperature",
    }
    ds_t2min.t2min.attrs = {
        "long_name": "minimum_daily_temperature_at_2m",
        "units": "K",
        "standard_name": "air_temperature",
    }
    ds_t2max.t2max.attrs = {
        "long_name": "maximum_daily_temperature_at_2m",
        "units": "K",
        "standard_name": "air_temperature",
    }
    ds_ssrd.ssrd.attrs = {"long_name": "surface_solar_radiation", "units": "W m-2"}

    ds_tp.to_netcdf(
        path=flenme_out,
        mode="w",
        format="NETCDF4_CLASSIC",
        encoding={
            "tp": {
                "dtype": "int32",
                "scale_factor": 0.001,
                "zlib": True,
                "complevel": 6,
                "_FillValue": -9999,
                "chunksizes": [1, 640, 1280],
            }
        },
    )
    ds_t2m.t2m.to_netcdf(
        path=flenme_out,
        mode="a",
        format="NETCDF4_CLASSIC",
        encoding={
            "t2m": {
                "dtype": "int32",
                "scale_factor": 0.0001,
                "add_offset": 273.15,
                "zlib": True,
                "complevel": 6,
                "_FillValue": -9999,
                "chunksizes": [1, 640, 1280],
            }
        },
    )
    ds_t2min.t2min.to_netcdf(
        path=flenme_out,
        mode="a",
        format="NETCDF4_CLASSIC",
        encoding={
            "t2min": {
                "dtype": "int32",
                "scale_factor": 0.0001,
                "add_offset": 273.15,
                "zlib": True,
                "complevel": 6,
                "_FillValue": -9999,
                "chunksizes": [1, 640, 1280],
            }
        },
    )
    ds_t2max.t2max.to_netcdf(
        path=flenme_out,
        mode="a",
        format="NETCDF4_CLASSIC",
        encoding={
            "t2max": {
                "dtype": "int32",
                "scale_factor": 0.0001,
                "add_offset": 273.15,
                "zlib": True,
                "complevel": 6,
                "_FillValue": -9999,
                "chunksizes": [1, 640, 1280],
            }
        },
    )
    ds_ssrd.ssrd.to_netcdf(
        path=flenme_out,
        mode="a",
        format="NETCDF4_CLASSIC",
        encoding={
            "ssrd": {
                "dtype": "int32",
                "scale_factor": 0.001,
                "zlib": True,
                "complevel": 6,
                "_FillValue": -9999,
                "chunksizes": [1, 640, 1280],
            }
        },
    )


# Import the parameter file
with open("src/bcsd_parameter.json") as json_file:
    parameter = json.load(json_file)

# Get the current month
month = parameter["issue_date"]["month"]
# ...and year
year = parameter["issue_date"]["year"]

# Get the directory of the global data
glbldir = parameter["directories"]["glbldir"]

# Get the directory of the global "unprocessed" data
glbraw = glbldir + "raw" + "/" + format(year, "4") + "/" + format(month, "02") + "/"
glblout = glbldir + "daily" + "/" + format(year, "4") + "/" + format(month, "02") + "/"

# Create the output directory (if it does not already exist...)
if not os.path.exists(glblout):
    os.makedirs(glblout)

# Set the variables for processing
vars = ["tp", "t2m", "t2min", "t2max", "ssrd"]

# Set the global attributes for the output NetCDFs
glbl_attrs = {
    "title": "ECMWF SEAS5-forecasts",
    "Conventions": "CF-1.8",
    "institution": "European Centre for Medium Range Weather Forecast (ECMWF)",
    "source": "ECMWF SEAS5",
    "comment": "Grib-data has been converted to NetCDF; original data on gaussian grid has been transformed to regular grid; unit of precipitation has been changed from m/day to mm/day; unit of radiation has been changed to W/m^2",
    "Contact_person": "Christof Lorenz (Christof.Lorenz@kit.edu)",
    "Author": "Christof Lorenz (Christof Lorenz@kit.edu)",
    "Licence": "For non-commercial only",
}


# Set the handles for the different variables
tp_flenme = (
    glbraw
    + "ECMWF-seas5_03deg_daily_"
    + "tp"
    + "_"
    + format(year, "4")
    + format(month, "02")
    + ".nc"
)
tp_hndle = xr.open_dataset(tp_flenme, lock=False)

t2m_flenme = (
    glbraw
    + "ECMWF-seas5_03deg_daily_"
    + "t2m"
    + "_"
    + format(year, "4")
    + format(month, "02")
    + ".nc"
)
t2m_hndle = xr.open_dataset(t2m_flenme, lock=False)

t2min_flenme = (
    glbraw
    + "ECMWF-seas5_03deg_daily_"
    + "t2min"
    + "_"
    + format(year, "4")
    + format(month, "02")
    + ".nc"
)
t2min_hndle = xr.open_dataset(t2min_flenme, lock=False)

t2max_flenme = (
    glbraw
    + "ECMWF-seas5_03deg_daily_"
    + "t2max"
    + "_"
    + format(year, "4")
    + format(month, "02")
    + ".nc"
)
t2max_hndle = xr.open_dataset(t2max_flenme, lock=False)

ssrd_flenme = (
    glbraw
    + "ECMWF-seas5_03deg_daily_"
    + "ssrd"
    + "_"
    + format(year, "4")
    + format(month, "02")
    + ".nc"
)
ssrd_hndle = xr.open_dataset(ssrd_flenme, lock=False)

print(tp_flenme)
if "var228" in tp_hndle.data_vars:
    tp_hndle = tp_hndle.rename({"var228": "tp"})

if "var55" in t2m_hndle.data_vars:
    t2m_hndle = t2m_hndle.rename({"var55": "mean2t24"})

if "var52" in t2min_hndle.data_vars:
    t2min_hndle = t2min_hndle.rename({"var52": "mn2t24"})

if "var51" in t2max_hndle.data_vars:
    t2max_hndle = t2max_hndle.rename({"var51": "mx2t24"})

if "var169" in ssrd_hndle.data_vars:
    ssrd_hndle = ssrd_hndle.rename({"var169": "ssrd"})

# Create a multiprocessing-pool for speeding up the transformation
pool = mp.Pool(processes=1)

# Create an empty list which holds the function calls
inpt_lst = list()

# Run a loop over the ensemble members
for ens in range(0, 51):
    # In each iteration, append a new function call to the list
    inpt_lst.append(
        [
            glblout,
            ens,
            year,
            month,
            glbl_attrs,
            tp_hndle,
            t2m_hndle,
            t2min_hndle,
            t2max_hndle,
            ssrd_hndle,
        ]
    )

# Finally, call pool.map
pool.map(reformatarray, inpt_lst)
