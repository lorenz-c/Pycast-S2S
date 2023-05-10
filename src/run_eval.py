# import packages
import argparse
import json
import logging
from unittest.util import strclass

import dask
import modules
import numpy as np
import xarray as xr
from cdo import *
from dask.distributed import Client

import regional_processing_modules

cdo = Cdo()


def get_clas():
    # insert period, for which the bcsd-should be running! similar to process_regional_forecast
    parser = argparse.ArgumentParser(
        description="Python-based BCSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d", "--domain", action="store", type=str, help="Domain", required=True
    )
    # parser.add_argument("-y", "--year", action="store", type=int, help="Year of the actual forecast", required=True)
    # parser.add_argument("-m", "--month", action="store", type=int, help="Month of the actual forecast", required=True)
    parser.add_argument(
        "-p",
        "--period",
        action="store",
        type=str,
        help="Period for which the BCSD-evaluation should be executed",
        required=False,
    )
    parser.add_argument(
        "-s",
        "--forecast_structure",
        action="store",
        type=str,
        help="Structure of the line-chunked forecasts (can be 5D or 4D)",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--scheduler_file",
        action="store",
        type=str,
        help="If a scheduler-file is provided, the function does not start its own cluster but rather uses a running environment",
        required=False,
    )
    parser.add_argument(
        "-n",
        "--node",
        action="store",
        type=str,
        help="Node for running the code",
        required=False,
    )
    parser.add_argument(
        "-p",
        "--processes",
        action="store",
        type=int,
        help="Node for running the code",
        required=False,
    )

    return parser.parse_args()


def setup_logger(domain_name):
    logging.basicConfig(
        filename=f"logs/{domain_name}_run_eval.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )


if __name__ == "__main__":

    args = get_clas()

    if args.scheduler_file is not None:
        client = Client(scheduler_file=args.scheduler_file)
    elif args.node is not None:
        if args.processes is not None:
            client, cluster = modules.getCluster(args.node, 1, args.processes)
        else:
            logging.error(
                "Run BCSD-evaluation: If node is provided, you must also set number of processes"
            )
    else:
        logging.error(
            "Run BCSD-evaluation: Must either provide a scheduler file or node and number of processes."
        )

    # Make sure that all workers have consistent library versions
    client.get_versions(check=True)

    # Do the memory magic...
    client.amm.start()

    # Write some info about the cluster
    print(f"Dask Dashboard available at {client.dashboard_link}")

    # Read the domain configuration from the respective JSON
    with open("conf/domain_config.json", "r") as j:
        domain_config = json.loads(j.read())

    # Read the global configuration from the respective JSON --> Add this as further input parameter
    with open("conf/attribute_config.json", "r") as j:
        attribute_config = json.loads(j.read())

    # Read the variable configuration from the respective JSON
    with open("conf/variable_config.json", "r") as j:
        variable_config = json.loads(j.read())

    # Read the variable_evaluate configuration from the respective JSON
    with open("conf/variable_eval_config.json", "r") as j:
        variable_eval_config = json.loads(j.read())

    # Select the configuration for the actual domain --> We want to do that with the argument parser..
    domain_config = domain_config[args.domain]

    # Get only the variables that are needed for the current domain
    variable_config = {
        key: value
        for key, value in variable_config.items()
        if key in domain_config["variables"]
    }

    if args.period is not None:
        # Period can be in the format "year, year" or "year, month"
        period_list = [int(item) for item in args.period.split(",")]

        if period_list[0] > 1000 and (period_list[1] >= 1 and period_list[1] <= 12):
            # [year, month]
            syr = period_list[0]
            eyr = period_list[0]
            smnth = period_list[1]
            emnth = period_list[1]
        elif period_list[0] > 1000 and period_list[1] > 1000:
            syr = period_list[0]
            eyr = period_list[1]
            smnth = 1
            emnth = 12
        else:
            logging.error("Period not defined properly")
    else:
        syr = domain_config["syr_calib"]
        eyr = domain_config["eyr_calib"]
        smnth = 1
        emnth = 13

    # get important directories
    dir_dict = regional_processing_modules.set_and_make_dirs(domain_config)

    for year in range(syr, eyr + 1):
        if year < 2017:
            ens = 25
        else:
            ens = 51

        for month in range(smnth, emnth + 1):
            month_str = str(month).zfill(2)
            # Set filename of historical quantile-Files (extreme, quintile, tercile)
            tercile_ref_fle = f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_tercile_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
            quintile_ref_fle = f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_quintiles_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"
            extreme_ref_fle = f"{dir_dict['monthly_quantile']}/{domain_config['bcsd_forecasts']['prefix']}_{domain_config['version']}_monthly_extreme_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc"

            for variable in variable_config:
                # Get directory of selected forecast
                (
                    raw_dict,
                    bcsd_dict,
                    ref_hist_dict,
                    mdl_hist_dict,
                    bcsd_dict_monthly,
                ) = modules.set_filenames(
                    year, month, domain_config, variable_config, False
                )
                # Set the filename of forecast
                fle_in = bcsd_dict_monthly[variable]

                # Set filename climatology
                climatology = f"{dir_dict['ref_clim']}/{domain_config['reference_history']['prefix']}_climatology_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['prefix']}.nc"

                # Actual forecast
                dta_in = xr.open_mfdataset(fle_in, parallel=True, engine="netcdf4")
                dta_in = dta_in[variable]
                # Change the time-coord from number of months to the same timestep of the actual forecast
                dta_in = dta_in.sel(time=dta_in.time.dt.month < 8)

                # Climatology
                clim = xr.open_mfdataset(climatology, parallel=True, engine="netcdf4")
                clim = clim[variable]
                # Select only the overlaping months in climatology
                clim = clim.sel(time=clim.time.dt.month.isin(dta_in.time.dt.month))

                # ENS mean
                ens_mean = dta_in.mean(dim="ensemble")
                # ENS median
                ens_median = dta_in.median(dim="ensemble")
                # ENS sdt
                ens_std = dta_in.std(dim="ensemble")
                # ENs interquantile --> difference between 0.75 and 0.25 quantile
                ens_iqr = dta_in.quantile(
                    0.75, dim="ensemble", method="midpoint"
                ) - dta_in.quantile(0.25, dim="ensemble", method="midpoint")
                # ENS spread
                ens_spread = dta_in.max(dim="ensemble") - dta_in.min(dim="ensemble")
                # Difference to climatology
                # We need to match the timestamp before we can calculate the difference (just replace the timestamp of "clim" with the timestep of "ens_mean"
                clim = clim.assign_coords(time=ens_mean.time.values)
                delta_clim = ens_mean - clim

                # mean relative anomaly
                ens_mean_rel = (ens_mean / clim - 1) * 100

                # median relative anomaly
                # We need to match the timestamp before we can calculate the difference (just replace the timestamp of "clim" with the timestep of "ens_mean"
                clim = clim.assign_coords(time=ens_median.time.values)
                ens_med_rel = (ens_median / clim - 1) * 100

                # Load ref files for quantiles
                # extremes
                extreme_ref = xr.open_mfdataset(
                    extreme_ref_fle, parallel=True, engine="netcdf4"
                )
                # extreme_ref = extreme_ref.assign_coords({"ensemble": dta_in.ensemble.values})
                # Create new Dimension ensemble, in order to calculate the categories
                extreme_ref = extreme_ref.expand_dims(
                    dim={"ensemble": dta_in.ensemble.values}
                )
                extreme_ref = extreme_ref[variable]

                # qunitiles
                quintile_ref = xr.open_mfdataset(
                    quintile_ref_fle, parallel=True, engine="netcdf4"
                )
                # Create new Dimension ensemble, in order to calculate the categories
                quintile_ref = quintile_ref.expand_dims(
                    dim={"ensemble": dta_in.ensemble.values}
                )
                quintile_ref = quintile_ref[variable]

                # tercile
                # qunitiles
                tercile_ref = xr.open_mfdataset(
                    tercile_ref_fle, parallel=True, engine="netcdf4"
                )
                # Create new Dimension ensemble, in order to calculate the categories
                tercile_ref = tercile_ref.expand_dims(
                    dim={"ensemble": dta_in.ensemble.values}
                )
                tercile_ref = tercile_ref[variable]

                # extremes
                extreme_ref = extreme_ref.rename({"month": "time"}).assign_coords(
                    time=dta_in.time.values
                )

                # quintiles
                quintile_ref = quintile_ref.rename({"month": "time"}).assign_coords(
                    time=dta_in.time.values
                )

                # tercile
                tercile_ref = tercile_ref.rename({"month": "time"}).assign_coords(
                    time=dta_in.time.values
                )

                # Choose the right data range for quintiles, terciles and extremes based on the thresholds from the reference data (quantiles)
                # extremes
                extreme_name = [0.1, 0.9]
                extreme_name = [1, 2]
                extreme_0 = (
                    xr.where(dta_in < extreme_ref[:, :, 0, :, :], 1, 0)
                    .assign_coords({"quantiles": extreme_name[0]})
                    .expand_dims("quantiles")
                )  # expand dims "quantiles" for later merging
                extreme_1 = (
                    xr.where(dta_in > extreme_ref[:, :, 1, :, :], 1, 0)
                    .assign_coords({"quantiles": extreme_name[1]})
                    .expand_dims("quantiles")
                )
                # Write in one xarray
                extreme = xr.merge([extreme_0, extreme_1])
                extreme = extreme[variable]

                # quintile
                quintile_name = [0.1, 0.3, 0.5, 0.7, 0.9]
                quintile_name = [1, 2, 3, 4, 5]
                quintile_0 = (
                    xr.where(dta_in < quintile_ref[:, :, 0, :, :], 1, 0)
                    .assign_coords({"quantiles": quintile_name[0]})
                    .expand_dims("quantiles")
                )  # assign and expand dimension for quintile (in order to desribe the range 0-0.2, we take the mean)
                quintile_1 = (
                    xr.where(
                        (dta_in >= quintile_ref[:, :, 0, :, :])
                        & (dta_in < quintile_ref[:, :, 1, :, :]),
                        1,
                        0,
                    )
                    .assign_coords({"quantiles": quintile_name[1]})
                    .expand_dims("quantiles")
                )
                quintile_2 = (
                    xr.where(
                        (dta_in >= quintile_ref[:, :, 1, :, :])
                        & (dta_in < quintile_ref[:, :, 2, :, :]),
                        1,
                        0,
                    )
                    .assign_coords({"quantiles": quintile_name[2]})
                    .expand_dims("quantiles")
                )
                quintile_3 = (
                    xr.where(
                        (dta_in >= quintile_ref[:, :, 2, :, :])
                        & (dta_in < quintile_ref[:, :, 3, :, :]),
                        1,
                        0,
                    )
                    .assign_coords({"quantiles": quintile_name[3]})
                    .expand_dims("quantiles")
                )
                quintile_4 = (
                    xr.where(dta_in >= quintile_ref[:, :, 3, :, :], 1, 0)
                    .assign_coords({"quantiles": quintile_name[4]})
                    .expand_dims("quantiles")
                )

                # Write in one xarray
                quintile = xr.merge(
                    [quintile_0, quintile_1, quintile_2, quintile_3, quintile_4]
                )
                quintile = quintile[variable]

                # tercile
                tercile_name = [0.15, 0.45, 0.75]
                tercile_name = [1, 2, 3]
                tercile_0 = (
                    xr.where(dta_in < tercile_ref[:, :, 0, :, :], 1, 0)
                    .assign_coords({"quantiles": tercile_name[0]})
                    .expand_dims("quantiles")
                )  # assign and expand dimension for tercile (in order to desribe the range 0-0.33, we take the mean)
                tercile_1 = (
                    xr.where(
                        (dta_in >= tercile_ref[:, :, 0, :, :])
                        & (dta_in < tercile_ref[:, :, 1, :, :]),
                        1,
                        0,
                    )
                    .assign_coords({"quantiles": tercile_name[1]})
                    .expand_dims("quantiles")
                )
                tercile_2 = (
                    xr.where(dta_in < tercile_ref[:, :, 1, :, :], 1, 0)
                    .assign_coords({"quantiles": tercile_name[2]})
                    .expand_dims("quantiles")
                )

                # Write in one xarray
                tercile = xr.merge([tercile_0, tercile_1, tercile_2])
                tercile = tercile[variable]

                # Sum over ensemble dimension, in order to calculate the number of ensembles, which fulfill this condition
                # extremes
                extreme_sum = extreme.sum(dim="ensemble")

                # quintile
                quintile_sum = quintile.sum(dim="ensemble")

                # tercile
                tercile_sum = tercile.sum(dim="ensemble")

                # Create Lat-Lon-Mask, in order to distinguish between 0 (because of NAN) and 0 (because of no ensemble member falls into category) inside the categories of ensemble members
                mask = xr.where(
                    dta_in[0, 0, :, :] * 0 == 0, 1, np.nan
                )  # all real values multiplied with 0 will lead to 0 and will be replaced by 1, otherwise nan multiplied with zero results in nan and will replaced by nan

                # Calculate probability
                # extreme
                extreme_sum = extreme_sum / ens * 100 * mask

                # quintile
                quintile_sum = quintile_sum / ens * 100 * mask

                # tercile
                tercile_sum = tercile_sum / ens * 100 * mask

                # Get the categories with the most ensemble members
                # Here, Y holds the probability of a category and I the index of that
                # category
                # extreme
                Y_extreme = extreme_sum.max(dim="quantiles")
                I_extreme = extreme_sum.idxmax(dim="quantiles")

                # quintile
                Y_quintile = quintile_sum.max(dim="quantiles")
                I_quintile = quintile_sum.idxmax(dim="quantiles")

                # tercile
                Y_tercile = tercile_sum.max(dim="quantiles")
                I_tercile = tercile_sum.idxmax(dim="quantiles")

                #  In cases where all ensemble member fall in the same category, set the probability to 99.9% so that we do not "jump" into the next category´
                # extreme
                Y_extreme = Y_extreme.where(Y_extreme != 100, 99.99)
                # quintile
                Y_quintile = Y_quintile.where(Y_quintile != 100, 99.99)
                # tercile
                Y_tercile = Y_tercile.where(Y_tercile != 100, 99.99)

                # Construct a new variable which holds the category and the corresponding probability
                max_categ_quintile = I_quintile + Y_quintile / 100
                max_categ_tercile = I_tercile + Y_tercile / 100
                max_categ_extreme = I_extreme + Y_extreme / 100

                # In cases where the probability of the maximum category "less than low", set these pixels to -0.5; this is in accordance with the approach from SMHI; see
                # https://hypewebapp.smhi.se/hypeweb-climate/seasonal-forecasts/metadata/Glorious_SeFo_Metadata_RiverFlow.pdf
                max_categ_quintile = max_categ_quintile.where(Y_quintile != 25, -0.5)
                max_categ_tercile = max_categ_tercile.where(Y_tercile != 35, -0.5)

                # For precipitation forecasts, set all pixels with less than 1mm/day to -0.5
                if variable == "tp":

                    # start here
                    max_categ_quintile = max_categ_quintile.where(
                        (extreme_ref[0, :, 1, :, :] >= 1)
                        | (np.isnan(extreme_ref[0, :, 1, :, :])),
                        -0.5,
                    )  # we have to select the "first" ensemble member of reference, because we artificially created that dimension for processing, and we have to keep our nans
                    max_categ_tercile = max_categ_tercile.where(
                        (extreme_ref[0, :, 1, :, :] >= 1)
                        | (np.isnan(extreme_ref[0, :, 1, :, :])),
                        -0.5,
                    )
                    max_categ_extreme = max_categ_extreme.where(
                        (extreme_ref[0, :, 1, :, :] >= 1)
                        | (np.isnan(extreme_ref[0, :, 1, :, :])),
                        -0.5,
                    )

                #  For all the temperature data, transform values from °K to °C
                # if (vars == "t2m" or vars == "t2min" or vars == "t2max"):
                #     ens_mean = ens_mean - 273.15;
                #     ens_median = ens_median - 273.15;

                # Round most of the variables to one digit

                ens_mean = np.round(ens_mean, 1)
                ens_median = np.round(ens_median, 1)
                ens_spread = np.round(ens_spread, 1)
                ens_std = np.round(ens_std, 1)
                ens_iqr = np.round(ens_iqr, 1)
                delta_clim = np.round(delta_clim, 1)
                ens_mean_rel = np.round(ens_mean_rel, 1)

                max_categ_tercile = np.round(max_categ_tercile, 2)
                max_categ_quintile = np.round(max_categ_quintile, 2)
                max_categ_extreme = np.round(max_categ_extreme, 2)

                terciles_sum = np.round(tercile_sum, 1)
                extreme_sum = np.round(extreme_sum, 1)

                # Write out NetCDF-File
                coords = {
                    "time": dta_in["time"].values,
                    "lat": dta_in["lat"].values.astype(np.float32),
                    "lon": dta_in["lon"].values.astype(np.float32),
                }

                # Set filename of netCDF:
                fnme_out = f"{domain_config['raw_forecasts']['prefix']}_eval_{variable}_{year}_{month_str}_{domain_config['prefix']}.nc"

                ds = modules.create_3d_netcdf(
                    f"{dir_dict['monthly_eval']}/{fnme_out}",
                    attribute_config,
                    variable,
                    variable_config,
                    coords,
                )
                encoding = modules.set_encoding(variable_config, coords, type="eval")

                for variable_eval in variable_eval_config:
                    # write data to ds
                    ds_out_sel = ds[[variable_eval]]

                    # Fill this variable with corresponding values
                    if variable_eval == "ensemble_mean":
                        ds_out_sel[variable_eval].values = ens_mean.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_median":
                        ds_out_sel[variable_eval].values = ens_median.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_spread":
                        ds_out_sel[variable_eval].values = ens_spread.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_std":
                        ds_out_sel[variable_eval].values = ens_std.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_iqr":
                        ds_out_sel[variable_eval].values = ens_iqr.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_anomaly":
                        ds_out_sel[variable_eval].values = delta_clim.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "ensemble_mean_relative":
                        ds_out_sel[variable_eval].values = ens_mean_rel.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "tercile_probab":
                        ds_out_sel[variable_eval].values = max_categ_tercile.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "quintile_probab":
                        ds_out_sel[variable_eval].values = max_categ_quintile.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "extreme_probab":
                        ds_out_sel[variable_eval].values = max_categ_extreme.transpose(
                            "time", "lat", "lon"
                        ).values
                    elif variable_eval == "above_normal_probab":
                        ds_out_sel[variable_eval].values = (
                            tercile_sum[2, :, :, :]
                            .transpose("time", "lat", "lon")
                            .values
                        )
                    elif variable_eval == "below_normal_probab":
                        ds_out_sel[variable_eval].values = (
                            tercile_sum[0, :, :, :]
                            .transpose("time", "lat", "lon")
                            .values
                        )
                    elif variable_eval == "extreme_high_probab":
                        ds_out_sel[variable_eval].values = (
                            extreme_sum[1, :, :, :]
                            .transpose("time", "lat", "lon")
                            .values
                        )
                    elif variable_eval == "extreme_low_probab":
                        ds_out_sel[variable_eval].values = (
                            extreme_sum[0, :, :, :]
                            .transpose("time", "lat", "lon")
                            .values
                        )

                    ds_out_sel.to_netcdf(
                        f"{dir_dict['monthly_eval']}/{fnme_out}",
                        mode="a",
                        format="NETCDF4_CLASSIC",
                        engine="netcdf4",
                        encoding={variable_eval: encoding[variable_eval]},
                    )
