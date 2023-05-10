# import packages
import argparse
import json
import logging

# from cdo import *
import dask
from dask.distributed import Client
import helper_modules
import regional_processing_modules
from helper_modules import run_cmd
import xarray as xr
# cdo = Cdo()
import numpy as np
from os.path import exists

def get_clas():
    parser = argparse.ArgumentParser(
        description="Creation of a new domain for BCSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d", "--domain", action="store", type=str, help="Domain", required=True
    )
    parser.add_argument(
         "-m",
         "--mode",
        action="store",
        type=str,
         help="Selected mode for setup",
        required=True,
    )

    parser.add_argument(
        "-Y",
        "--Years",
        action="store",
        type=str,
        help="Years for which the processing should be executed",
        required=True,
    )

    parser.add_argument(
        "-M",
        "--Months",
        action="store",
        type=str,
        help="Months for which the processing should be executed",
        required=False,
    )

    parser.add_argument(
        "-N",
        "--nodes",
        action="store",
        type=int,
        help="Number of nodes for running the code",
        required=False,
    )

    parser.add_argument(
        "-n",
        "--ntasks",
        action="store",
        type=int,
        help="Number of tasks / CPUs",
        required=False,
    )

    parser.add_argument(
        "-p",
        "--partition",
        action="store",
        type=str,
        help="Partition to which we want to submit the job",
        required=False,
    )

    parser.add_argument(
        "-f",
        "--scheduler_file",
        action="store",
        type=str,
        help="""If a scheduler-file is provided, the function does not start its own cluster 
            but rather uses a running environment""",
        required=False,
    )

    return parser.parse_args()


def setup_logger(domain_name):
    logging.basicConfig(
        filename=f"logs/{domain_name}_aggregate.log",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )
    # encoding='utf-8'


if __name__ == "__main__":

    # Read the command line arguments
    args = get_clas()

    # Create a new logger file (or append to an existing file)
    setup_logger(args.domain)

    # Read the domain configuration from the respective JSON
    with open("conf/domain_config.json", "r") as j:
        domain_config = json.loads(j.read())

    # Read the global configuration from the respective JSON --> Add this as further input parameter
    with open("conf/attribute_config.json", "r") as j:
        attribute_config = json.loads(j.read())

    # Read the variable configuration from the respective JSON
    with open("conf/variable_config.json", "r") as j:
        variable_config = json.loads(j.read())

    # Set domain
    domain_config = domain_config[args.domain]

    variable_config = {
        key: value
        for key, value in variable_config.items()
        if key in domain_config["variables"]
    }

    reg_dir_dict, glob_dir_dict = helper_modules.set_and_make_dirs(domain_config)

    # get filename of grid-File
    grid_file = f"{reg_dir_dict['static_dir']}/domain_grid.txt"

    grid_file = regional_processing_modules.create_grd_file(domain_config, grid_file)

    process_years = helper_modules.decode_processing_years(args.Years)


    if args.Months is not None:
        process_months = helper_modules.decode_processing_months(args.Months)
    #print(process_years)
    #print(process_months)
    # Get some ressourcers
    if args.partition is not None:
        client, cluster = helper_modules.getCluster(
            args.partition, args.nodes, args.ntasks
        )

        client.get_versions(check=True)
        client.amm.start()

        print(f"Dask dashboard available at {client.dashboard_link}")

    if args.scheduler_file is not None:
        client = Client(scheduler_file=args.scheduler_file)

        client.get_versions(check=True)
        client.amm.start()

        print(f"Dask dashboard available at {client.dashboard_link}")

# Process steps:
    # 1. Process Ref (ERA5-Land)
        # a) Calculate daily to monthly data
    # 1. Calculate daily to monthly BCSD-files for period 1981 to 2016
    # 2. Calculate daily to monthly files ERA5-Land

    # Convert SEAS5 raw daily data to monthly data (store in seperate files)
    if args.mode == "day2mon_seas":
        results = []
        for variable in variable_config:

            for year in process_years:

                for month in process_months:
                    results.append(
                        helper_modules.day2mon_seas(domain_config, variable_config, reg_dir_dict, year, month,
                                                    variable))

        try:
            dask.compute(results)
            logging.info("Day to month: successful")
        except:
            logging.warning("Day to month: Something went wrong")


    # Ref

    # Convert REF from daily to monthly data (separate files):
    if args.mode == "day2mon_ref":
        results = []
        for variable in variable_config:

            for year in process_years:

                results.append(helper_modules.day2mon_ref(domain_config, variable_config, reg_dir_dict, year, variable))

        try:
            dask.compute(results)
            logging.info("Day to month ref: successful")
        except:
            logging.warning("Day to month ref: Something went wrong")



    # Convert BCSD daily data to monthly data (store in seperate files)
    if args.mode == "day2mon_bcsd":
        results = []
        for variable in variable_config:

            for year in process_years:

                for month in process_months:

                    results.append(helper_modules.day2mon_bcsd(domain_config,variable_config, reg_dir_dict, year, month, variable))

        try:
            dask.compute(results)
            logging.info("Day to month: successful")
        except:
            logging.warning("Day to month: Something went wrong")

    # Concat BCSD-Forecast on a daily Basis for calibration period or other desired period
    elif args.mode == "concat_bcsd_daily":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        flenms = []

        # Loop over variables, years, and months and save filenames of all selected forecasts in a list
        for month in process_months:

            for variable in variable_config:

                for year in process_years:
                    # Get BCSD-Filename pp_full
                    (raw_full, pp_full, refrcst_full, ref_full,) = helper_modules.set_input_files(domain_config,
                                                                                                  reg_dir_dict, month,
                                                                                                  year, variable)
                    # set input files
                    full_in = pp_full
                    flenms.append(full_in)

            # Now, let's open all files and concat along the time-dimensions
            ds = xr.open_mfdataset(
                flenms,
                parallel=True,
                chunks={"time": 215, "ens": 25, "lat": "auto", "lon": "auto"},
                engine="netcdf4",
                autoclose=True,
            )

            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
            else:
                zarr_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}.zarr"

            full_out = f"{reg_dir_dict['bcsd_forecast_zarr_dir']}{zarr_out}"

            # First, let's check if a ZARR-file exists
            if exists(full_out):
                try:
                    ds.to_zarr(full_out, mode="a", append_dim="time")
                    logging.info("Concat forecast: appending succesful")
                except:
                    logging.error(
                        "Concat forecast: something went wrong during appending"
                    )

            else:
                coords = {
                    "time": ds["time"].values,
                    "ens": ds["ens"].values,
                    "lat": ds["lat"].values.astype(np.float32),
                    "lon": ds["lon"].values.astype(np.float32),
                }

                encoding = helper_modules.set_zarr_encoding(variable_config)

                try:
                    ds.to_zarr(full_out, encoding=encoding)
                    logging.info("Concat forecast: writing to new file succesful")
                except:
                    logging.error("Concat forecast: writing to new file failed")


    # Concat BCSD-Forecast on a monthly Basis for calibration period or other desired period
    elif args.mode == "concat_bcsd_monthly":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        flenms = []

        # Loop over variables, years, and months and save filenames of all selected forecasts in a list
        for month in process_months:
            # create list of month, which are included in SEAS5
            month_end = month + 8
            if month_end < 14:
                month_range = np.arange(month, month_end)
            else:
                month_range_1 = np.arange(month, 13)
                month_range_2 = np.arange(1, month_end - 12)
                month_range = np.concatenate((month_range_1, month_range_2), axis=0)
            # Store it as a list
            month_range = list(month_range)
            print(month_range)


            for variable in variable_config:
                for year in process_years:
                    # Get BCSD-Filename pp_full
                    (raw_full, pp_full, refrcst_full, ref_full,) = helper_modules.set_input_files(domain_config,
                                                                                                   reg_dir_dict, month,
                                                                                                   year, variable)
        #             # set input files
                    full_in = pp_full
                    flenms.append(full_in)
        #
              # Now, let's open all files and concat along the time-dimensions
            ds = xr.open_mfdataset(
                 flenms,
                 parallel=True,
                 chunks={"time": 215, "ens": 25, "lat": "auto", "lon": "auto"},
                 engine="netcdf4",
                 autoclose=True,
             )
        #
            # Take monthly mean for each year
            ds_mon = ds.resample(time="1MS").mean()
            # Only select the months which are needed, because resample add nan-values for other months, which we are not interested in
            ds_mon = ds_mon.sel(time=ds_mon.time.dt.month.isin(month_range))
        #
        #
            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
            else:
                zarr_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_mon_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}.zarr"
        #
            full_out = f"{reg_dir_dict['bcsd_forecast_mon_zarr_dir']}{zarr_out}"
        #
            # First, let's check if a ZARR-file exists
            if exists(full_out):
                try:
                    ds_mon.to_zarr(full_out, mode="a", append_dim="time")
                    logging.info("Concat forecast: appending succesful")
                except:
                    logging.error(
                         "Concat forecast: something went wrong during appending"
                     )
        #
            else:
                coords = {
                     "time": ds["time"].values,
                     "ens": ds["ens"].values,
                     "lat": ds["lat"].values.astype(np.float32),
                     "lon": ds["lon"].values.astype(np.float32),
                 }
        #
                encoding = helper_modules.set_zarr_encoding(variable_config)
        #
                try:
                    ds_mon.to_zarr(full_out, encoding=encoding)
                    logging.info("Concat forecast: writing to new file succesful")
                except:
                    logging.error("Concat forecast: writing to new file failed")


    # Create Climatology for ERA5-Land
    elif args.mode == "climatology":
        results = []
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]

        for variable in variable_config:

            results.append(helper_modules.create_climatology(domain_config, variable_config, reg_dir_dict, syr_calib, eyr_calib, variable))

        # print(results)
        try:
            dask.compute(results)
            logging.info("REF climatology: successful")
        except:
            logging.warning("REF climatology: Something went wrong")


    # Calc quantile for REF-Product (ERA5-Land) --> Input: ERA5 on monthly basis
    elif args.mode == "quantile_ref":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        # Loop over variables
        for variable in variable_config:

            # Set input File
            # fle_in = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
            # full_in = f"{reg_dir_dict['reference_zarr_dir']}{fle_in}"

            # Take the monthly aggregated data
            # or use
            # fle_in = f"{domain_config['reference_history']['prefix']}_mon_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
            fle_in = f"{domain_config['reference_history']['prefix']}_mon_{variable}_{domain_config['target_resolution']}_calib.zarr"
            full_in = f"{reg_dir_dict['ref_forecast_mon_zarr_dir']}{fle_in}"


            # Open dataset
            ds = xr.open_zarr(full_in, consolidated=False)
            ds = xr.open_zarr(
                full_in,
                chunks={"time": len(ds.time), "lat": 10, "lon": 10},
                consolidated=False
                # parallel=True,
                # engine="netcdf4",
            )

            # Calculate monthly mean for each year (only if daily data are used)
            # ds = ds[variable].resample(time="1MS").mean()

            # Calculate quantile, tercile and extremes on a monthly basis
            ds_quintiles = ds.groupby("time.month").quantile(q=[0.2, 0.4, 0.6, 0.8]) # , dim=["time"])
            ds_tercile = ds.groupby("time.month").quantile(q=[0.33, 0.66])
            ds_extreme = ds.groupby("time.month").quantile(q=[0.1, 0.9])

            # Set Filenames
            fle_out_quin = f"{domain_config['reference_history']['prefix']}_quintile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
            full_out_quin = f"{reg_dir_dict['statistic_dir']}/{fle_out_quin}"
            fle_out_ter = f"{domain_config['reference_history']['prefix']}_tercile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
            full_out_ter = f"{reg_dir_dict['statistic_dir']}/{fle_out_ter}"
            fle_out_ext = f"{domain_config['reference_history']['prefix']}_extreme_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
            full_out_ext = f"{reg_dir_dict['statistic_dir']}/{fle_out_ext}"


            # Save NC-File
            # ENCODING?!
            try:
                ds_quintiles.to_netcdf(full_out_quin)
            except:
                logging.error("Error: Create NC-File for quantiles")

            try:
                ds_quintiles.to_netcdf(full_out_ter)
            except:
                logging.error("Error: Create NC-File for tercile")

            try:
                ds_quintiles.to_netcdf(full_out_ext)
            except:
                logging.error("Error: Create NC-File for extreme")


    # Calc quantile for SEAS5 raw --> Input: SEAS5 Raw on monthly basis
    elif args.mode == "quantile_seas":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        # Loop over variables
        for variable in variable_config:
            for month in process_months:
                # Set input File
                # fle_in = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
                # full_in = f"{reg_dir_dict['reference_zarr_dir']}{fle_in}"

                # Take the monthly aggregated data
                # or use
                #  fle_in = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib_linechunks.zarr"
                fle_in = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
                full_in = f"{reg_dir_dict['seas_forecast_mon_zarr_dir']}{fle_in}"


                # Open dataset
                ds = xr.open_zarr(full_in, consolidated=False)
                ds = xr.open_zarr(
                    full_in,
                    chunks={"time": len(ds.time), "ens": len(ds.ens), "lat": 10, "lon": 10},
                    consolidated=False
                    # parallel=True,
                    # engine="netcdf4",
                )

                # Calculate monthly mean for each year (only if daily data are used)
                # ds = ds[variable].resample(time="1MS").mean()

                # Calculate quantile, tercile and extremes on a monthly basis
                ds_quintiles = ds.groupby("time.month").quantile(q=[0.2, 0.4, 0.6, 0.8], dim=["time", "ens"])
                ds_tercile = ds.groupby("time.month").quantile(q=[0.33, 0.66], dim=["time", "ens"]) # , dim=["ens"])
                ds_extreme = ds.groupby("time.month").quantile(q=[0.1, 0.9], dim=["time", "ens"]) # , dim=["ens"])
                # print(ds_quintiles)
                # Set Filenames
                fle_out_quin = f"{domain_config['raw_forecasts']['prefix']}_quintile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_quin = f"{reg_dir_dict['statistic_dir']}/{fle_out_quin}"
                fle_out_ter = f"{domain_config['raw_forecasts']['prefix']}_tercile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_ter = f"{reg_dir_dict['statistic_dir']}/{fle_out_ter}"
                fle_out_ext = f"{domain_config['raw_forecasts']['prefix']}_extreme_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_ext = f"{reg_dir_dict['statistic_dir']}/{fle_out_ext}"


                # Save NC-File
                # ENCODING?!
                try:
                    ds_quintiles.to_netcdf(full_out_quin)
                except:
                    logging.error("Error: Create NC-File for quantiles")

                try:
                    ds_quintiles.to_netcdf(full_out_ter)
                except:
                    logging.error("Error: Create NC-File for tercile")

                try:
                    ds_quintiles.to_netcdf(full_out_ext)
                except:
                    logging.error("Error: Create NC-File for extreme")

    # Calc quantile for BCSD-Product --> Input: BCSD on monthly basis
    elif args.mode == "quantile_bcsd":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        # Loop over variables
        for variable in variable_config:
            for month in process_months:
                # Set input File
                # fle_in = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
                # full_in = f"{reg_dir_dict['reference_zarr_dir']}{fle_in}"

                # Take the monthly aggregated data
                # or use
                #  fle_in = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib_linechunks.zarr"
                fle_in = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
                full_in = f"{reg_dir_dict['bcsd_forecast_mon_zarr_dir']}{fle_in}"


                # Open dataset
                ds = xr.open_zarr(full_in, consolidated=False)
                ds = xr.open_zarr(
                    full_in,
                    chunks={"time": len(ds.time), "ens": len(ds.ens), "lat": 10, "lon": 10},
                    consolidated=False
                    # parallel=True,
                    # engine="netcdf4",
                )

                # Calculate monthly mean for each year (only if daily data are used)
                # ds = ds[variable].resample(time="1MS").mean()

                # Calculate quantile, tercile and extremes on a monthly basis
                ds_quintiles = ds.groupby("time.month").quantile(q=[0.2, 0.4, 0.6, 0.8], dim=["time", "ens"])
                ds_tercile = ds.groupby("time.month").quantile(q=[0.33, 0.66], dim=["time", "ens"]) # , dim=["ens"])
                ds_extreme = ds.groupby("time.month").quantile(q=[0.1, 0.9], dim=["time", "ens"]) # , dim=["ens"])
                # print(ds_quintiles)
                # Set Filenames
                fle_out_quin = f"{domain_config['bcsd_forecasts']['prefix']}_quintile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_quin = f"{reg_dir_dict['statistic_dir']}/{fle_out_quin}"
                fle_out_ter = f"{domain_config['bcsd_forecasts']['prefix']}_tercile_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_ter = f"{reg_dir_dict['statistic_dir']}/{fle_out_ter}"
                fle_out_ext = f"{domain_config['bcsd_forecasts']['prefix']}_extreme_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
                full_out_ext = f"{reg_dir_dict['statistic_dir']}/{fle_out_ext}"


                # Save NC-File
                # ENCODING?!
                try:
                    ds_quintiles.to_netcdf(full_out_quin)
                except:
                    logging.error("Error: Create NC-File for quantiles")

                try:
                    ds_quintiles.to_netcdf(full_out_ter)
                except:
                    logging.error("Error: Create NC-File for tercile")

                try:
                    ds_quintiles.to_netcdf(full_out_ext)
                except:
                    logging.error("Error: Create NC-File for extreme")