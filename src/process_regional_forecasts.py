# import packages
import argparse
import json
import logging
import os
import sys
from os.path import exists
import shutil
import dask
import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from rechunker import rechunk

# import dir_fnme_v2 as dir_fnme
import helper_modules
import regional_processing_modules

# from helper_modules import getCluster


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
        "-v",
        "--variables",
        action="store",
        type=str,
        help="Variable",
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
 
    os.makedirs(os.path.dirname(f"logs/{domain_name}_setup_domain.log"), exist_ok=True)

    logging.basicConfig(
        filename=f"logs/{domain_name}_setup_domain.log",
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

    try:
        domain_config = domain_config[args.domain]
    except:
        logging.error(f"Init: no configuration for domain {args.domain}")
        sys.exit()

    if args.variables is not None:
        variable_config = {
            key: value
            for key, value in variable_config.items()
            if key in args.variables
        }
        print(variable_config)
    else:
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

    # Get some ressourcers
    if args.partition is not None:
        client, cluster = helper_modules.getCluster(
            args.partition, args.nodes, args.ntasks
        )

        client.get_versions(check=True)

        #client.amm.start()

        print(f"Dask dashboard available at {client.dashboard_link}")

    if args.scheduler_file is not None:
        client = Client(scheduler_file=args.scheduler_file)

        client.get_versions(check=True)
        client.amm.start()

        print(f"Dask dashboard available at {client.dashboard_link}")

    # Create some code for calculation of history files for period syr_calib to eyr_calib
    if args.mode == "truncate_forecasts":

        for year in process_years:
            
            results = []

            for month in process_months:

                # calculate the month_range, in order to just have a look at the 7 month, and not the first day of 8th month
                # month_end = month + 7
                # if month_end < 14:
                #     month_range = np.arange(month, month_end)
                # else:
                #     month_range_1 = np.arange(month, 13)
                #     month_range_2 = np.arange(1, month_end - 12)
                #     month_range = np.concatenate((month_range_1, month_range_2), axis=0)
                # Store it as a list
                # month_range = list(month_range)
                # print(month_range)

                results.append(
                    regional_processing_modules.truncate_forecasts(
                        domain_config,
                        variable_config,
                        reg_dir_dict,
                        glob_dir_dict,
                        year,
                        month,
                        # month_range,
                    )
                )
                
            try:
                dask.compute(results)
                logging.info("Truncate forecasts: successful")
            except:
                logging.warning("Truncate forecasts: Something went wrong")

    elif args.mode == "remap_forecasts":

        results = []

        for variable in variable_config:

            for year in process_years:

                for month in process_months:

                    results.append(
                        regional_processing_modules.remap_forecasts(
                            domain_config,
                            reg_dir_dict,
                            year,
                            month,
                            grid_file,
                            variable,
                        )
                    )

        try:
            with ProgressBar():
                dask.compute(results)
            logging.info("Remap forecasts: successful")

        except:
            logging.error("Remap forecasts: Something went wrong")

    #

    # Concat SEAS5-Forecast on a daily Basis for calibration period or other desired period
    elif args.mode == "concat_forecasts_daily":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]


        # Loop over variables, years, and months and save filenames of all selected forecasts in a list
        for month in process_months:

            for variable in variable_config:
                flenms = []
                for year in process_years:

                    fle_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
                    full_in = f"{reg_dir_dict['raw_forecasts_target_resolution_dir']}/{fle_in}"

                    flenms.append(full_in)


                # Now, let's open all files and concat along the time-dimensions
                ds = xr.open_mfdataset(
                    flenms,
                    parallel=True,
                    chunks={"time": 5, "ens": 25, "lat": "auto", "lon": "auto"},
                    engine="netcdf4",
                    autoclose=True,
                )

                # We need this step, because otherwise the chunks are not equally distributed....
                ds = ds.chunk({"time": 5, "ens": 25, "lat": "auto", "lon": "auto"})


                if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
                else:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}.zarr"

                full_out = f"{reg_dir_dict['raw_forecasts_zarr_dir']}{zarr_out}"

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
                        ds.to_zarr(full_out, encoding={variable: encoding[variable]})
                        logging.info("Concat forecast: writing to new file succesful")
                    except:
                        logging.error("Concat forecast: writing to new file failed")

    # Concat SEAS5-Forecast on a daily Basis for calibration period or other desired period
    elif args.mode == "concat_forecasts_monthly":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]


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
                flenms = []
                for year in process_years:
                    fle_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
                    full_in = f"{reg_dir_dict['raw_forecasts_target_resolution_dir']}/{fle_in}"

                    flenms.append(full_in)

                # Now, let's open all files and concat along the time-dimensions
                ds = xr.open_mfdataset(
                    flenms,
                    parallel=True,
                    chunks={"time": 5, "ens": 25, "lat": "auto", "lon": "auto"},
                    engine="netcdf4",
                    autoclose=True,
                )

                # Take monthly mean for each year
                ds_mon = ds.resample(time="1MS").mean()
                # Only select the months which are needed, because resample add nan-values for other months, which we are not interested in
                ds_mon = ds_mon.sel(time=ds_mon.time.dt.month.isin(month_range))

                # We need this step, because otherwise the chunks are not equally distributed....
                ds_mon = ds_mon.chunk({"time": 5, "ens": 25, "lat": "auto", "lon": "auto"})


                if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
                else:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}.zarr"

                full_out = f"{reg_dir_dict['seas_forecast_mon_zarr_dir']}{zarr_out}"

                # First, let's check if a ZARR-file exists
                if exists(full_out):
                    try:

                        ds_mon.to_zarr(full_out, mode="a", append_dim="time")
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

                        ds_mon.to_zarr(full_out, encoding={variable: encoding[variable]})
                        logging.info("Concat forecast: writing to new file succesful")
                    except:

                        logging.error("Concat forecast: writing to new file failed")

    elif args.mode == "rechunk_forecasts":
        for variable in variable_config:
            for month in process_months:

                if process_years[-1] < 2017:
                    zarr_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{month:02d}_{domain_config['target_resolution']}_calib.zarr"
                else:
                    zarr_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}.zarr"

                full_in = f"{reg_dir_dict['raw_forecasts_zarr_dir']}{zarr_in}"

                if process_years[-1] < 2017:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{month:02d}_{domain_config['target_resolution']}_calib_linechunks.zarr"
                else:
                    zarr_out = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{month:02d}_{domain_config['target_resolution']}_linechunks.zarr"

                full_out = f"{reg_dir_dict['raw_forecasts_zarr_dir']}{zarr_out}"

                intermed = f"{reg_dir_dict['raw_forecasts_zarr_dir']}intermed.zarr"

                # Delete the directory of the intermediate files
                if exists(intermed):
                    shutil.rmtree(intermed)

                # This needs to be changed as we might want to add more data to the ZARR stores
                if exists(full_out):
                    shutil.rmtree(full_out)

                ds = xr.open_zarr(
                    full_in, chunks={"time": 5, "ens": 25, "lat": "auto", "lon": "auto"}
                )

                encoding = helper_modules.set_zarr_encoding(variable_config)
                
                ds = ds.chunk({"time": len(ds.time), "ens": len(ds.ens), "lat": "auto", "lon": "auto"})
                
                ds.to_zarr(full_out, encoding={variable: encoding[variable], 'lat': encoding['lat'], 'lon': encoding['lon'], 'time': encoding['time']})
                

                #rechunked = rechunk(
                #    ds,
                #    target_chunks={
                #        "time": len(ds.time),
                #        "ens": len(ds.ens),
                #        "lat": 1,
                #        "lon": 1,
                #    },
                #    target_store=full_out,
                #    max_mem="2000MB",
                #    temp_store=intermed,
                #    target_options=encoding,
                #)

            #with ProgressBar():
            #    rechunked.execute()

    elif args.mode == "truncate_reference":

        for variable in variable_config:
            
            results = []
            
            for year in process_years:
                
                results.append(
                    regional_processing_modules.truncate_reference(
                        domain_config,
                        variable_config,
                        reg_dir_dict,
                        glob_dir_dict,
                        year,
                        variable,
                    )
                )

            try:
                dask.compute(results)
                logging.info("Truncate reference: successful")
            except:
                logging.warning("Truncate reference: Something went wrong")

    # calculate t2plus and t2minus
    elif args.mode == "calc_t2plus_minus":
        # variable config only for t2plus and t2minus
        # Read the variable configuration from the respective JSON
        with open("conf/variable_config.json", "r") as j:
            variable_config = json.loads(j.read())

        variable_config_t2plus_minus = {
            key: value
            for key, value in variable_config.items()
            if key in ["t2plus", "t2minus"]
        }

        for year in process_years:
            fnme_lst = []
            # load t2m
            file_t2m = f"{domain_config['reference_history']['prefix']}_t2m_{year}.nc"
            full_t2m = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_t2m}"
            fnme_lst.append(full_t2m)
            # t2max
            file_t2max = f"{domain_config['reference_history']['prefix']}_t2max_{year}.nc"
            full_t2max = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_t2max}"
            fnme_lst.append(full_t2max)
            # t2min
            file_t2min = f"{domain_config['reference_history']['prefix']}_t2min_{year}.nc"
            full_t2min = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_t2min}"
            fnme_lst.append(full_t2min)

            # Open all together
            ds = xr.open_mfdataset(
                fnme_lst,
                parallel=True,
                chunks={"time": 50},
                engine="netcdf4",
                autoclose=True,
            )

            try:
                # drop time_bounds
                ds = ds.drop_vars("time_bnds")
            except:
                print("no bnds available")

            # Calculate t2plus and t2minus
            ds["t2plus"] = ds.t2max - ds.t2m
            ds["t2minus"] = ds.t2m - ds.t2min

            # t2max
            file_t2plus = f"{domain_config['reference_history']['prefix']}_t2plus_{year}.nc"
            full_t2plus= f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_t2plus}"

            # t2min
            file_t2minus = f"{domain_config['reference_history']['prefix']}_t2minus_{year}.nc"
            full_t2minus = f"{reg_dir_dict['reference_initial_resolution_dir']}/{file_t2minus}"

            coords = {
                "time": ds["time"].values,
                "lat": ds["lat"].values.astype(np.float32),
                "lon": ds["lon"].values.astype(np.float32),
            }

            encoding = helper_modules.set_encoding(variable_config_t2plus_minus, coords)

            # Store as netcdf
            try:
                ds["t2plus"].to_netcdf(full_t2plus, encoding={"t2plus": encoding["t2plus"]})
                ds["t2minus"].to_netcdf(full_t2minus, encoding={"t2minus": encoding["t2minus"]})
            except:
                print("Calculation t2plus, t2minus: something went wrong")


    elif args.mode == "remap_reference":

        results = []

        for variable in variable_config:

            for year in process_years:

                results.append(
                    regional_processing_modules.remap_reference(
                        domain_config,
                        reg_dir_dict,
                        year,
                        grid_file,
                        variable,
                    )
                )

        try:
            with ProgressBar():
                dask.compute(results)
            logging.info("Remap forecasts: successful")

        except:
            logging.error("Remap forecasts: Something went wrong")

    # Concat reference for BCSD (for calibration period) or for any desired period (daily-data)
    elif args.mode == "concat_reference_daily":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]


        # Loop over variables, years, and months and save filenames of all selected forecasts in a list
        for variable in variable_config:
            filenames = []
            for year in process_years:

                file_out = f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}.nc"
                full_out = (
                    f"{reg_dir_dict['reference_target_resolution_dir']}/{file_out}"
                )

                filenames.append(full_out)

            # Now, let's open all files and concat along the time-dimensions
            ds = xr.open_mfdataset(
                filenames,
                parallel=False,
                # chunks={'time': 5, 'lat': 'auto', 'lon': 'auto'},
                engine="netcdf4",
                autoclose=True,
            )

            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_out = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib.zarr"
            else:
                zarr_out = f"{domain_config['reference_history']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{domain_config['target_resolution']}.zarr"

            full_out = f"{reg_dir_dict['reference_zarr_dir']}{zarr_out}"

            ds = ds.chunk({"time": 50})

            # First, let's check if a ZARR-file exists
            if exists(full_out):
                try:
                    ds.to_zarr(full_out, mode="a", append_dim="time")
                    logging.info("Concat forecast: appending succesful")
                except:
                    logging.error("Concat forecast: something went wrong during appending")

            else:
                coords = {
                    "time": ds["time"].values,
                    "lat": ds["lat"].values.astype(np.float32),
                    "lon": ds["lon"].values.astype(np.float32),
                }

                encoding = helper_modules.set_zarr_encoding(variable_config)
                try:
                    ds.to_zarr(full_out, encoding={variable: encoding[variable]})
                    logging.info("Concat forecast: writing to new file succesful")
                except:
                    logging.error("Concat forecast: writing to new file failed")

        #            if domain_config["reference_history"]["merged_variables"]:
        #                month_str = "01"  # dummy
        #                fnme_dict = dir_fnme.set_filenames(
        #                    domain_config, year, month_str, True, variable
        #                )
        #                fle_list.append(
        #                    f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_raw_dir']}"
        #                )
        #                fle_string = fle_list
        #            else:
        #                # Update Filenames
        #                month_str = "01"  # dummy
        #                fnme_dict = dir_fnme.set_filenames(
        #                    domain_config, year, month_str, False, variable
        #                )
        #                fle_string = f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_dir']}"
        #                results.append(
        #                    regional_processing_modules.truncate_reference(
        #                        domain_config,
        #                        variable_config,
        #                        dir_dict,
        #                        fnme_dict,
        #                        fle_string,
        #                        variable,
        #                    )
        #                )#
        #
        #        if domain_config["reference_history"]["merged_variables"]:
        #               results.append(
        #                   regional_processing_modules.truncate_reference(
        #                       domain_config,
        #                       variable_config,
        #                       dir_dict,
        #                       fnme_dict,
        #                       fle_string,
        #                      variable,
        #                  )
        #              )

    # Concat reference for BCSD (for calibration period) or for any desired period (monthly data))
    elif args.mode == "concat_reference_monthly":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]


        # Loop over variables, years, and months and save filenames of all selected forecasts in a list
        for variable in variable_config:
            filenames = []
            for year in process_years:
                file_out = f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}.nc"
                full_out = (
                    f"{reg_dir_dict['reference_target_resolution_dir']}/{file_out}"
                )

                filenames.append(full_out)

            # Now, let's open all files and concat along the time-dimensions
            ds = xr.open_mfdataset(
                filenames,
                parallel=True,
                # chunks={'time': 5, 'lat': 'auto', 'lon': 'auto'},
                engine="netcdf4",
                autoclose=True,
            )
            ds_mon = ds.resample(time="1MS").mean()

            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_out = f"{domain_config['reference_history']['prefix']}_mon_{variable}_{domain_config['target_resolution']}_calib.zarr"
            else:
                zarr_out = f"{domain_config['reference_history']['prefix']}_mon_{variable}_{process_years[0]}_{process_years[-1]}_{domain_config['target_resolution']}.zarr"

            full_out = f"{reg_dir_dict['ref_forecast_mon_zarr_dir']}{zarr_out}"

            ds = ds.chunk({"time": 50})

            # First, let's check if a ZARR-file exists
            if exists(full_out):
                try:
                    ds_mon.to_zarr(full_out, mode="a", append_dim="time")
                    logging.info("Concat forecast: appending succesful")
                except:
                    logging.error("Concat forecast: something went wrong during appending")

            else:
                coords = {
                    "time": ds["time"].values,
                    "lat": ds["lat"].values.astype(np.float32),
                    "lon": ds["lon"].values.astype(np.float32),
                }

                encoding = helper_modules.set_zarr_encoding(variable_config)
                try:
                    ds_mon.to_zarr(full_out, encoding={variable: encoding[variable]})
                    logging.info("Concat forecast: writing to new file succesful")
                except:
                    logging.error("Concat forecast: writing to new file failed")

        #            if domain_config["reference_history"]["merged_variables"]:
        #                month_str = "01"  # dummy
        #                fnme_dict = dir_fnme.set_filenames(
        #                    domain_config, year, month_str, True, variable
        #                )
        #                fle_list.append(
        #                    f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_raw_dir']}"
        #                )
        #                fle_string = fle_list
        #            else:
        #                # Update Filenames
        #                month_str = "01"  # dummy
        #                fnme_dict = dir_fnme.set_filenames(
        #                    domain_config, year, month_str, False, variable
        #                )
        #                fle_string = f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_dir']}"
        #                results.append(
        #                    regional_processing_modules.truncate_reference(
        #                        domain_config,
        #                        variable_config,
        #                        dir_dict,
        #                        fnme_dict,
        #                        fle_string,
        #                        variable,
        #                    )
        #                )#
        #
        #        if domain_config["reference_history"]["merged_variables"]:
        #               results.append(
        #                   regional_processing_modules.truncate_reference(
        #                       domain_config,
        #                       variable_config,
        #                       dir_dict,
        #                       fnme_dict,
        #                       fle_string,
        #                      variable,
        #                  )
        #              )



    # Rechunk reference for calibration period or for other periods (other periods are not tested yet)
    elif args.mode == "rechunk_reference":
        syr_calib = domain_config["syr_calib"]
        eyr_calib = domain_config["eyr_calib"]
        for variable in variable_config:
            # set input files
            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_in = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib.zarr"
            else:
                zarr_in = f"{domain_config['reference_history']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{domain_config['target_resolution']}.zarr"

            full_in = f"{reg_dir_dict['reference_zarr_dir']}{zarr_in}"

            # set output files

            if process_years[0] == syr_calib and process_years[-1] == eyr_calib:
                zarr_out = f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['target_resolution']}_calib_linechunks.zarr"
            else:
                zarr_out = f"{domain_config['reference_history']['prefix']}_{variable}_{process_years[0]}_{process_years[-1]}_{domain_config['target_resolution']}_linechunks.zarr"

            full_out = f"{reg_dir_dict['reference_zarr_dir']}{zarr_out}"

            intermed = f"{reg_dir_dict['reference_zarr_dir']}intermed.zarr"

            # Delete the directory of the intermediate files
            if exists(intermed):
                shutil.rmtree(intermed)

            # This needs to be changed as we might want to add more data to the ZARR stores
            if exists(full_out):
                shutil.rmtree(full_out)

            ds = xr.open_zarr(full_in, chunks={"time": 50, "lat": "auto", "lon": "auto"})

            encoding = helper_modules.set_zarr_encoding(variable_config)

            rechunked = rechunk(
                ds,
                target_chunks={"time": len(ds.time), "lat": 1, "lon": 1},
                target_store=full_out,
                max_mem="2000MB",
                temp_store=intermed,
                target_options=encoding,
            )

            with ProgressBar():
                rechunked.execute()
