# import packages
import argparse
import json
import logging

import dask
import numpy as np
import xarray as xr
from dask.distributed import Client

import delayed_module
import helper_modules
from bc_module_v2 import bc_module


def get_clas():
    # insert period, for which the bcsd-should be running! similar to process_regional_forecast
    parser = argparse.ArgumentParser(
        description="Python-based BCSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d", "--domain", action="store", type=str, help="Domain", required=True
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
        "-c",
        "--crossval",
        action="store",
        type=str,
        help="If TRUE, do not use actual forecast for computing forecast climatology ",
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
    # parser.add_argument("-p", "--processes", action="store", type=int, help="Node for running the code", required=False)

    return parser.parse_args()


def setup_logger(domain_name):
    logging.basicConfig(
        filename=f"logs/{domain_name}_run_bcsd.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )


if __name__ == "__main__":

    args = get_clas()

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

    variable_config = {
        key: value
        for key, value in variable_config.items()
        if key in domain_config["variables"]
    }

    reg_dir_dict, glob_dir_dict = helper_modules.set_and_make_dirs(domain_config)

    process_years = helper_modules.decode_processing_years(args.Years)

    if args.Months is not None:
        process_months = helper_modules.decode_processing_months(args.Months)

    syr_calib = domain_config["syr_calib"]
    eyr_calib = domain_config["eyr_calib"]

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

    # Insert IF-Statement in order to run the bcsd for the historical files
    for year in process_years:

        for month in process_months:

            for variable in variable_config:

                (
                    raw_full,
                    pp_full,
                    refrcst_full,
                    ref_full,
                ) = helper_modules.set_input_files(
                    domain_config, reg_dir_dict, month, year, variable
                )

                coords = helper_modules.get_coords_from_frcst(raw_full)

                global_attributes = helper_modules.update_global_attributes(
                    attribute_config, domain_config["bc_params"], coords, args.domain
                )

                encoding = helper_modules.set_encoding(variable_config, coords)

                ds = helper_modules.create_4d_netcdf(
                    pp_full,
                    global_attributes,
                    domain_config,
                    variable_config,
                    coords,
                    variable,
                )

                print(f"Opening {ref_full}")
                ds_obs = xr.open_zarr(ref_full, consolidated=False)

                ds_obs = xr.open_zarr(
                    ref_full,
                    # chunks={"time": len(ds_obs.time), "lat": 'auto', "lon": 'auto'},
                    consolidated=False
                    # parallel=True,
                    # engine="netcdf4",
                )
                da_obs = ds_obs[variable].persist()

                print(f"Opening {refrcst_full}")
                if args.forecast_structure == "5D":
                    ds_mdl = helper_modules.preprocess_mdl_hist(
                        refrcst_full, month, variable
                    )  # chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 1, 'lon': 1})
                    da_mdl = ds_mdl.persist()
                elif args.forecast_structure == "4D":
                    ds_mdl = xr.open_zarr(refrcst_full, consolidated=False)
                    ds_mdl = xr.open_zarr(
                        refrcst_full,
                        # chunks={
                        #   "time": len(ds_mdl.time),
                        #   "ens": len(ds_mdl.ens),
                        #   "lat": 'auto',
                        #   "lon": 'auto'
                        # },
                        consolidated=False
                        # parallel=True,
                        # engine="netcdf4",
                    )
                    da_mdl = ds_mdl[variable]

                # Pred (current year for one month and 215 days)
                ds_pred = xr.open_dataset(raw_full)
                ds_pred = xr.open_mfdataset(
                    raw_full,
                    chunks={
                        "time": 1,
                        "ens": len(ds_pred.ens),
                        "lat": "auto",
                        "lon": "auto",
                    },
                    parallel=True,
                    engine="netcdf4",
                )
                da_pred = ds_pred[variable]

                if args.crossval == True:
                    da_mdl = da_mdl.sel(time=~da_pred.time)
                    da_obs = da_obs.sel(time=~da_pred.time)

                da_temp = xr.DataArray(
                    None,
                    dims=["time", "lat", "lon", "ens"],
                    coords={
                        "time": (
                            "time",
                            coords["time"],
                            {"standard_name": "time", "long_name": "time"},
                        ),
                        "ens": (
                            "ens",
                            coords["ens"],
                            {
                                "standard_name": "realization",
                                "long_name": "ensemble_member",
                            },
                        ),
                        "lat": (
                            "lat",
                            coords["lat"],
                            {
                                "standard_name": "latitude",
                                "long_name": "latitude",
                                "units": "degrees_north",
                            },
                        ),
                        "lon": (
                            "lon",
                            coords["lon"],
                            {
                                "standard_name": "longitude",
                                "long_name": "longitude",
                                "units": "degrees_east",
                            },
                        ),
                    },
                )

                for timestep in range(0, 20):

                    da_temp[timestep, :, :, :] = delayed_module.intersect_and_correct(
                        timestep,
                        variable,
                        domain_config,
                        variable_config,
                        da_mdl,
                        da_obs,
                        da_pred,
                    )

                # da_temp = da_temp_delayed.compute()

                # da_temp = da_temp.astype("float64")

                # Select only the actual variable from the output dataset
                # ds_out_sel = ds[[variable]]

                # Fill this variable with some data...
                # ds[variable].values = da_temp.transpose(
                #    "time", "ens", "lat", "lon"
                # ).values

                ## ...and save everything to disk..
                # ds_out_sel.to_netcdf(bcsd_dict[variable], mode='a', format='NETCDF4_CLASSIC', engine='netcdf4', encoding = {variable: encoding[variable]})
                # ds.to_netcdf(
                #    pp_full,
                #    mode="w",
                #    engine="netcdf4",
                #    encoding={variable: encoding[variable]},
                # )

                # for timestep in range(82, 83):

                # #print(f"Correcting timestep {timestep}...")
                # #dayofyear_mdl = ds_mdl["time.dayofyear"]
                # #day = dayofyear_mdl[timestep]##

                # # Deal with normal and leap years
                # #for calib_year in range(syr_calib, eyr_calib + 1):

                # #    print(calib_year)
                # #    print(year)

                # #    ds_obs_year = ds_obs.sel(time=ds_obs.time.dt.year == calib_year)
                # #    ds_mdl_year = ds_mdl.sel(time=ds_mdl.time.dt.year == calib_year)

                # #    dayofyear_obs = ds_obs_year["time.dayofyear"]
                # #    dayofyear_mdl = ds_mdl_year["time.dayofyear"]

                # #    # normal years
                # #    if len(ds_obs_year.time.values) == 365:

                #         # day_range = (np.arange(day - domain_config['bc_params']['window'], day + domain_config['bc_params']['window'] + 1) + 365) % 365 + 1
                #         day_range = (
                #             np.arange(
                #                 day - domain_config["bc_params"]["window"] - 1,
                #                 day + domain_config["bc_params"]["window"],
                #             )
                #             + 365
                #         ) % 365 + 1

                #         # leap years
                #     else:
                #         day_range = (
                #             np.arange(
                #                 day - domain_config["bc_params"]["window"] - 1,
                #                 day + domain_config["bc_params"]["window"],
                #             )
                #             + 366
                #         ) % 366 + 1

                #     intersection_day_obs_year = np.in1d(dayofyear_obs, day_range)
                #     intersection_day_mdl_year = np.in1d(dayofyear_mdl, day_range)

                #     if calib_year == syr_calib:
                #         intersection_day_obs = intersection_day_obs_year
                #         intersection_day_mdl = intersection_day_mdl_year
                #     else:
                #         intersection_day_obs = np.append(
                #             intersection_day_obs, intersection_day_obs_year
                #         )
                #         intersection_day_mdl = np.append(
                #             intersection_day_mdl, intersection_day_mdl_year
                #         )

                # da_obs_sub = da_obs.loc[dict(time=intersection_day_obs)]

                # with dask.config.set(
                #     **{"array.slicing.split_large_chunks": False}
                # ):  # --> I really don't know why we need to silence the warning here...
                #     da_mdl_sub = da_mdl.loc[dict(time=intersection_day_mdl)]

                # da_mdl_sub = da_mdl_sub.stack(
                #     ens_time=("ens", "time"), create_index=True
                # )
                # da_mdl_sub = da_mdl_sub.drop("time")

                # da_pred_sub = da_pred.isel(time=timestep)

                # da_temp[timestep, :, :] = xr.apply_ufunc(
                #     bc_module,
                #     da_pred_sub,
                #     da_obs_sub,
                #     da_mdl_sub,
                #     kwargs={
                #         "bc_params": domain_config["bc_params"],
                #         "precip": variable_config[variable]["isprecip"],
                #     },
                #     input_core_dims=[["ens"], ["time"], ["ens_time"]],
                #     output_core_dims=[["ens"]],
                #     vectorize=True,
                #     dask="parallelized",
                #     output_dtypes=[np.float64],
                # )

                # Change the datatype from "object" to "float64" --> Can we somehow get around this???

                # ds_out_sel.close()

    # client.close()

    # if cluster is not None:
    #     cluster.close()

    # coords = helper_modules.get_coords_from_frcst(list(raw_dict.values())[0])

    # Update Filenames:
    # fnme_dict = dir_fnme.set_filenames(domain_config, syr_calib, eyr_calib, year, month_str, domain_config['bcsd_forecasts']['merged_variables'])

    # Get directories for the corresponding variable
    # if args.domain == 'germany':
    # (
    #    raw_dict,
    #    bcsd_dict,
    #    ref_hist_dict,
    #    mdl_hist_dict,
    # ) = helper_modules.set_filenames(
    #    domain_config,
    #    variable_config,
    #    dir_dict,
    #    syr_calib,
    #    eyr_calib,
    #    year,
    #    month_str,
    #    False,
    # )
    # else:
    #     raw_dict, bcsd_dict, ref_hist_dict, mdl_hist_dict = helper_modules.set_filenames(args.year, args.month, domain_config, variable_config, True)

    # IMPLEMENT A CHECK IF ALL INPUT FILES ARE AVAILABL!!!!!!

    # Read the dimensions for the output file (current prediction)

    # attribute_config = helper_modules.update_global_attributes(
    #    attribute_config, domain_config["bc_params"], coords, args.domain
    # )

    # encoding = helper_modules.set_encoding(variable_config, coords)

    # Loop over each variable
    # for variable in variable_config:
    # Create an empty NetCDF in which we write the BCSD output
    #    ds = helper_modules.create_4d_netcdf(
    #        bcsd_dict,
    #        attribute_config,
    #        domain_config,
    #        variable_config,
    #        coords,
    #        variable,
    #    )

    ###### Old IO-Module #####
    # load data as dask objects
    # print(f"Opening {ref_hist_dict[variable]}")
    # ds_obs = xr.open_dataset(ref_hist_dict[variable])
    # ds_obs = xr.open_mfdataset(
    #    ref_hist_dict[variable],
    #    chunks={"time": len(ds_obs.time), "lat": 50, "lon": 50},
    #    parallel=True,
    #    engine="netcdf4",
    # )
    # da_obs = ds_obs[variable].persist()

    # Mdl (historical, 1981 - 2016 for one month and 215 days)  215, 36, 25, 1, 1 ;
    # Preprocess historical mdl-data, create a new time coord, which contain year and day at once and not separate
    # print(f"Opening {mdl_hist_dict[variable]}")
    # if args.forecast_structure == "5D":
    #    ds_mdl = helper_modules.preprocess_mdl_hist(
    #        mdl_hist_dict[variable], month_str, variable
    #    )  # chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 1, 'lon': 1})
    #    da_mdl = ds_mdl.persist()
    # elif args.forecast_structure == "4D":
    #    ds_mdl = xr.open_mfdataset(mdl_hist_dict[variable])
    #    ds_mdl = xr.open_mfdataset(
    #        mdl_hist_dict[variable],
    #        chunks={
    #            "time": len(ds_mdl.time),
    #            "ens": len(ds_mdl.ens),
    #            "lat": 5,
    #            "lon": 5,
    #        },
    #        parallel=True,
    #        engine="netcdf4",
    #    )
    #    da_mdl = ds_mdl[variable].persist()

    # IMPLEMENT ELSE-Statement for logging

    # Pred (current year for one month and 215 days)
    # ds_pred = xr.open_dataset(raw_dict[variable])
    # ds_pred = xr.open_mfdataset(
    #    raw_dict[variable],
    #    chunks={
    #        "time": len(ds_pred.time),
    #        "ens": len(ds_pred.ens),
    #        "lat": 50,
    #        "lon": 50,
    #    },
    #    parallel=True,
    #    engine="netcdf4",
    # )
    # da_pred = ds_pred[variable].persist()

    # IF ABFAGE OB AKTUELLE VORHERSAGE AUS HISTORIE ENTFERNT WERDEN SOLL
    # if args.crossval == True:
    #    da_mdl = da_mdl.sel(time=~da_pred.time)
    #    da_obs = da_obs.sel(time=~da_pred.time)#

    # Change data type of latidude and longitude, otherwise apply_u_func does not work
    # da_pred = da_pred.assign_coords(lon=ds_pred.lon.values.astype(np.float32), lat=ds_pred.lat.values.astype(np.float32))

    # Calculate day of the year from time variable
    # dayofyear_obs = ds_obs['time.dayofyear']
    # dayofyear_mdl = ds_mdl['time.dayofyear']

    # da_temp = xr.DataArray(
    #    None,
    #    dims=["time", "lat", "lon", "ens"],
    #    coords={
    #        "time": (
    #            "time",
    #            coords["time"],
    #            {"standard_name": "time", "long_name": "time"},
    #        ),
    #        "ens": (
    #            "ens",
    #            coords["ens"],
    #            {
    #                "standard_name": "realization",
    #                "long_name": "ensemble_member",
    #            },
    #        ),
    #        "lat": (
    #            "lat",
    #            coords["lat"],
    #            {
    #                "standard_name": "latitude",
    #                "long_name": "latitude",
    #                "units": "degrees_north",
    #            },
    #        ),
    #        "lon": (
    #            "lon",
    #            coords["lon"],
    #            {
    #                "standard_name": "longitude",
    #                "long_name": "longitude",
    #                "units": "degrees_east",
    #             #            },
    #             #        ),
    #             #    },
    #             #).persist()

    #             for timestep in range(0, len(ds_pred.time)):
    #                 # for timestep in range(82, 83):

    #                 print(f"Correcting timestep {timestep}...")
    #                 dayofyear_mdl = ds_mdl["time.dayofyear"]
    #                 day = dayofyear_mdl[timestep]

    #                 # Deal with normal and leap years
    #                 for year in range(syr_calib, eyr_calib + 1):

    #                     ds_obs_year = ds_obs.sel(time=ds_obs.time.dt.year == year)
    #                     ds_mdl_year = ds_mdl.sel(time=ds_mdl.time.dt.year == year)
    #                     dayofyear_obs = ds_obs_year["time.dayofyear"]
    #                     dayofyear_mdl = ds_mdl_year["time.dayofyear"]

    #                     # normal years
    #                     if len(ds_obs_year.time.values) == 365:

    #                         # day_range = (np.arange(day - domain_config['bc_params']['window'], day + domain_config['bc_params']['window'] + 1) + 365) % 365 + 1
    #                         day_range = (
    #                             np.arange(
    #                                 day - domain_config["bc_params"]["window"] - 1,
    #                                 day + domain_config["bc_params"]["window"],
    #                             )
    #                             + 365
    #                         ) % 365 + 1

    #                         # leap years
    #                     else:
    #                         day_range = (
    #                             np.arange(
    #                                 day - domain_config["bc_params"]["window"] - 1,
    #                                 day + domain_config["bc_params"]["window"],
    #                             )
    #                             + 366
    #                         ) % 366 + 1

    #                     intersection_day_obs_year = np.in1d(dayofyear_obs, day_range)
    #                     intersection_day_mdl_year = np.in1d(dayofyear_mdl, day_range)

    #                     if year == syr_calib:
    #                         intersection_day_obs = intersection_day_obs_year
    #                         intersection_day_mdl = intersection_day_mdl_year
    #                     else:
    #                         intersection_day_obs = np.append(
    #                             intersection_day_obs, intersection_day_obs_year
    #                         )
    #                         intersection_day_mdl = np.append(
    #                             intersection_day_mdl, intersection_day_mdl_year
    #                         )

    #                     # print(f'Correcting timestep {timestep}...')

    #                     # day = dayofyear_mdl[timestep]

    #                     # day_range = (np.arange(day - domain_config['bc_params']['window'], day + domain_config['bc_params']['window'] + 1) + 365) % 365 # +1
    #                     # intersection_day_obs = np.in1d(dayofyear_obs, day_range)
    #                     # intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)

    #                 da_obs_sub = da_obs.loc[dict(time=intersection_day_obs)]

    #                 with dask.config.set(
    #                     **{"array.slicing.split_large_chunks": False}
    #                 ):  # --> I really don't know why we need to silence the warning here...
    #                     da_mdl_sub = da_mdl.loc[dict(time=intersection_day_mdl)]

    #                 da_mdl_sub = da_mdl_sub.stack(
    #                     ens_time=("ens", "time"), create_index=True
    #                 )
    #                 da_mdl_sub = da_mdl_sub.drop("time")

    #                 # If the actual year of prediction (e.g. 1981) is within the calibration period (1981 to 2016): cut this year out in both, the historical obs and mdl data
    #                 # da_obs_sub = da_obs_sub.sel(time=~da_obs_sub.time.dt.year.isin(args.year))
    #                 # da_mdl_sub = da_mdl_sub.sel(time=~da_mdl_sub.time.dt.year.isin(args.year))

    #                 da_pred_sub = da_pred.isel(time=timestep)

    #                 da_temp[timestep, :, :] = xr.apply_ufunc(
    #                     bc_module,
    #                     da_pred_sub,
    #                     da_obs_sub,
    #                     da_mdl_sub,
    #                     kwargs={
    #                         "bc_params": domain_config["bc_params"],
    #                         "precip": variable_config[variable]["isprecip"],
    #                     },
    #                     input_core_dims=[["ens"], ["time"], ["ens_time"]],
    #                     output_core_dims=[["ens"]],
    #                     vectorize=True,
    #                     dask="parallelized",
    #                     output_dtypes=[np.float64],
    #                 )

    #             # Change the datatype from "object" to "float64" --> Can we somehow get around this???
    #             da_temp = da_temp.astype("float64")

    #             # Select only the actual variable from the output dataset
    #             ds_out_sel = ds[[variable]]

    #             # Fill this variable with some data...
    #             ds_out_sel[variable].values = da_temp.transpose(
    #                 "time", "ens", "lat", "lon"
    #             ).values

    #             # ...and save everything to disk..
    #             # ds_out_sel.to_netcdf(bcsd_dict[variable], mode='a', format='NETCDF4_CLASSIC', engine='netcdf4', encoding = {variable: encoding[variable]})
    #             ds_out_sel.to_netcdf(
    #                 bcsd_dict[variable],
    #                 mode="a",
    #                 engine="netcdf4",
    #                 encoding={variable: encoding[variable]},
    #             )
    #             # ds_out_sel.close()

    # # client.close()

    # # if cluster is not None:
    # #     cluster.close()
