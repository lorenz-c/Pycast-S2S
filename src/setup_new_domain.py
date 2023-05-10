# import packages
import argparse
import json
import logging
import os
import sys

import dask
import helper_modules as modules
from genericpath import exists

import regional_processing_modules


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
        "-p",
        "--period",
        action="store",
        type=str,
        help="Period for which the pre-processing should be executed",
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
        "-f",
        "--scheduler_file",
        action="store",
        type=str,
        help="If a scheduler-file is provided, the function does not start its own cluster but rather uses a running environment",
        required=False,
    )

    return parser.parse_args()


def setup_logger(domain_name):
    logging.basicConfig(
        filename=f"logs/{domain_name}_setup_domain.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )


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

    variable_config = {
        key: value
        for key, value in variable_config.items()
        if key in domain_config["variables"]
    }

    dir_dict = regional_processing_modules.set_and_make_dirs(domain_config)

    grd_fle = regional_processing_modules.create_grd_file(domain_config, dir_dict)

    if args.period is not None:
        # Period can be in the format "year, year" or "year, month"
        period_list = [int(item) for item in args.period.split(",")]

        if period_list[0] > 1000 and (period_list[1] >= 1 and period_list[1] <= 12):
            # [year, month]
            syr = period_list[0]
            eyr = period_list[0] + 1
            smnth = period_list[1]
            emnth = period_list[1] + 1
        elif period_list[0] > 1000 and period_list[1] > 1000:
            syr = period_list[0]
            eyr = period_list[1] + 1
            smnth = 1
            emnth = 13
        else:
            logging.error("Period not defined properly")
    else:
        syr = domain_config["syr_calib"]
        eyr = domain_config["eyr_calib"]
        smnth = 1
        emnth = 13

    # Get some ressourcers
    client, cluster = modules.getCluster(args.node, 1, 35)

    client.get_versions(check=True)

    # Do the memory magic...
    client.amm.start()

    # Write some info about the cluster
    print(f"Dask dashboard available at {client.dashboard_link}")

    if args.mode == "trunc_frcst":

        for year in range(syr, eyr + 1):

            results = []

            for month in range(smnth, emnth):

                month_str = str(month).zfill(2)

                results.append(
                    regional_processing_modules.truncate_forecasts(
                        domain_config, variable_config, dir_dict, year, month_str
                    )
                )

            try:
                dask.compute(results)
                logging.info(
                    f"Truncate forecasts: Truncation for year {year} successful"
                )
            except:
                logging.warning(
                    f"Truncate forecasts: Something went wrong during truncation for year {year}"
                )

    elif args.mode == "remap_frcst":

        for year in range(syr, eyr + 1):

            results = []

            for month in range(smnth, emnth):

                month_str = str(month).zfill(2)

                results.append(
                    regional_processing_modules.remap_forecasts(
                        domain_config, dir_dict, year, month_str, grd_fle
                    )
                )

            # try:
            dask.compute(results)
            logging.info(f"Remap forecasts: Remapping for year {year} successful")

            # except:
            #    logging.warning(f"Remap forecasts: Something went wrong during remapping for year {year}")

    elif args.mode == "rechunk_frcst":

        for month in range(smnth, emnth):

            month_str = str(month).zfill(2)

            regional_processing_modules.rechunk_forecasts(
                domain_config, variable_config, dir_dict, syr, eyr, month_str
            )

    elif args.mode == "trunc_ref":

        regional_processing_modules.truncate_reference(
            domain_config, variable_config, dir_dict, syr, eyr
        )

    elif args.mode == "remap_ref":

        regional_processing_modules.remap_reference(
            domain_config, variable_config, dir_dict, syr, eyr, grd_fle
        )

    elif args.mode == "rechunk_ref":

        regional_processing_modules.rechunk_reference(
            domain_config, variable_config, dir_dict, syr, eyr
        )

    elif args.mode == "climatology":

        regional_processing_modules.create_climatology(
            domain_config, variable_config, dir_dict, syr, eyr
        )
