# import packages
import argparse
import json
import logging
import os
import sys

import dask
from genericpath import exists

import global_processing_modules
import helper_modules


def get_clas():

    parser = argparse.ArgumentParser(
        description="Creation of a new domain for BCSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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


def setup_logger():
    logging.basicConfig(
        filename=f"logs/global_processing.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )


if __name__ == "__main__":

    # Read the command line arguments
    args = get_clas()

    # Create a new logger file (or append to an existing file)
    setup_logger()

    # Period can be in the format "year, year" or "year, month"
    period_list = [int(item) for item in args.period.split(",")]

    year = period_list[0]
    month = period_list[1]
    month_str = str(month).zfill(2)

    with open("conf/global_config.json", "r") as j:
        global_config = json.loads(j.read())

    # if period_list[0] > 1000 and (period_list[1] >= 1 and period_list[1] <= 12):
    #    # [year, month]
    #    syr = period_list[0]
    #    eyr = period_list[0] + 1
    #    smnth = period_list[1]
    #    emnth = period_list[1] + 1
    # elif period_list[0] > 1000 and period_list[1] > 1000:
    #    syr = period_list[0]
    #    eyr = period_list[1] + 1
    #    smnth = 1
    #    emnth = 13
    # else:
    #    logging.error("Period not defined properly")

    # Get some ressourcers
    client, cluster = helper_modules.getCluster(args.node, 1, 35)

    client.get_versions(check=True)

    # Do the memory magic...
    client.amm.start()

    # Write some info about the cluster
    print(f"Dask dashboard available at {client.dashboard_link}")

    if args.mode == 1:

        if global_config["raw_forecasts"]["merged_variables"] == False:

            results = []

            for var in global_config["variables"]:

                results.append(
                    global_processing_modules.gauss_to_regular(
                        global_config, year, month, var
                    )
                )

            dask.compute(results)
            logging.info(
                f"Gauss to regular: Coordination transformation for {year} {month} successful"
            )
