# import packages
import argparse
import json
import logging

from cdo import *
from dask.distributed import Client

import helper_modules

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
        help="Period for which the BCSD should be executed",
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
        "-f",
        "--scheduler_file",
        action="store",
        type=str,
        help="""If a scheduler-file is provided, the function does not start its own cluster 
            but rather uses a running environment""",
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
        filename=f"logs/{domain_name}_run_day2mon.log",
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
            client, cluster = helper_modules.getCluster(args.node, 1, args.processes)
        else:
            logging.error(
                "Run day2mon: If node is provided, you must also set number of processes"
            )
    else:
        logging.error(
            "Run day2mon: Must either provide a scheduler file or node and number of processes."
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

    for year in range(syr, eyr + 1):
        for month in range(smnth, emnth + 1):
            # get filenames
            (
                raw_dict,
                bcsd_dict,
                ref_hist_dict,
                mdl_hist_dict,
            ) = helper_modules.set_filenames(
                year, month, domain_config, variable_config, False
            )

            # set input and output filenames
            flnm_in = bcsd_dict
            flnm_out = f"pd/data/regclim_data/gridded_data/processed/west_africa/{year}_{month}_test.nc"

            # monthly mean by using cdo
            cmd = (
                "cdo",
                "-O",
                "-f",
                "nc4c",
                "-z",
                "zip_6",
                "monmean",
                str(flnm_in),
                str(flnm_out),
            )
