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

# cdo = Cdo()


def get_clas():
    parser = argparse.ArgumentParser(
        description="Creation of a new domain for BCSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-d", "--domain", action="store", type=str, help="Domain", required=True
    )
    # parser.add_argument(
    #     "-m",
    #     "--mode",
    #    action="store",
    #    type=str,
    #     help="Selected mode for setup",
    #    required=True,
    #)

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
        filename=f"logs/{domain_name}_day2mon.log",
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
    print(process_years)
    print(process_months)
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


    results = []

    for year in process_years:
        for month in process_months:
            for variable in variable_config:
                # Get BCSD-Filename pp_full
                (raw_full, pp_full, refrcst_full, ref_full,) = helper_modules.set_input_files(domain_config, reg_dir_dict, month, year, variable)

                # set input files
                full_in = pp_full

                # set output files
                fle_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_mon_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
                full_out = f"{reg_dir_dict['monthly_dir']}/{fle_out}"

                # Raw file
                raw_in = reg_dir_dict["processed_dir"]

                # monthly mean by using cdo
                cmd = (
                    "cdo",
                    "-O",
                    "-f",
                    "nc4c",
                    "-z",
                    "zip_6",
                    "monmean",
                    str(full_in),
                    str(full_out),
                )

                results.append(run_cmd(cmd))

    try:
        dask.compute(results)
        logging.info("Day to month: successful")
    except:
        logging.warning("Day to month: Something went wrong")