# import packages
from genericpath import exists
import json
import dask
import argparse
import xarray as xr
import regional_processing_modules 
import dir_fnme 
import helper_modules 
import numpy as np

from dask.distributed import Client
# from helper_modules import getCluster

import logging

import os

import sys


def get_clas():
    parser = argparse.ArgumentParser(description="Creation of a new domain for BCSD",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-d", "--domain", action="store", type=str, help="Domain", required=True)
    parser.add_argument("-m", "--mode", action="store", type=str, help="Selected mode for setup", required=True)
    #parser.add_argument("-p", "--period", action="store", type=str, help="Period for which the pre-processing should be executed", required=False)
    parser.add_argument("-Y", "--Years", action="store", type=str, help="Years for which the processing should be executed", required=True)
    parser.add_argument("-M", "--Months", action="store", type=str, help="Months for which the processing should be executed", required=True)
    parser.add_argument("-n", "--node", action="store", type=str, help="Node for running the code", required=False)
    parser.add_argument("-f", "--scheduler_file", action="store", type=str,
                        help="If a scheduler-file is provided, the function does not start its own cluster but rather uses a running environment",
                        required=False)

    return parser.parse_args()


def setup_logger(domain_name):
    logging.basicConfig(filename=f"logs/{domain_name}_setup_domain.log", level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    # encoding='utf-8'


if __name__ == "__main__":

    # Read the command line arguments
    args = get_clas()

    # Create a new logger file (or append to an existing file)
    setup_logger(args.domain)

    # Read the domain configuration from the respective JSON
    with open('conf/domain_config.json', 'r') as j:
        domain_config = json.loads(j.read())

    # Read the global configuration from the respective JSON --> Add this as further input parameter
    with open('conf/attribute_config.json', 'r') as j:
        attribute_config = json.loads(j.read())

    # Read the variable configuration from the respective JSON
    with open('conf/variable_config.json', 'r') as j:
        variable_config = json.loads(j.read())

    try:
        domain_config = domain_config[args.domain]
    except:
        logging.error(f"Init: no configuration for domain {args.domain}")
        sys.exit()

    variable_config = {key: value for key, value in variable_config.items() if key in domain_config['variables']}

    dir_dict = dir_fnme.set_and_make_dirs(domain_config)

    # get filename of grid-File
    fnme_dict = dir_fnme.set_filenames(domain_config)

    grd_fle = regional_processing_modules.create_grd_file(domain_config, dir_dict, fnme_dict)
    
    process_years  = helper_modules.decode_processing_years(args.Years)
    process_months = helper_modules.decode_processing_months(args.Months)

    # Get some ressourcers
    if args.node is not None:
        client, cluster = helper_modules.getCluster(args.node, 1, 35)
    
    if args.scheduler_file is not None:
        client = Client(scheduler_file=args.scheduler_file)

    # local cluster for testing
    # from dask.distributed import Client
    # client = Client(processes = False)

    client.get_versions(check=True)

    # Do the memory magic...
    client.amm.start()

    # Write some info about the cluster
    print(f"Dask dashboard available at {client.dashboard_link}")

    ## Create some code for calculation of history files for period syr_calib to eyr_calib

    if args.mode == 'trunc_frcst':

        for year in process_years:

            results = []

            for month in process_months:
                
                results.append(regional_processing_modules.truncate_forecasts(domain_config, variable_config, dir_dict, year, month))
            try:
                dask.compute(results)
                logging.info(f"Truncate forecasts: Truncation for year {year} successful")
            except:
                logging.warning(f"Truncate forecasts: Something went wrong during truncation for year {year}")
                
                

    elif args.mode == 'remap_frcst':
        
        results = []
        
        for variable in variable_config:

            for year in process_years:

                for month in process_months:

                    results.append(
                    regional_processing_modules.remap_forecasts(domain_config, variable_config, dir_dict, year, month, grd_fle, variable))

        try:
            dask.compute(results)
            logging.info(f"Remap forecasts: Remapping for year {year} successful")

        except:
            logging.warning(f"Remap forecasts: Something went wrong during remapping for year {year}")

    #
    elif args.mode == 'rechunk_frcst':
        
        results = []
        
        for variable in variable_config:

            for year in process_years:
            
                for month in process_months:

                    results.append(regional_processing_modules.rechunk_forecasts(domain_config, variable_config, dir_dict, year, month, variable))

        try:
            dask.compute(results)
            logging.info(f"Rechunk forecasts: Rechunking for year {year} successful")

        except:
            logging.warning(f"Rechunk forecasts: Something went wrong during forecast rechunking for year")


    elif args.mode == 'calib-frcst':
        results = []
        syr_calib = domain_config['syr_calib']
        eyr_calib = domain_config['eyr_calib']
        # print(f"{syr_calib},{eyr_calib}")
        for variable in variable_config:
            for month in process_months:
                month_str = str(month).zfill(2)
                file_list = []
                for year in range(syr_calib, eyr_calib + 1):
                    # Update Filenames
                    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, domain_config['raw_forecasts']["merged_variables"], variable)
                    file_list.append(f"{dir_dict['frcst_high_reg_dir']}/{fnme_dict['frcst_high_reg_dir']}")
                    # file_list.append(f"{dir_dict['frcst_high_reg_lnch_dir']}/{fnme_dict['frcst_high_reg_lnch_dir']}")

                # year = 1981 #dummy
                regional_processing_modules.calib_forecasts(domain_config, variable_config, dir_dict, file_list, month_str, variable)
                # results.append(regional_processing_modules.calib_forecasts(domain_config, variable_config, dir_dict, file_list, month_str, variable))
                # print(results)
        # try:
        #    print("try")
        #     dask.compute(results)
        #    logging.info(f"Calib Forecast: Calibration period successful")

        # except:
        #    logging.warning(f"Calib Forecast:: Something went wrong during calibration period")

    elif args.mode == 'trunc_ref':

        for year in process_years:
            results = []
            fle_list = []
            for variable in variable_config:

                if domain_config['reference_history']['merged_variables'] == True:
                    month_str = "01"  # dummy
                    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, True, variable)
                    fle_list.append(f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_raw_dir']}")
                    fle_string = fle_list
                else:
                    # Update Filenames
                    month_str = "01"  # dummy
                    fnme_dict = dir_fnme.set_filenames(domain_config, year, month_str, False, variable)
                    fle_string = f"{dir_dict['ref_low_glob_dir']}/{fnme_dict['ref_low_glob_dir']}"
                    results.append(
                        regional_processing_modules.truncate_reference(domain_config, variable_config, dir_dict,
                                                                       fnme_dict, fle_string, variable))

            if domain_config['reference_history']['merged_variables'] == True:
                results.append(
                    regional_processing_modules.truncate_reference(domain_config, variable_config, dir_dict, fnme_dict,
                                                                   fle_string, variable))

        try:
            dask.compute(results)
            logging.info(f"Truncate reference: Truncation successful")
        except:
            logging.warning(f"Truncate reference: Something went wrong during truncation for year {year}")



    elif args.mode == 'remap_ref':

        results = []
        
        for year in process_years:
            
            for variable in variable_config:
        
                month = 1 # Dummy... In a future release, we also want to support monthly insead of yearly files
                results.append(regional_processing_modules.remap_reference(domain_config, variable_config, dir_dict, year, month, grd_fle, variable))

        try:
            dask.compute(results)
            logging.info(f"Remap reference: Remap for year {year} successful")
        except:
            logging.warning(f"Remap reference: Something went wrong during truncation for year {year}")


    elif args.mode == 'rechunk_ref':

        results = []
        
        for year in process_years:
            # Update Filenames
            month = 1
            for variable in variable_config:
                results.append(regional_processing_modules.rechunk_reference(domain_config, variable_config, dir_dict, year, month, variable))
            #results.append(regional_processing_modules.rechunk_reference(domain_config, variable_config, dir_dict, year, month))

        try:
            dask.compute(results)
            logging.info(f"Rechunk reference: Rechunk for year {year} successful")
        except:
            logging.warning(f"Rechunk reference: Something went wrong during rechunk for year {year}")
                
                

    elif args.mode == 'calib_ref':
        
        results = []

        syr_calib = domain_config['syr_calib']
        eyr_calib = domain_config['eyr_calib']
        
        for variable in variable_config:

            results.append(regional_processing_modules.calib_reference(domain_config, variable_config, dir_dict, syr_calib, eyr_calib, variable))

        try:
            dask.compute(results)
            logging.info(f"Rechunk reference: Rechunk for successful")
        except:
            logging.warning(f"Rechunk reference: Something went wrong during rechunking")
            

    # Calculate climatology for calibration period
    elif args.mode == 'climatology':

        syr_calib = domain_config['syr_calib']
        eyr_calib = domain_config['eyr_calib']
        # Climatology for SEAS5
        for month in process_months:
            dataset = 'seas5'
            month_str = str(month).zfill(2)
            regional_processing_modules.create_climatology(dataset, domain_config, variable_config, dir_dict, syr_calib,
                                                           eyr_calib, month_str)

        # Climatology for ERA5
        dataset = 'ref'
        month_str = ''
        regional_processing_modules.create_climatology(dataset, domain_config, variable_config, dir_dict, syr_calib,
                                                       eyr_calib, month_str)

    # Create quantiles, terciles, extremes, etc. for later evaluation
    elif args.mode == 'quantiles':
        for month in process_months:
            month_str = str(month).zfill(2)

            regional_processing_modules.calc_quantile_thresh(domain_config, dir_dict, syr, eyr, month_str)

















