# import packages
import json
import dask
import argparse

import modules
import setup_domain_func

import logging

import os


def get_clas():
    
    parser = argparse.ArgumentParser(description="Creation of a new domain for BCSD", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-d", "--domain", action="store", type=str, help="Domain", required=True)
    parser.add_argument("-m", "--mode", action="store", type=str, help="Selected mode for setup", required=True)
    
    return parser.parse_args()

def setup_logger(domain_name):
    logging.basicConfig(filename=f"logs/{domain_name}_setup_domain.log", encoding='utf-8', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

if __name__ == "__main__":
    
    args = get_clas()
    
    print(args.mode)
    
    setup_logger(args.domain)
    
    # Get some ressourcers
    client, cluster = modules.getCluster('haswell', 1, 35)
    
    # Do the memory magic...
    client.amm.start() 
    
    # Write some info about the cluster
    print(client.dashboard_link)


    # Read the domain configuration from the respective JSON
    with open('conf/domain_config.json', 'r') as j:
        domain_config = json.loads(j.read())

    # Read the global configuration from the respective JSON --> Add this as further input parameter
    with open('conf/global_config.json', 'r') as j:
        global_config = json.loads(j.read())

    # Read the variable configuration from the respective JSON
    with open('conf/variable_config.json', 'r') as j:
        variable_config = json.loads(j.read())
    
    
    domain_config = domain_config[args.domain]

    variable_config = { key:value for key,value in variable_config.items() if key in domain_config['variables']}

    dir_dict = setup_domain_func.set_and_make_dirs(domain_config)

    grd_fle = setup_domain_func.create_grd_file(domain_config, dir_dict)

    syr_calib = domain_config["syr_calib"]
    eyr_calib = domain_config["eyr_calib"]
    
    syr_calib = 1981
    eyr_calib = 1982
    
    if args.mode == 'trunc_frcst':
    
        for year in range(syr_calib, eyr_calib + 1):
            
            results = []
            
            for month in range(1, 13):
                
                month_str = str(month).zfill(2)
                
                results.append(setup_domain_func.prepare_forecast_dask(domain_config, variable_config, dir_dict, year, month_str))
    
            try:
                dask.compute(results)
                logging.info(f"Truncate forecasts: Truncation for year {year} successful")
            except:
                logging.warning(f"Truncate forecasts: Something went wrong during truncation for year {year}")
              
    elif args.mode == 'remap_frcst':
        
        for year in range(syr_calib, eyr_calib + 1):
            
            results = []
            
            for month in range(1, 13):
                
                month_str = str(month).zfill(2)
                
                #setup_domain_func.remap_forecasts(domain_config, dir_dict, year, month_str, grd_fle)
                
                results.append(setup_domain_func.remap_forecasts(domain_config, dir_dict, year, month_str, grd_fle))
                
            #try:
            dask.persist(results)
            logging.info(f"Remap forecasts: Remapping for year {year} successful")
            #except:
            #    logging.warning(f"Remap forecasts: Something went wrong during remapping for year {year}")
        
                
            
                
    
                
            
            
        
        
            