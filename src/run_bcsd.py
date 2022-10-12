# import packages
import json
from unittest.util import strclass
import xarray as xr
import numpy as np
from dask.distributed import Client
import dask
import argparse

import logging

import modules



def get_clas():
    
    parser = argparse.ArgumentParser(description="Python-based BCSD", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-d", "--domain", action="store", type=str, help="Domain", required=True)
    parser.add_argument("-y", "--year", action="store", type=int, help="Year of the actual forecast", required=True)
    parser.add_argument("-m", "--month", action="store", type=int, help="Month of the actual forecast", required=True)
    parser.add_argument("-s", "--forecast_structure", action="store", type=str, help="Structure of the line-chunked forecasts (can be 5D or 4D)", required=True)
    parser.add_argument("-f", "--scheduler_file", action="store", type=str, help="If a scheduler-file is provided, the function does not start its own cluster but rather uses a running environment", required=False)
    parser.add_argument("-n", "--node", action="store", type=str, help="Node for running the code", required=False)
    parser.add_argument("-p", "--processes", action="store", type=int, help="Node for running the code", required=False)
    
    return parser.parse_args()
    
def setup_logger(domain_name):
    logging.basicConfig(filename=f"logs/{domain_name}_run_bcsd.log", encoding='utf-8', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')   

if __name__ == '__main__':
    
    args = get_clas()
    
    if args.scheduler_file is not None:
        client=Client(scheduler_file=args.scheduler_file)
    elif args.node is not None:
        if args.processes is not None:
            client, cluster = modules.getCluster(args.node, 1,  args.processes)
        else:
            logging.error('Run BCSD: If node is provided, you must also set number of processes')
    else:
        logging.error('Run BCSD: Must either provide a scheduler file or node and number of processes.')
        
    # Make sure that all workers have consistent library versions
    client.get_versions(check=True)
    
    # Do the memory magic...  
    client.amm.start() 
    
    # It seems as if we need to manually make the two modules available to the workers...
    #client.upload_file('modules.py') 
    client.upload_file('bc_module_v2.py')
    
    import modules
    from bc_module_v2 import bc_module

    # Write some info about the cluster
    print(f"Dask Dashboard available at {client.dashboard_link}")
    
    # Read the domain configuration from the respective JSON
    with open('conf/domain_config.json', 'r') as j:
        domain_config = json.loads(j.read())

    # Read the global configuration from the respective JSON --> Add this as further input parameter
    with open('conf/global_config.json', 'r') as j:
        global_config = json.loads(j.read())

    # Read the variable configuration from the respective JSON
    with open('conf/variable_config.json', 'r') as j:
        variable_config = json.loads(j.read())

    # Select the configuration for the actual domain --> We want to do that with the argument parser..
    domain_config = domain_config[args.domain]

    # Get only the variables that are needed for the current domain
    variable_config = { key:value for key,value in variable_config.items() if key in domain_config['variables']}

    # Set all filenames, etc.
    if args.domain == 'germany':
        raw_dict, bcsd_dict, ref_hist_dict, mdl_hist_dict = modules.set_filenames(args.year, args.month, domain_config, variable_config, False)
    else:
        raw_dict, bcsd_dict, ref_hist_dict, mdl_hist_dict = modules.set_filenames(args.year, args.month, domain_config, variable_config, True)
        


    # IMPLEMENT A CHECK IF ALL INPUT FILES ARE AVAILABL!!!!!!
    #
    #
    #
    #



    # Read the dimensions for the output file (current prediction)
    coords = modules.get_coords_from_files(list(raw_dict.values())[0])
    
    global_config = modules.update_global_attributes(global_config, domain_config['bc_params'], coords, args.domain)

    encoding = modules.set_encoding(variable_config, coords)
    
    # Create an empty NetCDF in which we write the BCSD output
    ds = modules.create_4d_netcdf(bcsd_dict, global_config, domain_config, variable_config, coords)
    
    # Loop over each variable
    for variable in variable_config:
     
        ###### Old IO-Module #####
        # load data as dask objects
        print(f"Opening {ref_hist_dict[variable]}")
        ds_obs = xr.open_dataset(ref_hist_dict[variable])
        ds_obs = xr.open_mfdataset(ref_hist_dict[variable], chunks={'time': len(ds_obs.time), 'lat': 50, 'lon': 50}, parallel=True, engine='netcdf4')
        da_obs = ds_obs[variable].persist()
        
        # Mdl (historical, 1981 - 2016 for one month and 215 days)  215, 36, 25, 1, 1 ;
        # Preprocess historical mdl-data, create a new time coord, which contain year and day at once and not separate
        print(f"Opening {mdl_hist_dict[variable]}")
        if args.forecast_structure == '5D':
            ds_mdl = modules.preprocess_mdl_hist(mdl_hist_dict[variable], args.month, variable) # chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 1, 'lon': 1})
            da_mdl = ds_mdl.persist()
        elif args.forecast_structure == '4D':
            ds_mdl = xr.open_mfdataset(mdl_hist_dict[variable])
            ds_mdl = xr.open_mfdataset(mdl_hist_dict[variable], chunks={'time': len(ds_mdl.time), 'ens': len(ds_mdl.ens), 'lat': 5, 'lon': 5}, parallel=True, engine='netcdf4')
            da_mdl = ds_mdl[variable].persist()
            
            
            
        # IMPLEMENT ELSE-Statement for logging
       
        
        # Pred (current year for one month and 215 days)
        ds_pred = xr.open_dataset(raw_dict[variable])
        ds_pred = xr.open_mfdataset(raw_dict[variable], chunks={'time': len(ds_pred.time), 'ens': len(ds_pred.ens), 'lat': 50, 'lon': 50}, parallel=True, engine='netcdf4')
        da_pred = ds_pred[variable].persist()
        
        # Change data type of latidude and longitude, otherwise apply_u_func does not work
        #da_pred = da_pred.assign_coords(lon=ds_pred.lon.values.astype(np.float32), lat=ds_pred.lat.values.astype(np.float32))

        # Calculate day of the year from time variable
        dayofyear_obs = ds_obs['time.dayofyear']
        dayofyear_mdl = ds_mdl['time.dayofyear']

    
        da_temp = xr.DataArray(
            None, 
            dims = ['time', 'lat', 'lon', 'ens'], 
            coords = {
                'time': ('time', coords['time'], {'standard_name': 'time', 'long_name': 'time'}),
                'ens': ('ens', coords['ens'], {'standard_name': 'realization', 'long_name': 'ensemble_member'}),
                'lat': ('lat', coords['lat'], {'standard_name': 'latitude', 'long_name': 'latitude', 'units': 'degrees_north'}),
                'lon': ('lon', coords['lon'], {'standard_name': 'longitude', 'long_name': 'longitude', 'units': 'degrees_east'})
            }
        ).persist()
            
        for timestep in range(0, len(ds_pred.time)):
            
            print(f'Correcting timestep {timestep}...')

            day = dayofyear_mdl[timestep]
    
            day_range = (np.arange(day - domain_config['bc_params']['window'], day + domain_config['bc_params']['window'] + 1) + 365) % 365 + 1
            intersection_day_obs = np.in1d(dayofyear_obs, day_range)
            intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)
            
            da_obs_sub = da_obs.loc[dict(time=intersection_day_obs)]
            with dask.config.set(**{'array.slicing.split_large_chunks': False}): # --> I really don't know why we need to silence the warning here...
                da_mdl_sub = da_mdl.loc[dict(time=intersection_day_mdl)]
                
            da_mdl_sub = da_mdl_sub.stack(ens_time=("ens", "time"), create_index=True)
            da_mdl_sub = da_mdl_sub.drop('time')

            da_pred_sub = da_pred.isel(time=timestep)

            da_temp[timestep, :, :] = xr.apply_ufunc(
                bc_module, 
                da_pred_sub, 
                da_obs_sub, 
                da_mdl_sub, 
                kwargs={'bc_params': domain_config['bc_params'], 'precip': variable_config[variable]['isprecip']},
                input_core_dims=[["ens"], ["time"], ['ens_time']], 
                output_core_dims=[["ens"]], 
                vectorize=True, 
                dask="parallelized", 
                output_dtypes=[np.float64]) 
        
            #da_temp.loc[dict(time=ds_pred.time.values[timestep])] = pred_corr_act
            #da_temp[timestep, :, :] = pred_corr_act
    
        # Change the datatype from "object" to "float64" --> Can we somehow get around this???
        da_temp = da_temp.astype('float64')
        
        # Select only the actual variable from the output dataset
        ds_out_sel = ds[[variable]]
        
        # Fill this variable with some data...
        ds_out_sel[variable].values = da_temp.transpose('time', 'ens', 'lat', 'lon').values
        
        # ...and save everything to disk..
        ds_out_sel.to_netcdf(bcsd_dict[variable], mode='a', format='NETCDF4_CLASSIC', engine='netcdf4', encoding = {variable: encoding[variable]})

    client.close()
    
    if args.scheduler_file is not None:
        cluster.close()


