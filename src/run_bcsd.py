# import packages
import json
import xarray as xr
import dask
import numpy as np
from dask.distributed import Client, LocalCluster
import argparse

import modules


def get_clas():
    
    parser = argparse.ArgumentParser(description="Python-based BCSD",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-y", "--year", action="store_true", help="Year of the actual forecast")
    parser.add_argument("-m", "--month", action="store_true", help="Month of the actual forecast")
    parser.add_argument("-d", "--domain", action="store_true", help="Domain")
    parser.add_argument("-p", "--parameter_file", help="Parameter file")
    
    

if __name__ == '__main__':
    

    
    
    f = open('bcsd_parameter.json')
    parameter = json.load(f)
    
    # Set month and year of the current forecast
    month = parameter["issue_date"]["month"]
    year = parameter["issue_date"]["year"]
    
    # Convert the domain names in the parameter JSON to an array:
    domain_names = [domain_names['name'] for domain_names in parameter["domains"]]
    
    # Do only the Khuzestan-run
    i = 1
    
    # Get some ressourcers
    client, cluster = getCluster('rome', 1, 30)
    
    # Do the memory magic...
    client.amm.start() 
    
    # Write some info about the cluster
    print(client.scheduler_info)
    
    # Set all the metadata for the output file
    global_attributes, variable_attributes = modules.set_metadata(15, 15)

    # Set all filenames for in- and output files
    obs_dict, mdl_dict, pred_dict, month, bc_out_lns = modules.set_filenames(month, year, domain_names[i], parameter["directories"]["regroot"], parameter["version"])

    # Read the dimensions for the output file (current prediction)
    coords = modules.get_coords_from_files(list(pred_dict.values())[0])

    # Create an empty NetCDF in which we write the BCSD output
    ds = modules.create_4d_netcdf(bc_out_lns, global_attributes, variable_attributes, coords)
    
    # Load the NetCDF to get a handle for the output
    ds_out = xr.open_dataset(bc_out_lns)
    


    # Loop over each variable
    for variable in variable_attributes:
     

        ###### Old IO-Module #####
        # load data as dask objects
        # Obs (1981 - 2016 on daily basis)
        ds_obs = xr.open_mfdataset(list(obs_dict.values())[variable], chunks={'time': 13149, 'lat': 1, 'lon': 1})
        ds_obs = ds_obs[vars[variable]]
        
        # Mdl (historical, 1981 - 2016 for one month and 215 days)  215, 36, 25, 1, 1 ;
        # Preprocess historical mdl-data, create a new time coord, which contain year and day at once and not separate
        ds_mdl = modules.preprocess_mdl_hist(list(mdl_dict.values())[variable], month) # chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 1, 'lon': 1})
        ds_mdl = ds_mdl[vars[variable]]
        
        # Pred (current year for one month and 215 days)
        ds_pred = xr.open_mfdataset(list(pred_dict.values())[variable], chunks={'time': 215, 'ens': 51, 'lat': 1, 'lon': 1})
        ds_pred = ds_pred[vars[variable]]
        
        # Change data type of latidude and longitude, otherwise apply_u_func does not work
        ds_pred = ds_pred.assign_coords(lon=ds_pred.lon.values.astype(np.float32), lat=ds_pred.lat.values.astype(np.float32))

        # Calculate day of the year from time variable
        dayofyear_obs = ds_obs['time.dayofyear']
        dayofyear_mdl = ds_mdl['time.dayofyear']

        # Set important parameter for BCSD
        
        
        window_obs = 15
        
        if vars[v] == "tp":
            bc_params = {
                'dry_thresh': 0.01,
                'precip': True,
                'low_extrapol': "delta_additive",
                'up_extrapol': "delta_additive",
                'extremes': "weibull",
                'intermittency': True,
                'nquants': 2500
            }
        else:
            bc_params = {
                'dry_thresh': 0.01,
                'precip': False,
                'low_extrapol': "delta_additive",
                'up_extrapol': "delta_additive",
                'extremes': "weibull",
                'intermittency': True,
                'nquants': 2500
            }
            
            
            
            
        for timestep in range(0, len(ds_pred.time)):
    
        
            day = dayofyear_mdl[timestep]
    
            day_range = (np.arange(day - window_obs, day + window_obs + 1) + 365) % 365 + 1
    
            intersection_day_obs = np.in1d(dayofyear_obs, day_range)
            intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)
    
            ds_obs_sub = ds_obs.loc[dict(time=intersection_day_obs)]
    
            ds_mdl_sub = ds_mdl.loc[dict(time=intersection_day_mdl)]
        
            ds_mdl_sub = ds_mdl_sub.stack(ens_time=("ens", "time"), create_index=True)
            ds_mdl_sub = ds_mdl_sub.drop('time')
    
    
            ds_pred_sub = ds_pred.isel(time=timestep)
    
    
            pred_corr_act = xr.apply_ufunc(
                bc_module, 
                ds_pred_sub, 
                ds_obs_sub, 
                ds_mdl_sub, 
                kwargs={'bc_params': bc_params},
                input_core_dims=[["ens"], ["time"], ['ens_time']], 
                output_core_dims=[["ens"]], 
                vectorize=True, 
                dask="parallelized", 
                output_dtypes=[np.float64]) 
    
    
            ds_out[variable].loc[dict(time=ds_pred.time.values[timestep])] = pred_corr_act[v].transpose('ens', 'lat', 'lon')
