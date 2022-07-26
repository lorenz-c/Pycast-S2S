# import packages
import json
import xarray as xr
import dask
import numpy as np
from dask.distributed import Client, LocalCluster

# import python-files
from apply_bc import apply_bc

import modules
from io_module import slice_and_correct

# f = open('src/bcsd_parameter.json')
f = open('bcsd_parameter.json')
parameter = json.load(f)

# Set month and year of the current forecast
month = parameter["issue_date"]["month"]
year = parameter["issue_date"]["year"]

####### Setup of Dask-Cluster required! ##########

# Convert the domain names in the parameter JSON to an array:
domain_names = [domain_names['name'] for domain_names in parameter["domains"]]

i = 0
#for i in range(0, 1): # domain_names
# Run function




if __name__ == '__main__':

    # Set all the metadata for the output file
    global_attributes, variable_attributes = modules.set_metadata(15, 15)

    # Set all filenames for in- and output files
    obs_dict, mdl_dict, pred_dict, month, bc_out_lns = modules.set_filenames(month, year, domain_names[i], parameter["directories"]["regroot"], parameter["version"])

    # Read the dimensions for the output file (current prediction)
    coords = modules.get_coords_from_files(list(pred_dict.values())[0])

    # Create an empty NetCDF in which we write the BCSD output
    ds = modules.create_4d_netcdf(bc_out_lns, global_attributes, variable_attributes, coords)

    # Initialize a dictionary in order to store outputs of the BCSD
    queue_out = {}
    # Set some important variables (output-name, number of time steps, number of ensemble members)
    queue_out['fnme'] = bc_out_lns
    # queue_out['nts'] = len(coords['time'])
    # queue_out['nens'] = len(coords['ens'])


    # Idea:
    # - inizialize queue_out only once, and filling this dictionary with the BCSD-corrected data and write output only at the end, when the loop over time is finished


    # Loop over each variable
    for v in range(0, 1):
        print('Variable: ' + vars[v])

        # Set variable-name
        queue_out['varnme'] = vars[v]

        ###### Old IO-Module #####
        # load data as dask objects
        # Obs (1981 - 2016 on daily basis)
        ds_obs = xr.open_mfdataset(list(obs_dict.values())[v], chunks={'time': 13149, 'lat': 1, 'lon': 1})
        ds_obs = ds_obs[vars[v]]
        # Mdl (historical, 1981 - 2016 for one month and 215 days)  215, 36, 25, 1, 1 ;
        # Preprocess historical mdl-data, create a new time coord, which contain year and day at once and not separate
        ds_mdl = modules.preprocess_mdl_hist(list(mdl_dict.values())[v]) # chunks={'time': 215, 'year': 36, 'ens': 25, 'lat': 1, 'lon': 1})
        ##### Missing Chunking!!!!
        ds_mdl = ds_mdl[vars[v]]
        # Pred (current year for one month and 215 days)
        ds_pred = xr.open_mfdataset(list(pred_dict.values())[v], chunks={'time': 215, 'ens': 51, 'lat': 1, 'lon': 1})
        ds_pred = ds_pred[vars[v]]
        # Change data type of latidude and longitude, otherwise apply_u_func does not work
        ds_pred = ds_pred.assign_coords(lon=ds_pred.lon.values.astype(np.float32), lat=ds_pred.lat.values.astype(np.float32))

        # Calculate day of the year from time variable
        dayofyear_obs = ds_obs['time.dayofyear']
        dayofyear_mdl = ds_mdl['time.dayofyear']

        # Set important parameter for BCSD
        window_obs = 15
        if vars[v] == "tp":
            dry_thresh = 0.01
            precip = True
            low_extrapol = "delta_additive"
            up_extrapol = "delta_additive"
            extremes = "weibull"
            intermittency = True
        else:
            dry_thresh = 0.01
            precip = False
            low_extrapol = "delta_additive"
            up_extrapol = "delta_additive"
            extremes = "weibull"
            intermittency = False


        # Set Parameter for Dask
        dask_allow = False
        dask_jobs = []

        for k in range(len(coords['time'])):
            print('# Time step ' + str(k + 1) + ' / ' + str(len(coords['time'])))
            if dask_allow == True:
                lst_jobs = dask.delayed(slice_and_correct)(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl, ds_pred, coords, queue_out, window_obs, dry_thresh, precip, low_extrapol, up_extrapol, extremes, intermittency, k)
                dask_jobs.append(lst_jobs)
            else:
                slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl, ds_pred, coords, queue_out, window_obs, dry_thresh, precip, low_extrapol, up_extrapol, extremes, intermittency, k)

    print()

    # queue_out, dask_jobs = io_module(obs_dict, mdl_dict, pred_dict, month, bc_out_lns, 15, 15)


    # cluster = LocalCluster()
    # client = Client()

    # dask.compute(*dask_jobs)


