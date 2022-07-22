# import packages
import json

import dask	
from dask.distributed import Client, LocalCluster

# import python-files
from apply_bc import apply_bc

import modules

from io_module import io_module

f = open('src/bcsd_parameter.json')
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

    # Read the dimensions for the output file
    coords = modules.get_coords_from_files(list(pred_dict.values())[0])

    # Create an empty NetCDF in which we write the output
    ds = modules.create_4d_netcdf(bc_out_lns, global_attributes, variable_attributes, coords)

    queue_out, dask_jobs = io_module(obs_dict, mdl_dict, pred_dict, month, bc_out_lns, 15, 15)


    cluster = LocalCluster()
    client = Client()

    dask.compute(*dask_jobs)





print()
