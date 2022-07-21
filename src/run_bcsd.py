# import packages
import json

import dask	
from dask.distributed import Client, LocalCluster

# import python-files
from apply_bc import apply_bc

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
obs_struct, mdl_struct, pred_struct, queue_out, dask_jobs = apply_bc(month, year, domain_names[i], parameter["directories"]["regroot"], parameter["version"])


if __name__ == '__main__':


    cluster = LocalCluster()
    client = Client()

    dask.compute(*dask_jobs)





print()
