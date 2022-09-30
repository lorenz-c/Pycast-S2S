import json
import xarray as xr
import dask
import numpy as np
from dask.distributed import Client, LocalCluster
import importlib

import modules
import setup_domain_func


# Get some ressourcers
#client, cluster = modules.getCluster('fat', 1, 40)

cluster = LocalCluster()
client  = Client(cluster)

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

domain_config = domain_config['germany']

variable_config = { key:value for key,value in variable_config.items() if key in domain_config['variables']}

dir_dict = setup_domain_func.set_and_make_dirs(domain_config)

grd_fle = setup_domain_func.create_grd_file(domain_config, dir_dict)

setup_domain_func.prepare_forecasts(domain_config, variable_config, dir_dict)