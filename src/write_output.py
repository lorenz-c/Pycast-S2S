import netCDF4
import os
import numpy as np

def write_output(output):
    # Avoid locking of nc-File

    os.environ['HDF5_USE_FILE_LOCKING'] = 'FALSE'

    # Load in Dataset
    ncid = netCDF4.Dataset('test_py.nc', mode = "a")

    # Transpose queue_out data from shape (51,215) to (215,51)
    output['data_t']= np.transpose(output['data'], (2,0,1))

    # Write Data
    ncid.variables[output['varnme']][output['time_step'],:,:,:] = output['data_t']

    # Close Dataset
    ncid.close()