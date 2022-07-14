import xarray as xr
import numpy as np
import pandas as pd
import json
import os

from cdo import *
cdo = Cdo()


# Import the parameter file
with open('src/bcsd_parameter.json') as json_file:
    parameter = json.load(json_file)


version = parameter['version']
month   = parameter['issue_date']['month']
year    = parameter['issue_date']['year']


# Get the directory of the global data
glbldir = parameter['directories']['glbldir']
# ...daily
dailydir   = glbldir + 'daily/'
# ...and monthly...
monthlydir = glbldir + 'monthly' + '/' + format(year, '4') + '/' + format(month, '02')

# Create (if necessary) the output directory for the global data
if not os.path.exists(monthlydir):
    os.makedirs(monthlydir)

# Set the directory of the regional data
regroot = parameter['directories']['regroot']

# Count the number of domains
ndoms = len(parameter['domains'])

# Compute the monthly averages from the global data
if parameter['process_params']['toglblmonthly'] == True:
    
    for ens in range (0, 51):
        flenme_daily   = glbldir + 'daily' + '/' + format(year, '4') + '/' + format(month, '02') + '/' + 'ECMWF_SEAS5_' + format(ens, '02') + '_' + format(year, '04') + format(month, '02') + '.nc' 
        flenme_monthly = glbldir + 'monthly' + '/' + format(year, '4') + '/' + format(month, '02') + '/' + 'ECMWF_SEAS5_' + format(ens, '02') + '_' + format(year, '04') + format(month, '02') + '_monthly.nc' 

        cdo.monmean(input=flenme_daily, output=flenme_monthly, options='-f nc4c -k grid -z zip_6') 

# Compute the monthly averages from the regional data
if parameter['process_params']['toregmonthly'] == True:
    for i in range(0, ndoms):

        domain_name = parameter['domains'][i]['name']

        flenme_dailyreg_grd   = regroot + domain_name + '/' + 'daily' + '/' + 'seas5_bcsd' + '/' + 'SEAS5_BCSD_v' + version + '_daily_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '.nc'
        flenme_monthlyreg_grd = regroot + domain_name + '/' + 'monthly' + '/' + 'seas5_bcsd' + '/' + 'SEAS5_BCSD_v' + version + '_monthly_' + format(year, '04') + format(month, '02') + '_0.1_' + domain_name + '.nc'

        cdo.monmean(input=flenme_dailyreg_grd, output=flenme_monthlyreg_grd, options='-f nc4c -k grid -z zip_6')

if parameter['process_params']['toregtsmonthly'] == True:

    for i in range(0, ndoms):

        domain_name = parameter['domains'][i]['name']
    
        flenme_dailyreg_ts   = regroot + domain_name + '/' + 'daily' + '/' + 'seas5_bcsd_timeseries' + '/' + 'SEAS5_BCSD_v' + version + '_daily_' + format(year, '04') + format(month, '02') + '_TS_' + domain_name + '.nc'
        flenme_monthlyreg_ts = regroot + domain_name + '/' + 'monthly' + '/' + 'seas5_bcsd_timeseries' + '/' + 'SEAS5_BCSD_v' + version + '_monthly_' + format(year, '04') + format(month, '02') + '_TS_' + domain_name + '.nc'

        cdo.monmean(input=flenme_dailyreg_ts, output=flenme_monthlyreg_ts, options='-f nc4c -k grid -z zip_6')
