import json

from cdo import *
cdo = Cdo()

# Import the parameter file
with open('src/bcsd_parameter.json') as json_file:
    parameter = json.load(json_file)

month   = parameter['issue_date']['month']
year    = parameter['issue_date']['year']

# Get the directory of the global data
glbldir = parameter['directories']['glbldir']

# Get the directory of the global "unprocessed" data
glbraw  = glbldir + 'raw' + '/' + format(year, '4') + '/' + format(month, '02') + '/'

#fullgribdir = gribdir + format(year, '4') + '/' + format(month, '02') + '/'
#fullrawdir  = rawdir + format(year, '4') + '/' + format(month, '02') + '/'
#fulloutdir  = outdir + format(year, '4') + '/' + format(month, '02') + '/'

# Set the variables for processing
vars = ['tp', 't2m', 't2min', 't2max', 'ssrd']

# Loop over the variables and change the gridtype from "reduced-gaussian" to "regular"
for var in vars:
    flenme_grb = glbraw + 'ECMWF-seas5_03deg_daily_' + var + '_' + format(year, '4') + format(month, '02') + '.grb'
    flenme_nc  = glbraw + 'ECMWF-seas5_03deg_daily_' + var + '_' + format(year, '4') + format(month, '02') + '.nc'
    
    cdo.setgridtype('regular', input=flenme_grb, output=flenme_nc, options = "-f nc4 -k grid -z zip_6")

# Create a dictionary that holds the global attributes for the NetCDF-Files
#glbl_attrs = {'title': 'ECMWF SEAS5-forecasts', 
#              'Conventions': 'CF-1.8', 
#              'institution': 'European Centre for Medium Range Weather Forecast (ECMWF)', 
#              'source': 'ECMWF SEAS5', 
#              'comment': 'Grib-data has been converted to NetCDF; original data on gaussian grid has been transformed to regular grid; unit of precipitation has been changed from m/day to mm/day; unit of radiation has been changed to W/m^2',
#              'Contact_person': 'Christof Lorenz (Christof.Lorenz@kit.edu)',
#              'Author': 'Christof Lorenz (Christof Lorenz@kit.edu)',
#              'Licence': 'For non-commercial only',
#              'references': 'https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf'}

# Set the handles for the different variables
#tp_flenme = fullrawdir + 'ECMWF-seas5_03deg_daily_' + 'tp' + '_' + format(year, '4') + format(month, '02') + '.nc'
#tp_hndle  = xr.open_dataset(tp_flenme, engine='netcdf4')

#t2m_flenme = fullrawdir + 'ECMWF-seas5_03deg_daily_' + 't2m' + '_' + format(year, '4') + format(month, '02') + '.nc'
#t2m_hndle  = xr.open_dataset(t2m_flenme, engine='netcdf4')

#t2min_flenme = fullrawdir + 'ECMWF-seas5_03deg_daily_' + 't2min' + '_' + format(year, '4') + format(month, '02') + '.nc'
#t2min_hndle  = xr.open_dataset(t2min_flenme, engine='netcdf4')

#t2max_flenme = fullrawdir + 'ECMWF-seas5_03deg_daily_' + 't2max' + '_' + format(year, '4') + format(month, '02') + '.nc'
#t2max_hndle  = xr.open_dataset(t2max_flenme, engine='netcdf4')

#ssrd_flenme = fullrawdir + 'ECMWF-seas5_03deg_daily_' + 'ssrd' + '_' + format(year, '4') + format(month, '02') + '.nc'
#ssrd_hndle  = xr.open_dataset(ssrd_flenme, engine='netcdf4')

# Change the variable names
#if 'var228' in tp_hndle.data_vars:
#    tp_hndle = tp_hndle.rename({'var228': 'tp'})
    
#if 'var55' in t2m_hndle.data_vars:
#    t2m_hndle = t2m_hndle.rename({'var55': 'mean2t24'})
    
#if 'var52' in t2min_hndle.data_vars:
#    t2min_hndle = t2min_hndle.rename({'var52': 'mn2t24'})
    
#if 'var51' in t2max_hndle.data_vars:
#    t2max_hndle = t2max_hndle.rename({'var51': 'mx2t24'})
    
#if 'var169' in ssrd_hndle.data_vars:
#    ssrd_hndle = ssrd_hndle.rename({'var169': 'ssrd'})
