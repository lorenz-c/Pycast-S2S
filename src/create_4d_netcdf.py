# import packages
import os
from os.path import exists
import netCDF4
import numpy as np

def create_4d_netcdf(fnme, dtainfo, varnme, varstandard, varlong, varunits, varprec, varfill, var_scale, var_offset, times, lat, lon, time_units, lev_name, lev_long, lev_standard, lev_units, lev_vals, lev_size, chnks, overwrite):
    # This function creates a NetCDF-file for 3D-Variables (time, lon, lat).
    # The user must provide several input parameters so that the NetCDF-file
    # can be set up correctly: 

    # dtainfo: structure-array with the following elements: 
    # - dtainfo.title       --> The title of the dataset
    # - dtainfo.source      --> How was the data created (model, observation, 
    #                           etc.)
    # - dtainfo.institution --> Where was the data created
    # - dtainfo.references  --> Publications about the data
    # - dtainfo.history     --> Each NetCDF-file should contain the global
    #                               attribute "history", where different
    #                               post-processing steps can be documented
    # - dtainfo.comment     --> Additional information about the data
    # - dtainfo.Conventions --> If the file fullfills the CF-Conventions
    #                           (http://cfconventions.org/), the user should
    #                           add the version of the conventions-document
    #                           (e.g. CF-1.7)
    #
    # varnme:  cell array, which contains the names of the variables (e.g. 
    #  varnme = {'prec'); varnme = {'t2min', 't2max'};
    # varstandard (optional): cell array, which contains the standard name of 
    #  the variables. This standard name should be consistent with e.g. the CF
    #  Standard Name Table (http://cfconventions.org/standard-names.html)
    # varlong (optional): cell-array which contains the long names of the 
    #  variables 
    # varunits (optional): cell-array which contains the units of the variables 
    #  (e.g. varunits = {'mm/day'}
    # varprec (optional): Cell-arraay, which contains the precision of the
    # variables. Valid values are e.g. 'NC_DOUBLE', 'NC_FLOAT', 'NC_INT',
    #  'NC_SHORT', etc. 
    ###########. ---> fit to PYHTON
    # varfill (optional): Cell-array which contains the fill-value (i.e. the
    #  identifier for missing values)
    # var_scale (optional): Cell-array which contains the scaling-factor for
    #  the variable. This is useful if e.g. float data should be stored as
    #  integers for saving memory without loosing all the digits.
    # var_offset (optional): Cell-array which contains the additive offset for
    #   the variable. Similar to the scaling-factor, this parameter can be used
    #   as simple data compression to store low-resolution floating-point data
    #   as small integers.
    # When the data is written to the newly created file, Matlab automatically 
    # applies the scaling- and offset-factors to the data: 
    #           Varout = (varin - add_offset)/scale_factor

    # times: vector which contains the time-values
    # time_units: string which contains the unit of the time-vector (e.g. 'days
    #  since 1950-01-01 00:00:00')

    # lat, lon: vector with latitudes and longitudes

    # chnks (optional): vector which defines the chunking of the data. This
    #  highly depends on the application. For maps, chnks is usually set to
    #  chnks = [length(lon) length(lat) 1]. However, this can be a very bad
    #  choice if data at single locations is of interest...

    # overwrite: boolean

    # if nargin < 16
    #     overwrite = true;
    # end

    # Get the global attributes
    glbl_Atts = dtainfo.keys()

    ntimes = len(times)

    # Make sure that lon and lat are column-vectors
    # if size(lat, 2) > 1, lat = lat(:, 1); end   
    # if size(lon, 2) > 1, lon = lon(1, :)'; end

    nlat   = len(lat)
    nlon   = len(lon)

    # Get the ID of the global attributes
    # glob_id = netcdf.getConstant('NC_GLOBAL');

    # Set the parameters for NETCDF4-classic 
    # cmode   = netcdf.getConstant('NETCDF4');
    # cmode   = bitor(cmode,netcdf.getConstant('CLASSIC_MODEL'));

    if overwrite == 1:
        # import os
        # from os.path import exists
        if exists(fnme):
            os.remove(fnme)


    # Create a new NetCDF-file (Version 4, classic)
    #ncid    = netcdf.create(fnme, cmode);
    ncid     = netCDF4.Dataset(fnme, 'w')
    
    # Write the global attributes
    for name, value in dtainfo.items():
        setattr(ncid, name, value)

    tim_dim_id = ncid.createDimension('time', ntimes)
    lat_dim_id = ncid.createDimension('lat', nlat)
    lon_dim_id = ncid.createDimension('lon', nlon)
    lev_dim_id = ncid.createDimension(lev_name, lev_size)
    
    time_id     = ncid.createVariable('time', np.float32, ('time',))
    lat_id      = ncid.createVariable('lat', np.float32, ('lat',))
    lon_id      = ncid.createVariable('lon', np.float32, ('lon',))
    lev_id      = ncid.createVariable(lev_name, np.float32, (lev_name,))

    time_id.units = time_units 
    lat_id.units = "degrees_north"
    lon_id.units = "degrees_east"

    lat_id.standard_name = "latitude"
    lon_id.standard_name = "longitude"
    time_id.standard_name = "time"
    time_id.calendar = "proleptic_gregorian"


    if lev_units != "":
        lev_id.units = lev_units

    if lev_standard != "":
        lev_id.standard_name = lev_standard

    if lev_long != "":
        lev_id.long_name = lev_long

    var_dims  = ['time', lev_name, 'lat', 'lon']

    for i in range(0,len(varnme)):


        if np.any(varfill[i]):
            if np.any(chnks):
                var_id      = ncid.createVariable(varnme[i], varprec[i], (var_dims[0], var_dims[1], var_dims[2], var_dims[3]), zlib = True, complevel = 6, fill_value = varfill[i], chunksizes = chnks)
            else:
                var_id      = ncid.createVariable(varnme[i], varprec[i], (var_dims[0], var_dims[1], var_dims[2], var_dims[3]), fill_value = varfill[i])
        else:
            var_id      = ncid.createVariable(varnme[i], varprec[i], (var_dims[0], var_dims[1], var_dims[2], var_dims[3]))

        if varstandard[i] != "":
            var_id.standard_name = varstandard[i]

        if varlong[i] != "":
            var_id.long_name = varlong[i]

        if varunits[i] != "":
            var_id.units = varunits[i]

        if np.any(var_scale[i]):
            var_id.scale_factor = var_scale[i]

        if np.any(var_offset[i]):
            var_id.add_offset = var_offset[i]

    time_id[:] = times.astype("float32") #equivalent to single() in Matlab:https://de.mathworks.com/help/matlab/matlab_external/passing-data-to-python.html
    lev_id[:] = lev_vals.astype("float32")
    lat_id[:] = lat.astype("float32")
    lon_id[:] = lon.astype("float32")

    ncid.close()

