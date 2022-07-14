function TS_out = nc_regionaverage_shape_oper(fle_in, fle_out, shapefile)
% The function computes time-series of area-weighted means over selected 
% areas. These areas are defined by an id_map (a matrix where connected
% regions have the same id) and an area_id vector (or scalar), which 
% contains all the areas over which the input fields should be
% aggregated. In contrast to the multiregionaverage.m-function, this
% function accepts a list of NetCDF-files as inputs. From each NetCDF-file,
% the function simultaneously loads a number of [blocks]-maps, which are
% then processed. 

% Load latitude and longitude from the input files
lat = ncread(fle_in, 'lat');
lon = ncread(fle_in, 'lon');

% Create a mask for each of the regions in the shapefile
[LAT, LON] = meshgrid(lat, lon);

S = shaperead(shapefile);

A = area_wghts(lat, lon, 'vec', 'regular', 6378137, false);
A = repmat(A', [length(lon), 1]);

for i = 1:length(S)
    mask{i, 1} = zeros(size(LON));
    IN                  = inpolygon(LAT, LON, S(i).Y, S(i).X);
    mask{i, 1}(IN == 1) = 1;
    mask{i, 1}          = (mask{i, 1} .* A)./sum(sum(mask{i, 1} .* A));
    
    region_ids(i) = S(i).ID;
end


% Read the time-vector and -unit
tme      = ncread(fle_in, 'time');
tme_unit = ncreadatt(fle_in, 'time', 'units');

% Convert relative to absolute dates
tme_abs  = reldate2absdate(tme, tme_unit);

% Convert the absolute dates to relative dates in the unit days since
% ... --> CHANGE THIS!!!
tme_rel  = days(datetime(tme_abs) - datetime('1950-01-01 00:00:00'));
    
if tme_abs(1, 1) > 2016
    ens = 51;
else
    ens = 25;
end

[dtainfo, vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_metadata();
[dimnames, dimstandard, dimlong, dimunits, dimvals] = set_dimensions(region_ids, ens, tme_rel);

create_nd_netcdf(fle_out, dtainfo, vars, varstandard, varlong, units, varprec, varfill, varscale, varoffset, dimnames, dimstandard, dimlong, dimunits, dimvals, [], true);

for j = 1:length(vars)
        
    dta = ncread(fle_in, vars{j});
        
    tmp = size(dta);
        
    % Throw away the first two dimensions (i.e. lon and lat)
    TS_out = NaN([length(S), tmp(3:end)]);
        
    for k = 1:length(mask)
            
        mask_act = mask{k, 1};
        mask_act = repmat(mask_act, [1 1 tmp(3:end)]);
            
        TS_tmp   = nansum(nansum(mask_act .* dta, 1), 2);
        TS_tmp   = squeeze(TS_tmp);
            
        % This needs to be changed in order to accept an arbitrary
        % number of dimensions...
        TS_out(k, :, :) = TS_tmp;         
    end
        
    ncwrite(fle_out, vars{j}, TS_out);
end

        
        
end
     

function [dimnames, dimstandard, dimlong, dimunits, dimvals] = set_dimensions(region_ids, ens, time)
    dimnames = {'region'; ...
                'ensemble'; ...
                'time'};
    dimstandard = {'region'; ...
                   []; ...
                   'time'};
    dimlong  = {[], 'ensemble member', []};
    dimunits = {[], [], 'days since 1950-01-01 00:00:00'};
    dimvals  = {region_ids; 0:ens-1; time};
end   
     
function [dtainfo, vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_metadata()

    dtainfo.title          = ['SEAS5 BCSD area aggregated forecasts'];
    dtainfo.Conventions    = 'CF-1.8';
    dtainfo.references     = 'TBA';
    dtainfo.institution    = 'Karlsruhe Institute of Technology - Institute of Meteorology and Climate Research';
    dtainfo.source         = 'SEAS5 BCSD (Bias-corrected and spatially disaggregated seasonal forecasts)';
    dtainfo.comment        = [''];
    dtainfo.history        = [datestr(now, 'yyyy-mm-dd HH:MM:SS'), ': File created.'];
    dtainfo.Contact_person = 'Christof Lorenz (Christof.Lorenz@kit.edu)';
    dtainfo.Author         = 'Christof Lorenz (Christof.Lorenz@kit.edu)';
    dtainfo.License        = 'For non-commercial use only';

    vars = {'tp', 't2m', 't2min', 't2max', 'ssrd'};

    varlong = {'total_precipitation', ...
               '2m_temperature', ...
               'minimum_daily_temperature_at_2m', ... 
               'maximum_daily_temperature_at_2m', ...
               'surface_solar_radiation'};
             
    varstandard = {'precipitation_flux', ...
                   'air_temperature', ...
                   'air_temperature', ...
                   'air_temperature', ...
                   'surface_solar_radiation'};
           
    units = {'mm/day', ...
             'K', ...
             'K', ...
             'K', ...
             'W m-2'};

    varprec = {'NC_INT', ...
               'NC_INT', ...
               'NC_INT', ...
               'NC_INT', ...
               'NC_INT'};
           
    varfill    = {-9999, -9999, -9999, -9999, -9999};
    varscale   = {0.001, 0.0001, 0.0001, 0.0001, 0.001};
    varoffset  = {[], 273.15, 273.15, 273.15, []};
    
end     



