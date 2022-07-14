addpath(genpath('/home/lorenz-c/bin_new/Matlab_GeoTS_Tools'))

% Read the parameters from the current parameter file 
parameter = jsondecode(fileread('bcsd_parameter.json'));

% Convert the domain names in the parameter JSON to an array:
domain_names =  cellstr(char(parameter.domains.name))

% Set month and year of the current forecast
month = parameter.issue_date.month;
year  = parameter.issue_date.year;

yr_str   = num2str(year);
mnth_str = num2str(month, '%02.0f');


parfor i = 1:length(domain_names)

    fle_in    = [parameter.directories.regroot, domain_names{i}, '/daily/seas5_bcsd/SEAS5_BCSD_v', parameter.version, '_daily_', yr_str, mnth_str, '_0.1_', domain_names{i}, '.nc'];
    fle_out   = [parameter.directories.regroot, domain_names{i}, '/daily/seas5_bcsd_timeseries/SEAS5_BCSD_v', parameter.version, '_daily_', yr_str, mnth_str, '_TS_', domain_names{i}, '.nc'];
    shapefile = ['shapefiles/SaWaM_Forecast_Domains_', domain_names{i}, '.shp'];

    ts_out    = nc_regionaverage_shape_oper(fle_in, fle_out, shapefile);

end


