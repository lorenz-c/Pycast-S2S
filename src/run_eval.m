addpath(genpath('/home/lorenz-c/bin_new/Matlab_GeoTS_Tools'))

% Read the parameters from the current parameter file 
parameter = jsondecode(fileread('bcsd_parameter.json'))

% Convert the domain names in the parameter JSON to an array:
domain_names =  cellstr(char(parameter.domains.name))

% Get the version of the BCSD dataset
version = parameter.version;

% Set month and year of the current forecast
month = parameter.issue_date.month;
year  = parameter.issue_date.year;

for i = 1:length(domain_names)

    evaluate_forecasts(domain_names{i}, month, year, version);
    evaluate_regav_forecasts(domain_names{i}, month, year, version);

end

