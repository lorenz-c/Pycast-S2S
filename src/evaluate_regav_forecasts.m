function [] = evaluate_regav_forecasts(domain, month, year, version)

vars = {'tp', 't2m'};

yr_str   = num2str(year);
mnth_str = num2str(month, '%02.0f');

issue_date = [yr_str, mnth_str, '01'];


if year < 2017
    ens = 25;
else
    ens = 51;
end

if strcmp(domain, 'Khuzestan')
    basin_name = 'khuzestan';
    basin_id   = '1000000';
elseif strcmp(domain, 'SF_Basin')
    basin_name = 'sf_basin';
    basin_id   = '2000000';
elseif strcmp(domain, 'TABN')
    basin_name = 'tabn';
    basin_id   = '3000000';
elseif strcmp(domain, 'Chira')
    basin_name = 'chira';
    basin_id   = '4000000';
end


% Set the base-directory for the current domain
basedir = ['/pd/data/regclim_data/gridded_data/processed/', domain];
dirout  = ['/pd/data/regclim_data/sawam_output/', basin_name, '/frcst/regav/hydromet/'];

climatology      = [basedir, '/climatology/era5_land_timeseries/ERA5_Land_climatology_1981_2016_TS_', domain, '.nc'];
tercile_ref_fle  = [basedir, '/monthly/seas5_bcsd_timeseries_thresholds/SEAS5_BCSD_v', version, '_monthly_terciles_', mnth_str, '_TS_', domain, '.nc'];
quintile_ref_fle = [basedir, '/monthly/seas5_bcsd_timeseries_thresholds/SEAS5_BCSD_v', version, '_monthly_quintiles_', mnth_str, '_TS_', domain, '.nc'];
extreme_ref_fle  = [basedir, '/monthly/seas5_bcsd_timeseries_thresholds/SEAS5_BCSD_v', version, '_monthly_extreme_quantiles_', mnth_str, '_TS_', domain, '.nc'];

% Set the filename of the current forecast
fle_in  = [basedir, '/monthly/seas5_bcsd_timeseries/SEAS5_BCSD_v', version, '_monthly_', yr_str, mnth_str, '_TS_', domain, '.nc'];

% Get the time of the forecast in the current directory
tme      = ncread(fle_in, 'time');
tme_unit = ncreadatt(fle_in, 'time', 'units');
tme_abs  = reldate2absdate(tme, tme_unit);

tme_abs(:, 3)     = 15;
tme_abs(:, 4:end) = 0;
tme_abs           = tme_abs(1:7, :);

tme_out           = days(datetime(tme_abs) - datetime('1950-01-01 00:00:00'));

tme_unit_out = 'days since 1950-01-01 00:00:00';

% Get the regions
region_ids = ncread(fle_in, 'region');

% Calculate the lenghts of the dimensions
nregions    = length(region_ids);



% Loop over the forecasted months

[fleinfo, varinfo] = set_metadata(vars);
    
fle_out = [dirout, basin_id, '_frcst_regav_hydromet_monthly_multiple_kit_seas5bcsd_v', version, '_e1_', yr_str, mnth_str, '15.nc'];
       
diminfo = set_dimensions(region_ids, tme_out);
       
create_nd_netcdf(fle_out, ...
                 fleinfo, ...
                 varinfo.short, ...
                 varinfo.standard, ...
                 varinfo.long, ...
                 varinfo.units, ...
                 varinfo.precision, ...
                 varinfo.fill, ...
                 varinfo.scale, ...
                 varinfo.offset, ...
                 diminfo.short, ...
                 diminfo.standard, ...
                 diminfo.long, ...
                 diminfo.units, ...
                 diminfo.vals, ...
                 [length(region_ids) 7], ...
                 true);

ncwriteatt(fle_out, '/', 'issue_date', issue_date)   

for i = 1:length(vars)
    
    dta_in = ncread(fle_in, vars{i});
    dta_in = dta_in(:, :, 1:7);
    
    tercile_ref  = ncread(tercile_ref_fle, vars{i});
    quintile_ref = ncread(quintile_ref_fle, vars{i});
    extreme_ref  = ncread(extreme_ref_fle, vars{i});
    
    clim         = ncread(climatology, vars{i});
    clim         = clim(:, tme_abs(1:7, 2));
    
    ens_mean     = squeeze(mean(dta_in, 2));
    ens_median   = squeeze(median(dta_in, 2));
    ens_std      = squeeze(std(dta_in, [], 2));
    ens_iqr      = squeeze(iqr(dta_in, 2));
        
    ens_spread   = squeeze(max(dta_in, [], 2) - min(dta_in, [], 2));
    
    if length(region_ids) == 1
        ens_mean = ens_mean';
        ens_median = ens_median';
        ens_std = ens_std';
        ens_iqr = ens_iqr';
        
        ens_spread = ens_spread';
    end

 
    delta_clim   = ens_mean - clim;
    
    ens_mean_rel = (ens_mean ./ clim - 1) * 100;
    ens_med_rel  = (ens_median ./ clim - 1) * 100;
    
    terciles_sum  = NaN(nregions, 3, size(dta_in, 4));
    quintiles_sum = NaN(nregions, 5, size(dta_in, 4));
    extreme_sum   = NaN(nregions, 3, size(dta_in, 4));
         
    for j = 1:size(dta_in, 3)
        
        % Count the number of ensemble members for each tercile category
        terciles_sum(:, 1, j) = sum(dta_in(:, :, j) < tercile_ref(:, 1, j), 2);
        terciles_sum(:, 2, j) = sum(dta_in(:, :, j) >= tercile_ref(:, 1, j) & dta_in(:, :, j) < tercile_ref(:, 2, j), 2);
        terciles_sum(:, 3, j) = sum(dta_in(:, :, j) >= tercile_ref(:, 2, j), 2);
  
        % Count the number of ensemble members for each quintile category
        quintiles_sum(:, 1, j) = sum(dta_in(:, :, j) <  quintile_ref(:, 1, j), 2);
        quintiles_sum(:, 2, j) = sum(dta_in(:, :, j) >= quintile_ref(:, 1, j) & dta_in(:, :, j) < quintile_ref(:, 2, j), 2);
        quintiles_sum(:, 3, j) = sum(dta_in(:, :, j) >= quintile_ref(:, 2, j) & dta_in(:, :, j) < quintile_ref(:, 3, j), 2);
        quintiles_sum(:, 4, j) = sum(dta_in(:, :, j) >= quintile_ref(:, 3, j) & dta_in(:, :, j) < quintile_ref(:, 4, j), 2);
        quintiles_sum(:, 5, j) = sum(dta_in(:, :, j) >= quintile_ref(:, 4, j), 2);
    
        % Count the number of ensemble members for each extreme category
        extreme_sum(:, 1, j) = sum(dta_in(:, :, j) < extreme_ref(:, 1, j), 2);
        extreme_sum(:, 2, j) = sum(dta_in(:, :, j) >= extreme_ref(:, 1, j) & dta_in(:, :, j) <= extreme_ref(:, 2, j),2);
        extreme_sum(:, 3, j) = sum(dta_in(:, :, j) > extreme_ref(:, 2, j), 2);  
 
    end
    
    terciles_sum  = terciles_sum / ens * 100; 
    quintiles_sum = quintiles_sum / ens * 100; 
    extreme_sum   = extreme_sum / ens * 100;
    
    
    % Get the categories with the most ensemble members
    % Here, Y holds the probability of a category and I the index of that
    % category
    [Y_quintiles, I_quintiles] = max(quintiles_sum, [], 2);
    [Y_terciles, I_terciles]   = max(terciles_sum, [], 2);
    [Y_extreme, I_extreme]     = max(extreme_sum, [], 2);
    
    Y_quintiles = squeeze(Y_quintiles);
    I_quintiles = squeeze(I_quintiles);
    Y_terciles  = squeeze(Y_terciles);
    I_terciles  = squeeze(I_terciles);
    Y_extreme   = squeeze(Y_extreme);
    I_extreme   = squeeze(I_extreme);

    % In cases where all ensemble member fall in the same category, set the probability to 99.99% so that we do not "jump" into the next category´
    Y_quintiles(Y_quintiles == 100) = 99.99;
    Y_terciles(Y_terciles == 100)   = 99.99;
    Y_extreme(Y_extreme == 100)     = 99.99;
    
    % Construct a new variable which holds the category and the
    % corresponding probability
    max_categ_quintiles = NaN(size(Y_quintiles));
    max_categ_terciles  = NaN(size(Y_quintiles));
    max_categ_extreme   = NaN(size(Y_quintiles));
    
    max_categ_quintiles = I_quintiles + Y_quintiles/100;
    max_categ_terciles  = I_terciles + Y_terciles/100;
    max_categ_extreme   = I_extreme + Y_extreme/100;
    
    % In cases where the probability of the maximum category "less than low", set these pixels to -0.5; this is in accordance with the approach from SMHI; see 
    % https://hypewebapp.smhi.se/hypeweb-climate/seasonal-forecasts/metadata/Glorious_SeFo_Metadata_RiverFlow.pdf
    max_categ_quintiles(Y_quintiles == 25) = 0.5;
    max_categ_terciles(Y_terciles == 35)   = 0.5;
    
    if strcmp(vars{i}, 'tp')
        max_categ_quintiles(squeeze(extreme_ref(:, 2, :)) < 1) = -0.5;
        max_categ_terciles(squeeze(extreme_ref(:, 2, :)) < 1) = -0.5;
        max_categ_extreme(squeeze(extreme_ref(:, 2, :)) < 1) = -0.5;
    end
    
    boxplot_data = NaN(length(region_ids), 4, 7);

    boxplot_data(:, 1, :) = quantile(dta_in, 0.75, 2);
    boxplot_data(:, 2, :) = max(dta_in, [], 2);
    boxplot_data(:, 3, :) = min(dta_in, [], 2);
    boxplot_data(:, 4, :) = quantile(dta_in, 0.25, 2);

    dims      = {'region', 'values', 'time'};
    dimlength = [nregions 4 7];
    chnks     = [nregions 4 7];

    [vars_add, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_boxplot_metadata(vars{i});

    add_nd_var(fle_out, ...
               vars_add, ...
               varstandard, ...
               varlong, ...
               units, ...
               varprec, ...
               varfill, ...
               varscale, ...
               varoffset, ...
               dims, ...
               dimlength, ...
               chnks);
           
           
    dims      = {'region', 'terciles', 'time'};
    dimlength = [nregions 3 7];
    chnks     = [nregions 3 7];

    [vars_add, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_tercile_metadata(vars{i});

    add_nd_var(fle_out, ...
               vars_add, ...
               varstandard, ...
               varlong, ...
               units, ...
               varprec, ...
               varfill, ...
               varscale, ...
               varoffset, ...
               dims, ...
               dimlength, ...
               chnks);    
   
    dims      = {'region', 'quintiles', 'time'};
    dimlength = [nregions 5 7];
    chnks     = [nregions 5 7];

    [vars_add, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_quintile_metadata(vars{i});

    add_nd_var(fle_out, ...
               vars_add, ...
               varstandard, ...
               varlong, ...
               units, ...
               varprec, ...
               varfill, ...
               varscale, ...
               varoffset, ...
               dims, ...
               dimlength, ...
               chnks); 
           
    dims      = {'region', 'extremes', 'time'};
    dimlength = [nregions 3 7];
    chnks     = [nregions 3 7];

    [vars_add, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_extreme_metadata(vars{i});

    add_nd_var(fle_out, ...
               vars_add, ...
               varstandard, ...
               varlong, ...
               units, ...
               varprec, ...
               varfill, ...
               varscale, ...
               varoffset, ...
               dims, ...
               dimlength, ...
               chnks);      
               
    
    if strcmp(vars{i}, 't2m') | strcmp(vars{i}, 't2min') | strcmp(vars{i}, 't2max')
        ens_mean     = ens_mean - 273.15;
        ens_median   = ens_median - 273.15;
        boxplot_data = boxplot_data - 273.15;
    end

    % Round most of the variables to one digit
    if strcmp(vars{i}, 'tp') | strcmp(vars{i}, 't2m') | strcmp(vars{i}, 't2min') | strcmp(vars{i}, 't2max') | strcmp(vars{i}, 'ssrd')
        ens_mean     = round(ens_mean, 1);
        ens_median   = round(ens_median, 1);
        ens_spread   = round(ens_spread, 1);
        ens_std      = round(ens_std, 1);
        ens_iqr      = round(ens_iqr, 1);
        delta_clim   = round(delta_clim, 1);
        ens_mean_rel = round(ens_mean_rel, 1);
    
        max_categ_terciles  = round(max_categ_terciles, 2);
        max_categ_quintiles = round(max_categ_quintiles, 2);
        max_categ_extreme   = round(max_categ_extreme, 2);
    
        boxplot_data = round(boxplot_data, 1);
    end

    ncwrite(fle_out, [vars{i}, '_ensemble_mean'], ens_mean);
    ncwrite(fle_out, [vars{i}, '_ensemble_median'], ens_median);
    ncwrite(fle_out, [vars{i}, '_ensemble_spread'], ens_spread);
    ncwrite(fle_out, [vars{i}, '_ensemble_std'], ens_std);
    ncwrite(fle_out, [vars{i}, '_ensemble_iqr'], ens_iqr);
    ncwrite(fle_out, [vars{i}, '_ensemble_anomaly'], delta_clim);
    ncwrite(fle_out, [vars{i}, '_ensemble_mean_relative'], ens_mean_rel);
    
    if length(region_ids) == 1
        max_categ_terciles  = max_categ_terciles';
        max_categ_quintiles = max_categ_quintiles';
        max_categ_extreme   = max_categ_extreme';
    end
        
    ncwrite(fle_out, [vars{i}, '_tercile_probab'], max_categ_terciles);
    ncwrite(fle_out, [vars{i}, '_quintile_probab'], max_categ_quintiles);
    ncwrite(fle_out, [vars{i}, '_extreme_probab'], max_categ_extreme);
    
    ncwrite(fle_out, [vars{i}, '_boxplot_data'], boxplot_data); 
    ncwrite(fle_out, [vars{i}, '_terciles'], terciles_sum);
    ncwrite(fle_out, [vars{i}, '_quintiles'], quintiles_sum);
    ncwrite(fle_out, [vars{i}, '_extremes'], extreme_sum);
    
end

ncwriteatt(fle_out, 't2m_terciles', 'categories', 'below_normal near_normal above_normal');
ncwriteatt(fle_out, 't2m_terciles', 'category_color', '#08519c #006d2c #67001f');
ncwriteatt(fle_out, 't2m_quintiles', 'categories', 'very_cold moderate_cold normal moderate_warm very_warm');
ncwriteatt(fle_out, 't2m_quintiles', 'category_color', '#54278f #08519c #006d2c #993404 #a50f15');
ncwriteatt(fle_out, 't2m_extremes', 'categories', 'extreme_cold no_extremes extreme_warm');
ncwriteatt(fle_out, 't2m_extremes', 'category_color', '#053061 #f7f7f7 #67001f');

ncwriteatt(fle_out, 'tp_terciles', 'categories', 'below_normal near_normal above_normal');
ncwriteatt(fle_out, 'tp_terciles', 'category_color', '#993304 #006d2c #08529c');
ncwriteatt(fle_out, 'tp_quintiles', 'categories', 'very_dry moderate_dry normal moderate_wet very_wet');
ncwriteatt(fle_out, 'tp_quintiles', 'category_color', '#a50f14 #993304 #006d2c #08529c #54278f');
ncwriteatt(fle_out, 'tp_extremes', 'categories', 'extreme_dry no_extremes extreme_wet');
ncwriteatt(fle_out, 'tp_extremes', 'category_color', '#67001f #f7f7f7 #053061');
           
end


function diminfo = set_dimensions(region_ids, tme)

    diminfo.short    = {'region', 'time'};
    diminfo.standard = {[], 'time'};
    diminfo.long     = {[], 'time'};
    diminfo.units    = {[], 'days since 1950-01-01 00:00:00'};
    diminfo.vals     = {region_ids, tme};

end  

function [fleinfo, varinfo] = set_metadata(variable)

% Set the global attributes
fleinfo.title          = 'SEAS5 BCSD v2.1, probabilistic forecasts and forecast measures';
fleinfo.Conventions    = 'CF-1.8';
fleinfo.references     = 'TBA';
fleinfo.institution    = 'Karlsruhe Institute of Technology - Institute of Meteorology and Climate Research';
fleinfo.source         = 'ECMWF SEAS5, ERA5-Land';
fleinfo.comment        = '';
fleinfo.history        = [datestr(now, 'yyyy-mm-dd HH:MM:SS'), ': File created.'];
fleinfo.Contact_person = 'Christof Lorenz (Christof.Lorenz@kit.edu)';
fleinfo.Author         = 'Christof Lorenz (Christof.Lorenz@kit.edu)';
fleinfo.License        = 'For non-commercial use only';


varinfo = struct('short',    [], ...
                 'long',     [], ...
                 'standard', [], ...
                 'units', [], ...
                 'precision', [], ...
                 'fill', [], ...
                 'scale', [], ...
                 'offset', []);


for i = 1:length(variable)
    
    varinfo.short = [varinfo.short, ...
                     {[variable{i}, '_ensemble_mean'], ...
                      [variable{i}, '_ensemble_median'], ...
                      [variable{i}, '_ensemble_spread'], ...
                      [variable{i}, '_ensemble_std'], ...
                      [variable{i}, '_ensemble_iqr'], ...
                      [variable{i}, '_ensemble_anomaly'], ...
                      [variable{i}, '_ensemble_mean_relative'], ...
                      [variable{i}, '_tercile_probab'], ...
                      [variable{i}, '_quintile_probab'], ...
                      [variable{i}, '_extreme_probab']}];



    varinfo.long  = [varinfo.long, ...
                    {'Ensemble mean', ...
                     'Ensemble median', ...
                     'Ensemble spread', ...
                     'Ensemble standard deviation', ...
                     'Ensemble inter quartile range', ...
                     'Ensemble anomaly from the climatology', ...
                     'Relative ensemble anomaly', ...
                     'Tercile category and probability with most ensemble members', ...
                     'Quintile category and probability with most ensemble members', ...
                     'Extreme category and probability with most ensemble members'}];
             
    varinfo.standard  = [varinfo.standard, ...
                        {[], ...
                         [], ...
                         [], ...
                         [], ...
                         [], ...
                         [], ...
                         []', ...
                         []', ...
                         []', ...
                         []'}];

    if strcmp(variable{i}, 'tp')
        varinfo.units  = [varinfo.units, ...
                         {'mm/day', ...
                          'mm/day', ...
                          'mm/day', ...
                          'mm/day', ...
                          'mm/day', ...
                          'mm/day', ...
                          [], ...
                          [], ...
                          [], ...
                          []}];
                      
    elseif strcmp(variable{i}, 't2m') | strcmp(variable{i}, 't2min') | strcmp(variable{i}, 't2max')
        varinfo.units  = [varinfo.units, ...
                         {'°C', ...
                          '°C', ...
                          '°C', ...
                          '°C', ...
                          '°C', ...
                          '°C', ...
                          [], ...
                          [], ...
                          [], ...
                          []}];
                      
    elseif strcmp(variable{i}, 'ssrd')
        varinfo.units  = [varinfo.units, ...
                         {'W m-2', ...
                          'W m-2', ...
                          'W m-2', ...
                          'W m-2', ...
                          'W m-2', ...
                          'W m-2', ...
                          [], ...
                          [], ...
                          [], ...
                          []}];
                      
    end

    varinfo.precision  = [varinfo.precision, ...
                         {'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT', ...
                          'NC_FLOAT'}];

                 
    varinfo.fill       = [varinfo.fill, ...
                             {-9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999, ...
                              -9999}];
                          
    varinfo.scale          = [varinfo.scale, ...
                             {[], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              []}];
                          
    varinfo.offset         = [varinfo.offset, ...
                             {[], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              [], ...
                              []}];
                          
end



end


function [vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_boxplot_metadata(variable)
    
    vars        = [variable, '_boxplot_data'];
    varlong     = 'statistical_boxplot_parameter';     
    varstandard = [];     
    units       = '';
    varprec     = 'NC_FLOAT';
    varfill     = -9999;
    varscale    = [];
    varoffset   = [];
    
end

function [vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_tercile_metadata(variable)
    
    vars        = [variable, '_terciles'];
    varlong     = 'number_of_ensemble_members_per_tercile_category';     
    varstandard = [];     
    units       = '';
    varprec     = 'NC_FLOAT';
    varfill     = -9999;
    varscale    = [];
    varoffset   = [];
    
end

function [vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_quintile_metadata(variable)
    
    vars        = [variable, '_quintiles'];
    varlong     = 'number_of_ensemble_members_per_quintile_category';     
    varstandard = [];     
    units       = '';
    varprec     = 'NC_FLOAT';
    varfill     = -9999;
    varscale    = [];
    varoffset   = [];
    
end

function [vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard] = set_extreme_metadata(variable)
    
    vars        = [variable, '_extremes'];
    varlong     = 'number_of_ensemble_members_per_extreme_category';     
    varstandard = [];     
    units       = '';
    varprec     = 'NC_FLOAT';
    varfill     = -9999;
    varscale    = [];
    varoffset   = [];
    
end
