import xarray as xr
import numpy as np
import os


def set_and_make_dirs(domain_config):

    #if data_set  == "seas5":
        # Global directory of Domain
        # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'

    # List of Directories
    dir_general = {
        "domain_dir":             f"{domain_config['regroot']}",
        #"frcst_low_glob_dir":     "/Users/borkenhagen-c/KIT_Master/test/files",
        #"ref_low_glob_dir":       "/Users/borkenhagen-c/KIT_Master/test/files",
        "frcst_low_glob_dir":     f"/pd/data/regclim_data/gridded_data/seasonal_predictions/seas5/daily",
        "ref_low_glob_dir":       f"/pd/data/regclim_data/gridded_data/reanalyses/era5_land/daily",
        "raw_low_reg_dir":        f"{domain_config['regroot']}/01_raw_low_reg/",
        "raw_high_reg_dir":       f"{domain_config['regroot']}/02_raw_high_reg/",
        "eval_dir":               f"{domain_config['regroot']}/03_evaluation",
        "statistics":             f"{domain_config['regroot']}/04_statistics/",
        "grd_dir":                f"{domain_config['regroot']}/05_static"

    }


    # Specific
    dir_dict = {
        # Forecast-Directories (frcst_resolution_global/regional_directory)
        "frcst_low_reg_dir":                  f"{dir_general['raw_low_reg_dir']}/{domain_config['raw_forecasts']['prefix']}",
        "frcst_high_reg_dir":                 f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h",
        "frcst_high_reg_lnch_dir":            f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h/lnch",
        "frcst_high_reg_lnch_calib_dir":      f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h/lnch",
        "frcst_high_reg_bcsd_daily_dir":      f"{dir_general['eval_dir']}/daily",
        "frcst_high_reg_bcsd_daily_lnch_dir": f"{dir_general['eval_dir']}/daily/lnch",
        "frcst_high_reg_bcsd_monthly_dir":    f"{dir_general['eval_dir']}/monthly",

        # Ref-Directories
        "ref_low_reg_dir":                      f"{dir_general['raw_low_reg_dir']}/{domain_config['reference_history']['prefix']}",
        "ref_high_reg_daily_dir":               f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily",# Because ERA5-Land is 0.1 resulution, low and high resolution is the same
        "ref_high_reg_daily_lnch_dir":          f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily/lnch",
        "ref_high_reg_daily_lnch_calib_dir":    f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily/lnch",
        "ref_high_reg_monthly_dir":             f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/monthly",

        # Statistics
        "climatology_dir":          f"{dir_general['statistics']}/climatology",
        "frcst_climatology":        f"{dir_general['statistics']}/climatology/{domain_config['raw_forecasts']['prefix']}",
        "ref_climatology":          f"{dir_general['statistics']}/climatology/{domain_config['reference_history']['prefix']}",
        "frcst_quantile":           f"{dir_general['statistics']}/quantiles/{domain_config['bcsd_forecasts']['prefix']}_thresholds",

    }
    # Update directories (merge them together)
    dir_dict.update(dir_general)

    for key in dir_dict:
        if not os.path.isdir(dir_dict[key]):
            os.makedirs(dir_dict[key])

    return dir_dict

# Set filenames
# Change this function and add some history filenames with syr_calib and eyr_calib
#def set_filenames(domain_config, syr = None, eyr = None, year = None, month_str = None, merge = None, variable = None):

def set_filenames(domain_config, year = None, month_str = None, merge = None, variable = None):
    
    # Filenames are different depending, if all variables should be merged into one file, or each variable will be stored as sepperate file
    if merge == True:
        fnme_dict = {
            # Grid
            "grd_dir":              f"{domain_config['prefix']}_{domain_config['target_resolution']}_grd.txt",

            # Forecast
            "frcst_low_glob_dir":                    f"ECMWF_SEAS5_*_{year}{month_str}.nc",
            "frcst_low_reg_dir":                     f"{domain_config['raw_forecasts']['prefix']}_{year}{month_str}_O320_{domain_config['prefix']}.nc",
            "frcst_high_reg_dir":                    f"{domain_config['raw_forecasts']['prefix']}_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "frcst_high_reg_lnch_dir":               f"{domain_config['raw_forecasts']['prefix']}_{year}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "frcst_high_reg_lnch_calib_dir":         f"{domain_config['raw_forecasts']['prefix']}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "frcst_high_reg_bcsd_daily_dir":         f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "frcst_high_reg_bcsd_daily_lnch_dir":    f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_daily_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",

            # Ref
            "ref_low_glob_raw_dir":              f"{domain_config['reference_history']['prefix']}_daily_{variable}_{year}.nc",
            "ref_low_glob_dir":                  f"{domain_config['reference_history']['prefix']}_daily_{year}.nc",
            "ref_low_reg_dir":                   f"{domain_config['reference_history']['prefix']}_{year}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_dir":            f"{domain_config['reference_history']['prefix']}_{year}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_lnch_dir":       f"{domain_config['reference_history']['prefix']}_{year}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "ref_high_reg_daily_lnch_calib_dir": f"{domain_config['reference_history']['prefix']}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",


            # Statistics
            "frcst_climatology":             f"{domain_config['raw_forecasts']['prefix']}_climatology_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_0320_{domain_config['prefix']}.nc",
            "ref_climatology":               f"{domain_config['reference_history']['prefix']}_climatology_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['prefix']}.nc"
        }
    else:
        fnme_dict = {
            # Grid
            "grd_dir": f"{domain_config['prefix']}_{domain_config['target_resolution']}_grd.txt",

            # Forecast
            "frcst_low_glob_dir":            f"ECMWF_SEAS5_*_{year}{month_str}.nc",
            "frcst_low_reg_dir":             f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month_str}_O320_{domain_config['prefix']}.nc",
            "frcst_high_reg_dir":            f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "frcst_high_reg_lnch_dir":       f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "frcst_high_reg_lnch_calib_dir": f"{domain_config['raw_forecasts']['prefix']}_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "frcst_high_reg_bcsd_daily_dir": f"{domain_config['bcsd_forecasts']['prefix']}_{variable}_v{domain_config['version']}_daily_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "frcst_high_reg_bcsd_daily_lnch_dir": f"{domain_config['bcsd_forecasts']['prefix']}_{variable}_v{domain_config['version']}_daily_{year}{month_str}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",

            # Ref
            "ref_low_glob_dir":                  f"{domain_config['reference_history']['prefix']}_daily_{variable}_{year}.nc",
            "ref_low_reg_dir":                   f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_dir":            f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_lnch_dir":       f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",
            "ref_high_reg_daily_lnch_calib_dir": f"{domain_config['reference_history']['prefix']}_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['target_resolution']}_{domain_config['prefix']}_lns.nc",

            # Statistics
            "frcst_climatology":                 f"{domain_config['raw_forecasts']['prefix']}_climatology_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{month_str}_0320_{domain_config['prefix']}.nc",
            "ref_climatology":                   f"{domain_config['reference_history']['prefix']}_climatology_{variable}_{domain_config['syr_calib']}_{domain_config['eyr_calib']}_{domain_config['prefix']}.nc"
    }

    return fnme_dict