import os


def set_and_make_dirs(domain_config):

    # if data_set  == "seas5":
    # Global directory of Domain
    # glb_dir = '/Volumes/pd/data/regclim_data/gridded_data/processed/'

    # List of Directories
    dir_general = {
        "domain_dir": f"{domain_config['regroot']}",
        # "frcst_low_glob_dir":     "/Users/borkenhagen-c/KIT_Master/test/files",
        # "ref_low_glob_dir":       "/Users/borkenhagen-c/KIT_Master/test/files",
        "frcst_low_glob_dir": "/pd/data/regclim_data/gridded_data/seasonal_predictions/seas5/daily",
        "ref_low_glob_dir": "/pd/data/regclim_data/gridded_data/reanalyses/era5_land/daily",
        "raw_low_reg_dir": f"{domain_config['regroot']}/01_raw_low_reg/",
        "raw_high_reg_dir": f"{domain_config['regroot']}/02_raw_high_reg/",
        "eval_dir": f"{domain_config['regroot']}/03_evaluation",
        "statistics": f"{domain_config['regroot']}/04_statistics/",
        "grd_dir": f"{domain_config['regroot']}/05_static",
    }

    # Specific
    dir_dict = {
        # Forecast-Directories (frcst_resolution_global/regional_directory)
        "frcst_low_reg_dir": f"{dir_general['raw_low_reg_dir']}/{domain_config['raw_forecasts']['prefix']}",
        "frcst_high_reg_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h",
        "frcst_high_reg_lnch_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h/lnch",
        "frcst_high_reg_lnch_calib_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['raw_forecasts']['prefix']}_h/lnch",
        "frcst_high_reg_bcsd_daily_dir": f"{dir_general['eval_dir']}/daily",
        "frcst_high_reg_bcsd_daily_lnch_dir": f"{dir_general['eval_dir']}/daily/lnch",
        "frcst_high_reg_bcsd_monthly_dir": f"{dir_general['eval_dir']}/monthly",
        # Ref-Directories
        "ref_low_reg_dir": f"{dir_general['raw_low_reg_dir']}/{domain_config['reference_history']['prefix']}",
        "ref_high_reg_daily_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily",
        "ref_high_reg_daily_lnch_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily/lnch",
        "ref_high_reg_daily_lnch_calib_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/daily/lnch",
        "ref_high_reg_monthly_dir": f"{dir_general['raw_high_reg_dir']}/{domain_config['reference_history']['prefix']}/monthly",
        # Statistics
        "climatology_dir": f"{dir_general['statistics']}/climatology",
        "frcst_climatology": f"{dir_general['statistics']}/climatology/{domain_config['raw_forecasts']['prefix']}",
        "ref_climatology": f"{dir_general['statistics']}/climatology/{domain_config['reference_history']['prefix']}",
        "frcst_quantile": f"{dir_general['statistics']}/quantiles/{domain_config['bcsd_forecasts']['prefix']}_thresholds",
    }
    # Update directories (merge them together)
    dir_dict.update(dir_general)

    for key in dir_dict:
        if not os.path.isdir(dir_dict[key]):
            os.makedirs(dir_dict[key])

    return dir_dict


# Set filenames
# Change this function and add some history filenames with syr_calib and eyr_calib
# def set_filenames(domain_config, syr = None, eyr = None, year = None, month_str = None, merge = None, variable = None):


def set_gridfile(domain_config: dict) -> str:

    return f"{domain_config['regroot']}/05_static/{domain_config['prefix']}_{domain_config['target_resolution']}_grd.txt"


def set_filenames(
    domain_config: dict, year: int, month: int, merge=None, variable=None
):

    frcst_prefix = f"{domain_config['raw_forecasts']['prefix']}"
    frcst_var_prefix = f"{frcst_prefix}_{variable}"
    pp_prefix = (
        f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}"
    )
    pp_var_prefix = f"{domain_config['bcsd_forecasts']['prefix']}_{variable}_v{domain_config['version']}"
    ref_prefix = f"{domain_config['reference_history']['prefix']}"

    calib_period = f"{domain_config['syr_calib']}_{domain_config['eyr_calib']}"
    domain = f"{domain_config['prefix']}"
    target_res = f"{domain_config['target_resolution']}"

    # Filenames are different depending, if all variables should be merged into one file, or each variable will be
    # stored as sepperate file
    if merge:
        fnme_dict = {
            # Forecast
            "frcst_low_glob_dir": f"ECMWF_SEAS5_*_{year}{month:02d}.nc",
            "frcst_low_reg_dir": f"{frcst_prefix}_{year}{month:02d}_O320_{domain_config['prefix']}.nc",
            "frcst_high_reg_dir": f"{frcst_prefix}_{year}{month}_{target_res}_{domain}.nc",
            "frcst_high_reg_lnch_dir": f"{frcst_prefix}_{year}_{month:02d}_{target_res}_{domain}_lns.nc",
            "frcst_high_reg_lnch_calib_dir": f"{frcst_prefix}_{calib_period}_{month:02d}_{target_res}_{domain}_lns.nc",
            "frcst_high_reg_bcsd_daily_dir": f"{pp_prefix}_daily_{year}{month:02d}_{target_res}_{domain}.nc",
            "frcst_high_reg_bcsd_daily_lnch_dir": f"{pp_prefix}_daily_{year}{month:02d}_{target_res}_{domain}.nc",
            # Ref
            "ref_low_glob_raw_dir": f"{ref_prefix}_daily_{variable}_{year}.nc",
            "ref_low_glob_dir": f"{ref_prefix}_daily_{year}.nc",
            "ref_low_reg_dir": f"{ref_prefix}_{year}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_dir": f"{ref_prefix}_{year}_{target_res}_{domain}.nc",
            "ref_high_reg_daily_lnch_dir": f"{ref_prefix}_{year}_{target_res}_{domain}_lns.nc",
            "ref_high_reg_daily_lnch_calib_dir": f"{ref_prefix}_{calib_period}_{target_res}_{domain}_lns.nc",
            # Statistics
            "frcst_climatology": f"{frcst_prefix}_climatology_{calib_period}_{month:02d}_0320_{domain}.nc",
            "ref_climatology": f"{frcst_prefix}_climatology_{calib_period}_{domain}.nc",
        }
    else:
        fnme_dict = {
            # Forecast
            "frcst_low_glob_dir": f"ECMWF_SEAS5_*_{year}{month:02d}.nc",
            "frcst_low_reg_dir": f"{frcst_var_prefix}_{year}{month:02d}_O320_{domain_config['prefix']}.nc",
            "frcst_high_reg_dir": f"{frcst_var_prefix}_{year}{month:02d}_{target_res}_{domain}.nc",
            "frcst_high_reg_lnch_dir": f"{frcst_var_prefix}_{year}_{month:02d}_{target_res}_{domain}_lns.nc",
            # "frcst_high_reg_lnch_dir": f"{frcst_var_prefix}_{month:02d}_{target_res}_{domain}_lns.zarr",
            "frcst_high_reg_lnch_calib_dir": f"{frcst_var_prefix}_{calib_period}_{month:02d}_{target_res}_{domain}_lns.nc",
            "frcst_high_reg_bcsd_daily_dir": f"{pp_var_prefix}_daily_{year}{month:02d}_{target_res}_{domain}.nc",
            "frcst_high_reg_bcsd_daily_lnch_dir": f"{pp_var_prefix}_daily_{year}{month:02d}_{target_res}_{domain}.nc",
            # Ref
            "ref_low_glob_dir": f"{ref_prefix}_daily_{variable}_{year}.nc",
            "ref_low_reg_dir": f"{ref_prefix}_{variable}_{year}_{domain_config['prefix']}.nc",
            "ref_high_reg_daily_dir": f"{ref_prefix}_{variable}_{year}_{target_res}_{domain}.nc",
            "ref_high_reg_daily_lnch_dir": f"{ref_prefix}_{variable}_{year}_{target_res}_{domain}_lns.nc",
            "ref_high_reg_daily_lnch_calib_dir": f"{ref_prefix}_{variable}_{calib_period}_{target_res}_{domain}_lns.nc",
            # Statistics
            "frcst_climatology": f"{frcst_prefix}_climatology_{variable}_{calib_period}_{month:02d}_0320_{domain}.nc",
            "ref_climatology": f"{ref_prefix}_climatology_{variable}_{calib_period}_{domain}.nc",
        }

    fnme_dict[
        "frcst_high_zarr"
    ] = f"{frcst_prefix}_{month:02d}_{target_res}_{domain}.zarr"
    fnme_dict[
        "frcst_high_lns_zarr"
    ] = f"{frcst_prefix}_{month:02d}_{target_res}_{domain}_lns.zarr"

    return fnme_dict
