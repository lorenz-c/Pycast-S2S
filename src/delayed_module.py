import dask
import numpy as np
import xarray as xr

#from bc_module_v3 import bc_module


def get_intersect_days(timestep, domain_config: dict, da_obs, da_mdl, da_pred):

    dayofyear_mdl = da_pred["time.dayofyear"]
    day = dayofyear_mdl[timestep]

    mdl_time = da_mdl.time
    obs_time = da_obs.time

    # Deal with normal and leap years
    for calib_year in range(domain_config["syr_calib"], domain_config["eyr_calib"] + 1):

        da_obs_year = obs_time.sel(time=obs_time.dt.year == calib_year)
        da_mdl_year = mdl_time.sel(time=mdl_time.dt.year == calib_year)

        dayofyear_obs = da_obs_year["time.dayofyear"]
        dayofyear_mdl = da_mdl_year["time.dayofyear"]

        # normal years
        if len(da_obs_year.time.values) == 365:

            day_range = (
                np.arange(
                    day - domain_config["bc_params"]["window"] - 1,
                    day + domain_config["bc_params"]["window"],
                )
                + 365
            ) % 365 + 1

            # leap years
        else:
            day_range = (
                np.arange(
                    day - domain_config["bc_params"]["window"] - 1,
                    day + domain_config["bc_params"]["window"],
                )
                + 366
            ) % 366 + 1

        intersection_day_obs_year = np.in1d(dayofyear_obs, day_range)
        intersection_day_mdl_year = np.in1d(dayofyear_mdl, day_range)

        if calib_year == domain_config["syr_calib"]:
            intersection_day_obs = intersection_day_obs_year
            intersection_day_mdl = intersection_day_mdl_year
        else:
            intersection_day_obs = np.append(
                intersection_day_obs, intersection_day_obs_year
            )
            intersection_day_mdl = np.append(
                intersection_day_mdl, intersection_day_mdl_year
            )

    return intersection_day_obs, intersection_day_mdl


def intersect_and_correct(
    timestep,
    variable,
    domain_config,
    variable_config,
    da_mdl,
    da_obs,
    da_pred,
):
    print(f"Correcting timestep {timestep}...")

    intersection_day_obs, intersection_day_mdl = get_intersect_days(
        timestep, domain_config, da_obs, da_mdl, da_pred
    )

    # with dask.config.set(**{"array.slicing.split_large_chunks": False}):
    print("Step2")
    da_obs_sub = da_obs.loc[dict(time=intersection_day_obs)]
    print("Step2..done")
    print("Step3")  # HERE!!!!!!!
    # da_mdl_sub = da_mdl.loc[dict(time=intersection_day_mdl)]
    da_mdl_sub = da_mdl.where(da_mdl["time"] == intersection_day_mdl, drop=True)
    print("Step3..done")
    print("Step4")
    da_mdl_sub = da_mdl_sub.stack(ens_time=("ens", "time"), create_index=True)
    print("Step4..done")
    print("Step5")
    da_mdl_sub = da_mdl_sub.drop("time")
    print("Step5..done")
    print("Step6")
    da_pred_sub = da_pred.isel(time=timestep)
    print("Step6..done")

    # da_obs_sub = xr.where(da_obs.time == intersection_day_obs, da_obs, None)
    # da_obs_sub = da_obs.where(da_obs['time'] == intersection_day_obs, drop=True)

    # da_mdl_sub = xr.where(da_mdl.time == intersection_day_mdl, da_mdl, None)

    # da_mdl_sub = da_mdl_sub.stack(ens_time=("ens", "time"), create_index=True)

    #

    #

    #
    # --> I really don't know why we need to silence the warning here...
    #

    #return xr.apply_ufunc(
    #    bc_module,
    #    da_pred_sub,
    #    da_obs_sub,
    #    da_mdl_sub,
    #    kwargs={
    #        "bc_params": domain_config["bc_params"],
    #        "precip": variable_config[variable]["isprecip"],
    #    },
    #    input_core_dims=[["ens"], ["time"], ["ens_time"]],
    #    output_core_dims=[["ens"]],
    #    vectorize=True,
    #    dask="parallelized",
    #    output_dtypes=[np.float64],
    #)
