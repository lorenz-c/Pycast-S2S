# import packages

import dask
import numpy as np
import xarray as xr

# import python-files
# from parameter_file import *
# from ind2sub import ind2sub
from bc_module import bc_module


# def slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl, ds_pred, coordinates, queue_out, window_obs,
# dry_thresh, precip, low_extrapol, up_extrapol, extremes, intermittency, k):
def slice_and_correct(
    dayofyear_obs,
    dayofyear_mdl,
    ds_obs,
    ds_mdl,
    ds_pred,
    coordinates,
    window_obs,
    dry_thresh,
    precip,
    low_extrapol,
    up_extrapol,
    extremes,
    intermittency,
    k,
):
    # queue_out["time_step"] = k
    # Fill in np.nan for the data
    # queue_out['data'] = np.full([len(coordinates['nens']), len(coordinates['lat']), len(coordinates['lon'])],
    # np.nan)

    day = dayofyear_mdl[k]  # initial day for Month 04

    # create range +- pool
    day_range = (np.arange(day - window_obs, day + window_obs + 1) + 365) % 365 + 1

    # Find days in day_range in dayofyear from obs
    intersection_day_obs = np.in1d(dayofyear_obs, day_range)
    intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)

    # Cut out obs, which correspond to intersected days
    ds_obs_sub = ds_obs.loc[dict(time=intersection_day_obs)]

    # Silence warning of slicing dask array with chunk size
    dask.config.set({"array.slicing.split_large_chunks": False})

    # Cut out mdl, which correspond to intersected days
    ds_mdl_sub = ds_mdl.loc[dict(time=intersection_day_mdl)]
    # Stack "ens" and "time" dimension
    ds_mdl_sub = ds_mdl_sub.stack(ens_time=("ens", "time"))

    print(ds_mdl_sub)
    print(ds_obs_sub)

    print(type(ds_mdl_sub.ens_time.values[0]), type(ds_obs_sub.time.values[0]))

    # Select pred
    ds_pred_sub = ds_pred.isel(time=k)

    print(ds_pred_sub)

    # This is where the magic happens:
    # apply_u_func apply the function bc_module over each Lat/Lon-Cell, processing the whole time period
    pred_corr_test = xr.apply_ufunc(
        bc_module,
        ds_pred_sub,
        ds_obs_sub,
        ds_mdl_sub,
        extremes,
        low_extrapol,
        up_extrapol,
        precip,
        intermittency,
        dry_thresh,
        input_core_dims=[["ens"], ["time"], ["ens_time"], [], [], [], [], [], []],
        output_core_dims=[["ens"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[np.float64],
    )  # , exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51}) #,  exclude_dims=set(("quantile",)))
    # pred_corr_test = xr.apply_ufunc(bc_module, ds_pred.load(), ds_obs_sub.load(), ds_mdl_sub.load(), extremes,
    # low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"],
    # ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize =
    # True, output_dtypes=[np.float64]).compute() # , exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51})
    # #,  exclude_dims=set(("quantile",)))

    # Fill NaN-Values with corresponding varfill, varscale and varoffset
    # if varoffset[v] != []:
    #    pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v] + varoffset[v])  # this is needed, because
    # otherwise the conversion of np.NAN to int does not work properly
    # else:
    #   pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v])
    #    queue_out['data'] = pred_corr_test.values

    # Run write_output.ipynb
    # write_output(queue_out)

    return pred_corr_test
