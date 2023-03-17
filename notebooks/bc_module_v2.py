# import packages
import numpy as np
import surpyval as surv
from scipy.interpolate import interp1d
from scipy.stats import gumbel_l, norm
#import delayed_module
import dask

def get_intersect_days(timestep, domain_config: dict, obs_time, mdl_time, pred_time):
    
    # Get forecasted day in the reforecasts
    
    
    dayofyear_mdl = pred_time.dt.dayofyear
  #      mdl_time = da_mdl.time
  #  obs_time = da_obs.time
    
    #dayofyear_mdl = da_pred["time.dayofyear"]
    day = dayofyear_mdl[timestep]

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

def bc_module(da_pred, da_obs, da_mdl, lat_index, lon_index, domain_config, precip):
    
    # Get the BC-Parameer
    bc_params = domain_config["bc_params"]
    
    #da_pred_sub = da_pred[:, lat_index, ]
    
    #da_pred_pixel = da_pred.isel(lat=lat_index, lon=lon_index)
    #da_obs_pixel = da_obs.isel(lat=lat_index, lon=lon_index)
    #da_mdl_pixel = da_mdl.isel(lat=lat_index, lon=lon_index)
    
    #print(da_pred_sub.shape())
    #print(da_pred_sub.shape())
    #dict(lat=act_lat, lon=act_lon)].persist()

    #out = np.nan()
    #out = da_pred.copy()
    #out[:] = np.nan
    
        
    #ds_nan = da_pred_sub.copy()
    #ds_nan[:] = np.nan
    #ds_mean = ds_nan
        
   
    # Check if we have sufficient values for the bcsd
    if np.any(~np.isnan(da_obs)) and np.any(~np.isnan(da_mdl)):
            
        if len(np.unique(da_mdl)) > 10 and len(np.unique(da_obs)) > 10:

            # if np.any(~np.isnan(obs)) and np.any(~np.isnan(mdl)):
            # nmdl = mdl.shape[0]
            nmdl = bc_params["nquants"]
            p_min_mdl = 1 / (nmdl + 1)
            p_max_mdl = nmdl / (nmdl + 1)
            p_mdl = np.linspace(p_min_mdl, p_max_mdl, nmdl)
            q_mdl = np.nanquantile(da_mdl_sub, p_mdl, interpolation="midpoint")

            # obs quantile
            nobs = da_obs.shape[0]
            p_min_obs = 1 / (nobs + 1)
            p_max_obs = nobs / (nobs + 1)
            p_obs = np.linspace(p_min_obs, p_max_obs, nobs)
            q_obs = np.nanquantile(da_obs_sub, p_obs, interpolation="midpoint")

            # Interpolate
            # Remove the dublicate values
            q_mdl, ids_mdl = np.unique(q_mdl, return_index=True)
            p_mdl = p_mdl[ids_mdl]

                # print(q_mdl)

            pred = da_pred.copy()
                # pred_1 = pred.copy()

            pred[pred > max(q_mdl)] = max(q_mdl)
            pred[pred < min(q_mdl)] = min(q_mdl)

                # if len(q_mdl)>1 and ~np.isnan(q_mdl.item(0)):
                # Transform the predictions to the rank space
                # from scipy.interpolate import interp1d
            Y_pred = interp1d(q_mdl, p_mdl)(pred)
                # else:
                # create nan-array with size, that match pred and contains nan
                #     Y_pred = ds_nan

            q_obs, ids_obs = np.unique(q_obs, return_index=True)
            p_obs = p_obs[ids_obs]

                # if len(q_obs)>1 and ~np.isnan(q_obs.item(0)):
                # Transform the predictions to the rank space
                # from scipy.interpolate import interp1d
                # Y_pred = interp1d(q_obs,p_obs)(pred)
                # else:
                # Y_pred = pred

                # pred_corr = interp1d(p_obs, q_obs, fill_value='extrapolate')(Y_pred) #bounds_error=True
            pred_corr = np.interp(Y_pred, p_obs, q_obs, left=np.nan, right=np.nan)
                # else:
                # pred_corr = ds_nan

            if precip:
                    # print("True")
                p_dry_obs = len(np.where(da_obs < bc_params["dry_thresh"])[0]) / len(da_obs_sub)
                p_dry_mdl = len(np.where(da_mdl < bc_params["dry_thresh"])[0]) / len(da_mdl_sub)
                    # print(p_dry_obs, p_dry_mdl)

                # Check if any of the prediction probabilities are above or below the
                # maximum or minimum observation probabilities
            if precip:
                up = np.where((Y_pred > p_max_obs) & (pred > 0))[0]
                low = np.where((Y_pred < p_min_obs) & (pred > 0))[0]
                # print(low)
            else:
                up = np.where(Y_pred > p_max_obs)[0]
                low = np.where(Y_pred < p_min_obs)[0]
                # print(low)

            # pred_corr = pred_corr.copy()

            if up.size != 0:
                if bc_params["up_extrapol"] == "constant":
                    pred_corr[up] = np.max(da_obs)
                elif bc_params["up_extrapol"] == "distribution":
                    if precip:
                            # Fit an extreme-value distribution to the observations
                            # from scipy.stats import gumbel_l
                        pd = gumbel_l.fit(da_obs)
                        pred_corr[up] = gumbel_l.ppf(Y_pred[up], pd[0], pd[1])
                    else:
                        #    from scipy.stats import norm
                        [MUHAT, SIGMAHAT] = norm.fit(da_obs)
                        pred_corr[up] = norm.ppf(Y_pred[up], MUHAT, SIGMAHAT)

                elif bc_params["up_extrapol"] == "delta_additive":
                    delta = np.quantile(
                        da_obs, p_max_obs, interpolation="midpoint"
                    ) - np.quantile(da_mdl, p_max_obs, interpolation="midpoint")
                    pred_corr[up] = pred[up] + delta

                elif bc_params["up_extrapol"] == "delta_scaling":
                    delta = np.quantile(
                        da_obs, p_max_obs, interpolation="midpoint"
                    ) / np.quantile(da_mdl, p_max_obs, interpolation="midpoint")
                    pred_corr[up] = pred[up] * delta

            if up.size != 0:
                if bc_params["low_extrapol"] == "constant":
                    pred_corr[low] = np.min(da_obs)
                elif bc_params["low_extrapol"] == "distribution":
                    if precip:
                            # Fit an extreme-value distribution to the observations
                            # There is a huge problem with packages for Weibull-Distribution in Matlab.
                            # The scipy.stats.weibull_min performs poor, maybe due to a different optimizer.
                            # Use instead Packages like: surpyval, or reliability
                            # import surpyval as surv
                            # from surpyval import Weibull
                        model = surv.Weibull.fit(da_obs[da_obs > 0])
                        pd = [model.alpha, model.beta]
                            # pred_corr[low] = surv.Weibull.qf(Y_pred[low], alpha, beta)
                    else:
                            # from scipy.stats import norm
                        [MUHAT, SIGMAHAT] = norm.fit(da_obs)
                    pred_corr[low] = norm.ppf(Y_pred[low], MUHAT, SIGMAHAT)
            elif bc_params["low_extrapol"] == "delta_additive":
                delta = np.quantile(
                    da_obs, p_min_obs, interpolation="midpoint"
                ) - np.quantile(da_mdl, p_min_obs, interpolation="midpoint")
                pred_corr[low] = pred[low] + delta
            elif bc_params["low_extrapol"] == "delta_scaling":
                delta = np.quantile(
                    da_obs, p_min_obs, interpolation="midpoint"
                ) / np.quantile(da_mdl, p_min_obs, interpolation="midpoint")
                pred_corr[low] = pred[low] * delta

                    # Intermittency correction for precipitation
            if precip:

                    # Set the precipitation values with lower probabilities than the
                    #  dry-day probability of the observations to 0.
                pred_corr[Y_pred <= p_dry_obs] = 0

                if bc_params["intermittency"]:
                        # Search for dry days in the predictions
                    zero_pred = np.where(pred < bc_params["dry_thresh"])[0]

                    if p_dry_obs >= p_dry_mdl:
                            # If the dry-day probability of the observations is higher than
                            # the model, set the corresponding forecast values to 0
                        pred_corr[zero_pred] = 0
                    elif p_dry_obs < p_dry_mdl:
                            # If the dry-day probability of the model is higher than the
                            # observations, do some magic...
                        if p_dry_mdl > 0:
                                # First, draw some uniform random samples between 0 and the
                                # dry-day probability of the model
                            zero_smples = p_dry_mdl * np.random.rand(len(zero_pred))
                                # Transform these random samples to the data space
                            if bc_params["extremes"] == "weibull":
                                    # if len(q_obs)>1 and ~np.isnan(q_obs.item(0)):
                                    # zero_corr = interp1d(p_obs, q_obs, bounds_error=False)(zero_smples)
                                zero_corr = np.interp(
                                    zero_smples, p_obs, q_obs, left=np.nan, right=np.nan
                                )
                                    ######################
                                    # Erstmal draußen lassen, brauchen wir erstmal nicht gibt auch kein Plug&Play für
                                    # "icdf" in Python
                                    ######################
                                    # else:
                                    # zero_corr   = icdf(Ofit, zero_smples);
                                    # else:
                                    #   zero_corr = zero_smples

                                    # Now, set all transfomed random samples with probabilities
                                    # lower than the dry day probability of the observations to
                                    # zero.
                                zero_corr[zero_smples <= p_dry_obs] = 0
                                    # Replace the elements in the predictions with the
                                    # corresponding intermittency-corrected values.
                                pred_corr[zero_pred] = zero_corr
                        else:

                            pred_corr[zero_pred] = 0
                                # If the probability of a dry day is 0 (which might happen
                                # in some very ... cases), we simply set the probabilities,
                                # which correspond to the forecasted zero values, to the
                                # minimum probability of the observations.

        else:
            ds_mean = np.nanmean(da_obs)
            pred_corr = ds_mean

    else:
        pred_corr = ds_nan
            
    
    
    return pred_corr
