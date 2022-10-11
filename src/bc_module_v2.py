# import packages
import numpy as np
from scipy.interpolate import interp1d
from scipy.stats import gumbel_l
from scipy.stats import norm
import surpyval as surv

def bc_module(pred, obs, mdl, bc_params, precip):
    
    #bc_params = {
    #    'dry_thresh': 0.01,
    #    'precip': True,
    #    'low_extrapol': "delta_additive",
    #    'up_extrapol': "delta_additive",
    #    'extremes': "weibull",
    #    'intermittency': True,
    #    'nquants': 2500
    #}
    
    
    
    
    #nts = len(pred.time.values)
        
    print(mdl)
    
    ds_nan = pred.copy()
    ds_nan[:] = np.nan
    

    # only do the bc-calculation, if obs and mdl are not NAN

    if np.any(~np.isnan(obs)) & np.any(~np.isnan(mdl)):
        # nmdl = mdl.shape[0]
        nmdl = bc_params['nquants']
        p_min_mdl = 1 / (nmdl + 1)
        p_max_mdl = nmdl / (nmdl + 1)
        p_mdl = np.linspace(p_min_mdl, p_max_mdl, nmdl)
        q_mdl = np.nanquantile(mdl, p_mdl, interpolation='midpoint')

        # obs quantile
        nobs = obs.shape[0]
        p_min_obs = 1 / (nobs + 1)
        p_max_obs = nobs / (nobs + 1)
        p_obs = np.linspace(p_min_obs, p_max_obs, nobs)
        q_obs = np.nanquantile(obs, p_obs, interpolation='midpoint')

        # Interpolate
        # Remove the dublicate values
        q_mdl, ids_mdl = np.unique(q_mdl, return_index=True)
        p_mdl = p_mdl[ids_mdl]

        pred = pred.copy()
        pred[pred > max(q_mdl)] = max(q_mdl)
        pred[pred < min(q_mdl)] = min(q_mdl)

        # if len(q_mdl)>1 and ~np.isnan(q_mdl.item(0)):
        # Transform the predictions to the rank space
        # from scipy.interpolate import interp1d
        Y_pred = interp1d(q_mdl, p_mdl)(pred)
        # else:
        # create nan-array with size, that match pred and contains nan

        # Y_pred = ds_nan

        q_obs, ids_obs = np.unique(q_obs, return_index=True)
        p_obs = p_obs[ids_obs]

        # if len(q_obs)>1 and ~np.isnan(q_obs.item(0)):
        # Transform the predictions to the rank space
        # from scipy.interpolate import interp1d
        # Y_pred = interp1d(q_obs,p_obs)(pred)
        # else:
        # Y_pred = pred

        pred_corr = interp1d(p_obs, q_obs, bounds_error=False)(Y_pred)

        # else:
        # pred_corr = ds_nan

        if precip == True:
            # print("True")
            p_dry_obs = len(np.where(obs < bc_params['dry_thresh'])[0]) / len(obs)
            p_dry_mdl = len(np.where(mdl < bc_params['dry_thresh'])[0]) / len(mdl)
            # print(p_dry_obs, p_dry_mdl)

        # Check if any of the prediction probabilities are above or below the
        # maximum or minimum observation probabilities
        if precip == True:
            up = np.where((Y_pred > p_max_obs) & (pred > 0))[0]
            low = np.where((Y_pred < p_min_obs) & (pred > 0))[0]
            # print(low)
        else:
            up = np.where(Y_pred > p_max_obs)[0]
            low = np.where(Y_pred < p_min_obs)[0]
            # print(low)

        pred_corr = pred_corr.copy()
        
        if np.any(up):
            if bc_params['up_extrapol'] == 'constant':
                pred_corr[up] = np.max(obs)
            elif bc_params['up_extrapol'] == 'distribution':
                if precip == True:
                    # Fit an extreme-value distribution to the observations
                    # from scipy.stats import gumbel_l
                    pd = gumbel_l.fit(obs)
                    pred_corr[up] = gumbel_l.ppf(Y_pred[up], pd[0], pd[1])
                else:
                    # from scipy.stats import norm
                    [MUHAT, SIGMAHAT] = norm.fit(obs)
                    pred_corr[up] = norm.ppf(Y_pred[up], MUHAT, SIGMAHAT)

            elif bc_params['up_extrapol'] == 'delta_additive':
                delta = np.quantile(obs, p_max_obs, interpolation='midpoint') - np.quantile(mdl, p_max_obs,
                                                                                            interpolation='midpoint')
                pred_corr[up] = pred[up] + delta

            elif bc_params['up_extrapol'] == 'delta_scaling':
                delta = np.quantile(obs, p_max_obs, interpolation='midpoint') / np.quantile(mdl, p_max_obs,
                                                                                            interpolation='midpoint')
                pred_corr[up] = pred[up] * delta

        if np.any(low):
            if bc_params['low_extrapol'] == 'constant':
                pred_corr[low] = np.min(obs)
            elif bc_params['low_extrapol'] == 'distribution':
                if precip == True:
                    # Fit an extreme-value distribution to the observations
                    # There is a huge problem with packages for Weibull-Distribution in Matlab. The scipy.stats.weibull_min performs poor, maybe due to a different optimizer.
                    # Use instead Packages like: surpyval, or reliability
                    # import surpyval as surv
                    # from surpyval import Weibull
                    model = surv.Weibull.fit(obs[obs > 0])
                    pd = [model.alpha, model.beta]
                    # pred_corr[low] = surv.Weibull.qf(Y_pred[low], alpha, beta)
                else:
                    # from scipy.stats import norm
                    [MUHAT, SIGMAHAT] = norm.fit(obs)
                    pred_corr[low] = norm.ppf(Y_pred[low], MUHAT, SIGMAHAT)
            elif bc_params['low_extrapol'] == 'delta_additive':
                delta = np.quantile(obs, p_min_obs, interpolation='midpoint') - np.quantile(mdl, p_min_obs,
                                                                                            interpolation='midpoint')
                pred_corr[low] = pred[low] + delta
            elif bc_params['low_extrapol'] == 'delta_scaling':
                delta = np.quantile(obs, p_min_obs, interpolation='midpoint') / np.quantile(mdl, p_min_obs,
                                                                                            interpolation='midpoint')
                pred_corr[low] = pred[low] * delta

                # Intermittency correction for precipitation
        if precip == True:

            # Set the precipitation values with lower probabilities than the
            #  dry-day probability of the observations to 0.
            pred_corr[Y_pred <= p_dry_obs] = 0

            if bc_params['intermittency'] == True:

                # Search for dry days in the predictions
                zero_pred = np.where(pred < bc_params['dry_thresh'])[0]

                if p_dry_obs >= p_dry_mdl:
                    # If the dry-day probability of the observations is higher than
                    # the model, set the corresponding forecast values to 0
                    pred_corr[zero_pred] = 0
                else:
                    # If the dry-day probability of the model is higher than the
                    # observations, do some magic...
                    if p_dry_mdl > 0:
                        # First, draw some uniform random samples between 0 and the
                        # dry-day probability of the model
                        zero_smples = p_dry_mdl * np.random.rand(len(zero_pred))
                        # Transform these random samples to the data space
                        if bc_params['extremes'] == 'weibull':
                            # if len(q_obs)>1 and ~np.isnan(q_obs.item(0)):
                            zero_corr = interp1d(p_obs, q_obs, bounds_error=False)(zero_smples)
                            ######################
                            # Erstmal draußen lassen, brauchen wir erstmal nicht gibt auch kein Plug&Play für "icdf" in Python
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

        pred_corr = ds_nan

    return pred_corr