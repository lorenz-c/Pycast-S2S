# import packages
import xarray as xr
import pandas as pd
import numpy as np
import datetime as dt
# import python-files
from set_metadata import set_metadata
from create_4d_netcdf import create_4d_netcdf
# from parameter_file import *
# from ind2sub import ind2sub
from bc_module import bc_module
from write_output import write_output
import dask.array as da
import dask

def io_module(obs, mdl, pred, month, fnme_out, window_obs, window_mdl):
    create_new = True

    # Run set_metadata
    (dtainfo, vars, varlong, units, varprec, varfill, varscale, varoffset, varstandard) = set_metadata(window_obs,
                                                                                                       window_mdl)

    # Read lat, lon
    # ds = xr.open_dataset("test_files/obs_struct/ERA5_Land_daily_tp_1981_2016_Khuzestan_lns.nc") # further work has to be done
    ds = xr.open_dataset(list(obs.values())[0])
    lat = ds['lat'].values
    lon = ds['lon'].values
    # Read time
    # ds = xr.open_dataset("test_files/mdl_struct/SEAS5_daily_1981_2016_04_0.1_Khuzestan_lns.nc") # further work has to be done
    ds = xr.open_dataset(list(mdl.values())[0])
    frcsts = ds['time'].values

    npts = len(lat) * len(lon)
    nts = len(frcsts)

    # ds = xr.open_dataset("test_files/obs_struct/ERA5_Land_daily_tp_1981_2016_Khuzestan_lns.nc", decode_times=False) # further work has to be done
    ds = xr.open_dataset(list(obs.values())[0], decode_times=False)
    tme = ds['time'].values
    tme_unit = ds['time'].units
    # ds = xr.open_dataset("test_files/obs_struct/ERA5_Land_daily_tp_1981_2016_Khuzestan_lns.nc", decode_times=True)
    ds = xr.open_dataset(list(obs.values())[0], decode_times=True)
    tme_abs = ds['time']

    # Create Dataframe and split Date into years, month, day, hour, minute, second
    df_tme_abs = pd.DataFrame({'Date': tme_abs})
    # split year, month, day, hour, minute, sec in column
    df_tme_abs['year'] = df_tme_abs['Date'].dt.year
    df_tme_abs['month'] = df_tme_abs['Date'].dt.month
    df_tme_abs['day'] = df_tme_abs['Date'].dt.day
    df_tme_abs['hour'] = df_tme_abs['Date'].dt.hour
    df_tme_abs['min'] = df_tme_abs['Date'].dt.minute
    df_tme_abs['sec'] = df_tme_abs['Date'].dt.second

    # Get a list with the years of absolute date
    yrs = np.unique(ds.time.dt.year.values)

    # ds = xr.open_dataset("test_files/raw_lnechnks/SEAS5_daily_202204_0.1_Khuzestan_lns.nc", decode_times=False) # further work has to be done
    ds = xr.open_dataset(list(pred.values())[0], decode_times=False)
    tme_frcst = ds['time'].values
    tme_frcst_unit = ds['time'].units
    # ds = xr.open_dataset("test_files/raw_lnechnks/SEAS5_daily_202204_0.1_Khuzestan_lns.nc", decode_times=True)
    ds = xr.open_dataset(list(pred.values())[0], decode_times=True)
    tme_frcst_abs = ds['time']

    # Create Dataframe and split Date into years, month, day, hour, minute, second
    df_tme_frcst_abs = pd.DataFrame({'Date': tme_frcst_abs})
    # split year, month, day, hour, minute, sec in column
    df_tme_frcst_abs['year'] = df_tme_frcst_abs['Date'].dt.year
    df_tme_frcst_abs['month'] = df_tme_frcst_abs['Date'].dt.month
    df_tme_frcst_abs['day'] = df_tme_frcst_abs['Date'].dt.day
    df_tme_frcst_abs['hour'] = df_tme_frcst_abs['Date'].dt.hour
    df_tme_frcst_abs['min'] = df_tme_frcst_abs['Date'].dt.minute
    df_tme_frcst_abs['sec'] = df_tme_frcst_abs['Date'].dt.second

    if df_tme_frcst_abs['year'][0] < 2017:
        nens = 25
    else:
        nens = 51

    # Initialize a queue for writing the output
    # queue = parallel.pool.DataQueue;
    # afterEach(queue, @write_output);

    queue_out = {}
    # Set the filename of the output
    queue_out['fnme'] = fnme_out
    queue_out['nts'] = nts
    queue_out['nens'] = nens

    ## Create a status map to count the processed pixels
    # status_map = zeros(length(lon), length(lat));

    # Create a NetCDF-File for the output
    if create_new == True:
        create_4d_netcdf(fnme_out,
                         dtainfo,
                         vars,
                         varstandard,
                         varlong,
                         units,
                         varprec,
                         varfill,
                         varscale,
                         varoffset,
                         tme_frcst,
                         lat,
                         lon,
                         tme_frcst_unit,
                         'ensemble',
                         "",
                         "",
                         '[]',
                         np.arange(0, nens),
                         nens,
                         [nts, nens, 1, 1],
                         True)

    start_time = dt.datetime.now()

    for v in range(0, 1):
        # spmd

        print("Variable: " + vars[v])

        queue_out['varnme'] = vars[v]

        # load data:
        # load data as dask objects
        # Obs (1981 - 2016 on daily basis)
        ds_obs = xr.open_mfdataset(list(obs.values())[v])
        ds_obs = ds_obs[vars[v]]
        # Mdl (historical, 1981 - 2016 for one month and 215 days)
        ds_mdl = xr.open_mfdataset(list(mdl.values())[v])
        ds_mdl = ds_mdl[vars[v]]
        # Pred (current year for one month and 215 days)
        ds_pred = xr.open_mfdataset(list(pred.values())[v])
        ds_pred = ds_pred[vars[v]]

        ######## Combine years and days in one time variable ############
        # Select variable
        arr_var = ds_mdl
        year_start = arr_var.year.values[0].astype(int)
        year_end = arr_var.year.values[-1].astype(int)
        nday = len(arr_var.time.values)
        nyear = len(arr_var.year.values)

        # Create new time based on day and year
        arr_date = da.empty(shape=0, dtype=np.datetime64)
        for yr in range(year_start, year_end + 1):
            date = np.asarray(pd.date_range(str(yr) + "-4-1 00:00:00", freq="D", periods=nday))
            arr_date = da.append(arr_date, date)

        # Stack time and year to one dimension in order to create datetime
        arr_var = arr_var.stack(date=('year', 'time'))

        # Assign Datetime-Object to new Date coordinate and rename it to "time"
        arr_var = arr_var.assign_coords(date=arr_date)
        arr_var = arr_var.rename(date="time")
        ds_mdl = arr_var
        ds_mdl_persist = ds_mdl.persist()  # load data in memory, so that it can be computed much faster

        # Find out which indices correspond to which time between obs and mdl
        time_obs = ds_obs.time.values
        time_mdl = ds_mdl_persist.time.values
        intersection, ind_obs, ind_mdl = np.intersect1d(time_obs, time_mdl, return_indices=True)

        # Calculate day of the year from time variable
        dayofyear_obs = ds_obs['time.dayofyear']
        dayofyear_mdl = ds_mdl_persist['time.dayofyear']

        def slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl_persist, ds_pred, nens, queue_out, window_obs, k):
            queue_out["time_step"] = k

            day = dayofyear_mdl[k]  # initial day for Month 04

            # create range +- pool
            day_range = (np.arange(day - window_obs, day + window_obs + 1) + 365) % 365 + 1
            # Find days in day_range in dayofyear from obs
            intersection_day_obs = np.in1d(dayofyear_obs, day_range)
            intersection_day_mdl = np.in1d(dayofyear_mdl, day_range)

            # Cut out obs, which correspond to intersected days
            ds_obs_sub = ds_obs.loc[dict(time=intersection_day_obs)]
            # Rechunk latitude and longitude
            # ds_obs_sub = ds_obs_sub.chunk({'lat': 'auto', 'lon': 'auto'})

            # Silence warning of slicing dask array with chunk size
            dask.config.set({"array.slicing.split_large_chunks": False})
            # Cut out mdl, which correspond to intersected days
            ds_mdl_sub = ds_mdl_persist.loc[dict(time=intersection_day_mdl)]
            # Stack "ens" and "time" dimension
            ds_mdl_sub = ds_mdl_sub.stack(ens_time=("ens", "time"))
            # Rechunk latitude and longitude
            ds_mdl_sub = ds_mdl_sub.chunk({'lat': 'auto', 'lon': 'auto'})

            # Select pred
            ds_pred_sub = ds_pred.isel(time=k)
            # Change data type of latidude and longitude, otherwise apply_u_func does not work
            ds_pred_sub = ds_pred_sub.assign_coords(lon=ds_pred.lon.values.astype(np.float32))
            ds_pred_sub = ds_pred_sub.assign_coords(lat=ds_pred.lat.values.astype(np.float32))

            queue_out['data'] = np.full([nens, 70, 85], np.nan)

            # Some IF Statements needed!!!!

            if vars[v] == "tp":
                dry_thresh = 0.01
                precip = True
                low_extrapol = "delta_additive"
                up_extrapol = "delta_additive"
                extremes = "weibull"
                intermittency = True
            else:
                dry_thresh = 0.01
                precip = False
                low_extrapol = "delta_additive"
                up_extrapol = "delta_additive"
                extremes = "weibull"
                intermittency = False

            pred_corr_test = xr.apply_ufunc(bc_module, ds_pred_sub, ds_obs_sub, ds_mdl_sub, extremes, low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"], ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize=True, dask="parallelized", output_dtypes=[np.float64])  # , exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51}) #,  exclude_dims=set(("quantile",)))
            # pred_corr_test = xr.apply_ufunc(bc_module, ds_pred.load(), ds_obs_sub.load(), ds_mdl_sub.load(), extremes, low_extrapol, up_extrapol, precip, intermittency, dry_thresh, input_core_dims=[["ens"], ["time"], ['ens_time'], [], [], [], [], [], []], output_core_dims=[["ens"]], vectorize = True, output_dtypes=[np.float64]).compute() # , exclude_dims=set(("ens",)), output_sizes={'dim':0, 'size':51}) #,  exclude_dims=set(("quantile",)))

            # Fill NaN-Values with corresponding varfill, varscale and varoffset
            if varoffset[v] != []:
                pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v] + varoffset[v])  # this is needed, because otherwise the conversion of np.NAN to int does not work properly
            else:
                pred_corr_test = pred_corr_test.fillna(varfill[v] * varscale[v])
            queue_out['data'] = pred_corr_test.values

            # Run write_output.ipynb
            write_output(queue_out)
            print('## Update NetCDF4-File completed')

        dask_allow = False
        dask_jobs = []
        for k in range(nts):
            print('# Time step ' + str(k+1) + ' / ' + str(nts))
            if dask_allow == True:
                test_jobs = dask.delayed(slice_and_correct)(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl_persist, ds_pred, nens,
                                                   queue_out, k)
                dask_jobs.append(test_jobs)
            else:
                slice_and_correct(dayofyear_obs, dayofyear_mdl, ds_obs, ds_mdl_persist, ds_pred, nens, queue_out, window_obs, k)
        print()
        # start_time = dt.datetime.now()
        # dask.compute(*dask_jobs)
        # end_time = dt.datetime.now()
        # calc_time = end_time - start_time
        # print("> Dask completed: " + str(calc_time.total_seconds()) + " seconds")

    return queue_out, dask_jobs