import xarray as xr
import dask
import logging

import helper_modules

@dask.delayed
def day2mon_seas(domain_config: dict,variable_config: dict, reg_dir_dict: dict, year: int, month: int, variable: str):
    # Get SEAS5-raw-Filename
    fle_in = f"{domain_config['raw_forecasts']['prefix']}_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    full_in = f"{reg_dir_dict['raw_forecasts_target_resolution_dir']}{fle_in}"

    # open-File
    ds = xr.open_dataset(full_in)
    ds = xr.open_mfdataset(
        full_in,
        parallel=True,
        chunks={"time": len(ds.time), 'ens': len(ds.ens), 'lat': "auto", 'lon': "auto"},
        # chunks={"time": 50},
        engine="netcdf4",
        autoclose=True,
    )

    # Monthly mean
    ds = ds.resample(time="1MS").mean(dim="time")
    # ds = ds.groupby("time.month").mean()

    coords = {
        "time": ds["time"].values,
        "ens": ds["ens"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = helper_modules.set_encoding(variable_config, coords, "lines")

    # set output files
    fle_out = f"{domain_config['raw_forecasts']['prefix']}_mon_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['monthly_dir']}/{fle_out}"

    try:
        ds.to_netcdf(full_out, encoding={variable: encoding[variable]})
        logging.info(
            f"Day to month: Convert day to month for {year}-{month:02d} successful"
        )
    except:
        logging.info(f"Day to month: Something went wrong for {year}-{month:02d}")


    # monthly mean by using cdo
    # cmd = (
    #    "cdo",
    #    "-O",
    #    "-f",
    #    "nc4c",
    #    "-z",
    #    "zip_6",
    #    "monmean",
    #    str(full_in),
    #    str(full_out),
    #)
    #try:
    #    os.path.isfile(full_in)
    #    run_cmd(cmd)
    #except:
    #    logging.error(f"Day to month: file {full_in} not available")





    # monthly mean by using cdo
    # cmd = (
    #    "cdo",
    #    "-O",
    #    "-f",
    #    "nc4c",
    #    "-z",
    #    "zip_6",
    #    "monmean",
    #    str(full_in),
    #    str(full_out),
    #)
    #try:
    #    os.path.isfile(full_in)
    #    run_cmd(cmd)
    #except:
    #    logging.error(f"Day to month: file {full_in} not available")



@dask.delayed
def day2mon_ref(domain_config: dict,variable_config: dict, reg_dir_dict: dict, year: int, variable: str):
    # Get ref-filename:
    file_in = f"{domain_config['reference_history']['prefix']}_{variable}_{year}_{domain_config['target_resolution']}.nc"
    full_in = f"{reg_dir_dict['reference_target_resolution_dir']}/{file_in}"

    # open-File
    ds = xr.open_dataset(full_in)
    ds = xr.open_mfdataset(
        full_in,
        parallel=True,
        chunks={"time": len(ds.time), 'lat': "auto", 'lon': "auto"},
        # chunks={"time": 50},
        engine="netcdf4",
        autoclose=True,
    )

    # Monthly mean
    ds = ds.resample(time="1MS").mean(dim="time")
    # ds = ds.groupby("time.month").mean()

    coords = {
        "time": ds["time"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = helper_modules.set_encoding(variable_config, coords, "lines")

    # set output files
    fle_out = f"{domain_config['reference_history']['prefix']}_mon_{variable}_{year}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['monthly_dir']}/{fle_out}"

    try:
        ds.to_netcdf(full_out, encoding={variable: encoding[variable]})
        logging.info(
            f"Day to month REF: Convert day to month for {year}- successful"
        )
    except:
        logging.info(f"Day to month REF: Something went wrong for {year}-")

@dask.delayed
def day2mon_bcsd(domain_config: dict,variable_config: dict, reg_dir_dict: dict, year: int, month: int, variable: str):
    # Get BCSD-Filename pp_full
    (raw_full, pp_full, refrcst_full, ref_full,) = helper_modules.set_input_files(domain_config, reg_dir_dict, month, year, variable)
    print(pp_full)
    # set input files
    full_in = pp_full

    # open-File
    ds = xr.open_dataset(
        full_in,
        #parallel=False,
        #chunks={"time": len(ds.time), 'ens': len(ds.ens), 'lat': "auto", 'lon': "auto"},
        engine="netcdf4",
        autoclose=True,
    )

    # Monthly mean
    ds = ds.resample(time="1MS").mean(dim="time")
    # ds = ds.groupby("time.month").mean()

    coords = {
        "time": ds["time"].values,
        "ens": ds["ens"].values,
        "lat": ds["lat"].values.astype(np.float32),
        "lon": ds["lon"].values.astype(np.float32),
    }

    encoding = helper_modules.set_encoding(variable_config, coords, "lines")

    # set output files
    fle_out = f"{domain_config['bcsd_forecasts']['prefix']}_v{domain_config['version']}_mon_{variable}_{year}{month:02d}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['monthly_dir']}/{fle_out}"

    try:
        ds.to_netcdf(
           full_out,                    
           encoding={
              variable: encoding[variable], 
              'ens': encoding['ens'],
              'time': encoding['time'],
              'lat': encoding['lat'],
              'lon': encoding['lon']
           }
        )
        logging.info(
            f"Day to month: Convert day to month for {year}-{month:02d} successful"
        )
    except:
        logging.info(f"Day to month: Something went wrong for {year}-{month:02d}")


    # monthly mean by using cdo
    # cmd = (
    #    "cdo",
    #    "-O",
    #    "-f",
    #    "nc4c",
    #    "-z",
    #    "zip_6",
    #    "monmean",
    #    str(full_in),
    #    str(full_out),
    #)
    #try:
    #    os.path.isfile(full_in)
    #    run_cmd(cmd)
    #except:
    #    logging.error(f"Day to month: file {full_in} not available")

@dask.delayed
def create_climatology(domain_config: dict, variable_config: dict, reg_dir_dict: dict, syr_calib: int, eyr_calib: int, variable: str):

    # Set input File
    fle_in = f"{domain_config['reference_history']['prefix']}_{domain_config['target_resolution']}_linechunks.zarr"
    full_in = f"{reg_dir_dict['reference_zarr_dir']}{fle_in}"

    # Open dataset
    ds = xr.open_zarr(full_in, consolidated=False)
    ds = xr.open_zarr(
        full_in,
        chunks={"time": len(ds.time), "lat": 10, "lon": 10},
        consolidated=False
        # parallel=True,
        # engine="netcdf4",
    )
    # Calculate climatogloy (mean)
    ds_clim = ds[variable].groupby("time.month").mean("time")
    ds_clim = ds_clim.rename({"month": "time"})
    # set encoding
    coords = {
        "time": ds_clim["time"].values,
        "lat": ds_clim["lat"].values.astype(np.float32),
        "lon": ds_clim["lon"].values.astype(np.float32),
    }
    encoding = helper_modules.set_encoding(variable_config, coords, "lines")

    fle_out  = f"{domain_config['reference_history']['prefix']}_clim_{variable}_{syr_calib}_{eyr_calib}_{domain_config['target_resolution']}.nc"
    full_out = f"{reg_dir_dict['climatology_dir']}/{fle_out}"

    # Save NC-File
    try:
        ds_clim.to_netcdf(full_out, encoding={variable: encoding[variable]},)
        logging.info(
            f"Calculate climatology of Ref: Climatology for variable suceeded!")
    except:
        logging.error(
            f"Calculate climatology of Ref: Climatology for variable failed!")