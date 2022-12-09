# Packages
import logging

import dask

from helper_modules import run_cmd


@dask.delayed
def gauss_to_regular(global_config, year, month, variable):

    flenme_grb = f"""{global_config['raw_directory']}/{year}/{month:02d}/
        {global_config['raw_forecasts']['prefix']}_daily_{variable}_{year}{month:02d}.grb"""
    flenme_nc = f"""{global_config['raw_directory']}/{year}/{month:02d}/
        {global_config['raw_forecasts']['prefix']}_daily_{variable}_{year}{month:02d}.nc"""

    cmd = (
        "cdo",
        "-O",
        "-f",
        "nc4c",
        "-z",
        "zip_6",
        "setgridtype,regular",
        str(flenme_grb),
        str(flenme_nc),
    )

    try:
        run_cmd(cmd)
        logging.info(
            f"Gauss to regular: Remapping complete for year {year} and month {month}"
        )
    except:
        logging.error(
            f"Gauss to regular: Something went wrong for year {year} and month {month}"
        )
