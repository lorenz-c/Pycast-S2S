# import packages
import datetime as dt

import numpy as np


def set_metadata(window_obs, window_mdl):
    # Empty Dictionary
    dtainfo = {}
    dtainfo["title"] = "Bias-corrected SEAS5-forecasts - version 2.1"
    dtainfo["Conventions"] = "CF-1.8"
    dtainfo["references"] = "TBA"
    dtainfo[
        "institution"
    ] = "Karlsruhe Institute of Technology - Institute of Meteorology and Climate Research"
    dtainfo["source"] = "ECMWF SEAS5, ERA5 Land"
    dtainfo["comment"] = (
        "Window length: "
        + str(window_obs)
        + " days (obs) and "
        + str(window_mdl)
        + " days (mdl); computation of quantiles using the quantile.m-function"
    )
    now = dt.datetime.now()
    now = now.strftime("%d/%m/%Y %H:%M:%S")
    dtainfo["history"] = now + ": BCSD applied to SEAS5 data"
    dtainfo["Contact_person"] = "Christof Lorenz (Christof.Lorenz@kit.edu)"
    dtainfo["Author"] = "Christof Lorenz (Christof.Lorenz@kit.edu)"
    dtainfo["License"] = "For non-commercial use only"
    dtainfo["date_created"] = now

    vars = ["tp", "t2m", "t2plus", "t2minus", "ssrd"]

    varlong = [
        "total_precipitation",
        "2m_temperature",
        "tmax_minus_tmean",
        "tmean_minus_tmin",
        "surface_solar_radiation",
    ]

    varstandard = [
        "precipitation_flux",
        "air_temperature",
        "air_temperature",
        "air_temperature",
        "surface_solar_radiation",
    ]

    units = ["mm/day", "K", "K", "K", "W m-2"]

    # Für Python umschreiben
    varprec = [np.int32, np.int16, np.int16, np.int16, np.int32]

    varfill = [-9999, -9999, -9999, -9999, -9999]
    varscale = [0.01, 0.01, 0.01, 0.01, 0.01]
    varoffset = [[], 273.15, 0, 0, []]

    return (
        dtainfo,
        vars,
        varlong,
        units,
        varprec,
        varfill,
        varscale,
        varoffset,
        varstandard,
    )
