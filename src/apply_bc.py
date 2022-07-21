import netCDF4
from io_module import io_module

def apply_bc(month=None,year=None,domain=None,regroot=None,version=None,*args,**kwargs):

    yr_str=str(year)

    mnth_str=str(month)
    mnth_str=mnth_str.zfill(2)
    basedir=f"{regroot}{domain}/daily/"

    raw_in=basedir+'seas5_h/SEAS5_daily_'+yr_str+mnth_str+'_0.1_'+domain+'.nc'
    raw_lnechnks=basedir+'linechunks/SEAS5_daily_'+yr_str+mnth_str+'_0.1_'+domain+'_lns.nc'
    bc_out_lns=basedir+'linechunks/SEAS5_BCSD_v'+version+'_daily_'+yr_str+mnth_str+'_0.1_'+domain+'_lns.nc'

    # Set the number of ensemble member
    if year < 2017:
        nrens=25
    else:
        nrens=51

    #ds = netCDF4.Dataset("test_files/SEAS5_daily_2017_2019_12_0.1_Khuzestan_lns.nc")
    #lat = ds['lat'][:]
    #lon = ds['lon'][:]

    #nlon = len(lon)
    #nlat = len(lat)

    syr_calib=1981

    eyr_calib=2016

    obs_struct = {'tp': basedir+'linechunks/ERA5_Land_daily_tp_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
             't2m': basedir+'linechunks/ERA5_Land_daily_t2m_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
             't2plus': basedir+'linechunks/ERA5_Land_daily_t2plus_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
             't2minus': basedir+'linechunks/ERA5_Land_daily_t2minus_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc',
             'ssrd': basedir+'linechunks/ERA5_Land_daily_ssrd_'+str(syr_calib)+'_'+str(eyr_calib)+'_'+domain+'_lns.nc'}

    mdl_struct = {'tp': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
             't2m': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
             't2plus': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
             't2minus': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc',
             'ssrd': basedir+'linechunks/SEAS5_daily_1981_2016_'+mnth_str+'_0.1_'+domain+'_lns.nc'}

    pred_struct = {'tp': raw_lnechnks,
             't2m': raw_lnechnks,
             't2plus': raw_lnechnks,
             't2minus': raw_lnechnks,
             'ssrd': raw_lnechnks}
# apply_bc.m:62

    
    # FOR TEST RUN:
    #bc_out_lns = "test_py.nc"

    #obs_struct = {'tp': 'test_files/obs_struct/ERA5_Land_daily_tp_1981_2016_Khuzestan_lns.nc',
    #              't2m': 'test_files/obs_struct/ERA5_Land_daily_t2m_1981_2016_Khuzestan_lns.nc'}
    #mdl_struct = {'tp': 'test_files/mdl_struct/SEAS5_daily_1981_2016_04_0.1_Khuzestan_lns.nc',
    #              't2m': 'test_files/mdl_struct/SEAS5_daily_1981_2016_04_0.1_Khuzestan_lns.nc'}
    #pred_struct = {'tp': 'test_files/raw_lnechnks/SEAS5_daily_202204_0.1_Khuzestan_lns.nc',
    #               't2m': 'test_files/raw_lnechnks/SEAS5_daily_202204_0.1_Khuzestan_lns.nc'}

    queue_out, dask_jobs = io_module(obs_struct, mdl_struct, pred_struct, month, bc_out_lns, 15, 15)
    return obs_struct, mdl_struct, pred_struct, queue_out, dask_jobs

