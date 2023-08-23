import xarray as xr
import netCDF4 as nc
import scipy as sp
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
from glob import glob
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
from scipy.signal import argrelextrema
from cftime import DatetimeNoLeap
import seaborn; seaborn.set()
import dask as dask
import sys
import functools as ft
import datetime

from scipy import signal
import numpy.polynomial.polynomial as poly
from netCDF4 import Dataset
#################################
before = pd.read_pickle('/home/hollythomas/data/ERA/df_40d_beforeevent_u10_1959-2021.pkl').drop(columns = {'ssw','count','latitude','event_id'})
before['time'] = pd.to_datetime(before.time)
before = before.set_index(before['time']).rename(columns = {'u':'ub4'})


after = pd.read_pickle('/home/hollythomas/data/ERA/df_40d_afterevent_u10_1959-2021.pkl').drop(columns = {'ssw','latitude','length','day','count','event_id'}).rename(columns = {'dist_to_event':'dist_to_event_2'})
after['time'] = pd.to_datetime(after.time)
after = after.set_index(after['time'])

both = [before,after]

merged = pd.concat(both, axis = 1).drop(columns = {'index','time'})

merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
merged['u'] = merged['u'].fillna(merged['ub4'])
merged = merged.drop(columns = {'dist_to_event_2'}).reset_index().drop(columns = 'ub4')
# # ###############
# # #dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r1i1p1f1/day/ta/gn' 
# dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/ta/gn'
# dfs = sorted(glob(f'{dpath}/*.nc'))
# # # glob retrieves files that match this pattern, sorts them 
# dask.config.set(**{'array.slicing.split_large_chunks': False})
# df = xr.open_mfdataset(dfs, combine='by_coords')
df['time'] = df['time'].astype("datetime64[ns]")
#df['ta'] = df['ta'].astype("float")
#just make it the right format

PC_mask = (df['lat'] >= 60)
PL_mask = (df['plev'] == 1e+04)
df = df.where(PC_mask, drop = True)
df = df.where(PL_mask, drop = True)
#get only the polar cap and the pressure levels (100,50,10mb)
df = df.mean(('lat','lon'))
#calculate the mean accross lat and lon dimenions, should now be time, plev
clima = df.groupby('time.dayofyear').mean()
clima = clima.to_dataframe().to_pickle('/home/hollythomas/data/t_1000_for_comp.pkl')

df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
#get the average anomaly of the polar cap 
df_ssw = df.where(df.time.isin(merged['time']), drop = True).drop_vars(['lat_bnds','lon_bnds'])
#time df down to just days within the 10 day window around ssws 
df_ssw = df_ssw.to_dataframe().to_pickle('/home/hollythomas/data/CESM_CAM/ta_anom_80daywindow_ssws_1950-2021.pkl')#.rename(columns = {'t':'10'}).drop(columns = "dayofyear")

########################################################
#df = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/t_anom_80daywindow_ssws_1950-2021.pkl').drop(columns = {'dayofyear'})
df = pd.read_pickle('/home/hollythomas/data/CESM_CAM/ta_anom_80daywindow_ssws_1950-2021.pkl').drop(columns = {'dayofyear'})
df1= df.unstack()
df1.columns = df1.columns.map('{0[0]}_{0[1]}'.format)
df1 = df1.reset_index().rename(columns = {'ta_10000.0':'t100','ta_5000.0':'t50','ta_1000.0':'t10'})
df1 = df1.merge(merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'u'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
df1 = df1.groupby('dist_to_event').mean()
print(df1)
fig = df1.plot.line()
plt.show()	
exit(1)