import xarray as xr
import netCDF4 as nc
import scipy as sp
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
from glob import glob
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
from cftime import DatetimeNoLeap
import seaborn; seaborn.set()
import dask as dask
import sys
import functools as ft

from scipy import signal
import numpy.polynomial.polynomial as poly
from netCDF4 import Dataset
################################
before = pd.read_pickle('/home/hollythomas/data/ERA/dates/df_40d_beforeevent_u10_1959-2021.pkl')
x = len(pd.unique(before['event_id']))
before = before.drop(columns = {'ssw','count','latitude','event_id'})
before['time'] = pd.to_datetime(before.time)
before = before.set_index(before['time'])

after = pd.read_pickle('/home/hollythomas/data/ERA/dates/df_40d_afterevent_u10_1959-2021.pkl').drop(columns = {'ssw','latitude','length','day','count','event_id'}).rename(columns = {'dist_to_event':'dist_to_event_2'})
after['time'] = pd.to_datetime(after.time)
after = after.set_index(after['time'])


both = [before,after]

merged = pd.concat(both, axis = 1).drop(columns = {'index','u','time'})
merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
merged = merged.drop(columns = {'dist_to_event_2'}).reset_index()

################

# dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/t/t1' 
# #dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/ua/gn'

# dfs = sorted(glob(f'{dpath}/*.nc'))
# # # glob retrieves files that match this pattern, sorts them 
# dask.config.set(**{'array.slicing.split_large_chunks': False})

# df = xr.open_mfdataset(dfs, combine='by_coords')
# #get only the polar cap 
# # def detrend_dim(da, dim, deg=1):
# #     # detrend along a single dimension
# #     p = da.polyfit(dim=dim, deg=deg)
# #     #evaluate first degree polynomial along the time dimension
# #     fit = xr.polyval(da[dim], p.polyfit_coefficients)
# #     #da[dim] = values at which to evaluate the polynomial 
# #     #p.poly_fircoefficients = coeffcients of the polynomial as calculated above
# #     return da - fit 
# # #this detrended the data for the anomaly calculation, this is not necessary for the CESM2 data. 
# # #I used the least sqaures polynomial fit
# # df = detrend_dim(df, 'time', deg=1)
# PC_mask = (df['latitude'] >= 60)
# #create polar cap mask
# df = df.where(PC_mask, drop = True).resample(time = 'D').mean()
# #get the polar cap and resample the tempresolution to daily 
# weights = np.cos(np.deg2rad(df.latitude))
# weights.name = "weights"
# df = df.weighted(weights)
# #weight the data frame
# df = df.mean(('longitude','latitude'))
# #calculate the mean of the polar cap s
# df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
# #calculate anomalies 
# #df_ssw = df.where(df.time.isin(merged['time']), drop = True)
# #time df down to just days within the 10 day window around ssws 
# df_ssw = df_ssw.to_dataframe().rename(columns = {'t':'1'}).drop(columns = "dayofyear")
# #this is the df of only the dates we're concerned about, at 60N and upwards 
# merged = df_ssw.merge(merged, how = 'inner', on = 'time').set_index('time')#.to_pickle('/home/hollythomas/data/ERA/t1_60N_1959-2021.pkl')
# #merge the dataframe with the merged dataframe so we get the daysto/from index

df1 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t1_anom_80daywindow_ssws_1959-2021.pkl')
df3 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t3_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df5 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t5_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df7 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t7_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df10 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t10_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df20 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t20_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df50 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t50_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")
df100 = pd.read_pickle('/home/hollythomas/data/ERA/ta/t100_anom_80daywindow_ssws_1959-2021.pkl').drop(columns = "dist_to_event")

dfs = [df100, df50, df20, df10, df7, df5, df3, df1]
df = ft.reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs)
df = df.groupby('dist_to_event').median()
print(df)
exit(1)
fig = df.plot.line()

plt.title('Reanalysis Events Strat Temp (u10 key) (K) anomaly: N ={}'.format(x))
plt.show()

exit(1)

######## just for ground truthing ########## 
clima = df.groupby("time.dayofyear").mean("time").to_dataframe().reset_index()
#clima = clima.to_dataframe()
climaCESM = pd.read_pickle('/home/hollythomas/data/t_1000_for_comp.pkl').reset_index().drop(columns = {'nbnd','plev','lat_bnds','lon_bnds'})
fig = climaCESM.plot.line()
plt.show()

print(climaCESM)

exit(1)
