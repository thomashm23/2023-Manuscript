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
sys.path.append('/home/hollythomas/python/functions')
dask.config.set(**{'array.slicing.split_large_chunks': False})
from get_events import rolling_count, intial_filter, get_major_events_ahead, get_events_behind
from get_months import is_winter
from itertools import cycle 
import functools as ft
import pylab
import scipy
from scipy.stats import norm
import statistics 
from scipy import signal
from matplotlib.colors import BoundaryNorm
import matplotlib.cm as cm
coolwarm = plt.get_cmap('coolwarm')
from datetime import timedelta
from scipy.stats import ttest_1samp
from matplotlib import colors
##############################################################################################################################
u10_wacm = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_wanom_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk').reset_index()
u10_cam = pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_wanom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk').reset_index()
u10_ERA = pd.read_pickle('/home/hollythomas/data/ERA/u/u10wanom_60N_ERA_1959-2021.pkl').reset_index()

u10_ERA = u10_ERA.rename(columns = {'u':'ua'})
u10_ERA['time'] = pd.to_datetime(u10_ERA['time'])

def before_dates(df,uf):
	u10 = df
	#input whichever of the two u files into the function to generate files 
	u10['time'] = u10['time'].astype(str).str[0:10].astype(str)
	uf = uf
	# do we want to filter by strength of reversal 
	tf = 'no'
	quantile = .5
	#remeber this is backwards since the values are (-) 
	
	df_with_count, dates, major_df = intial_filter(quantile, tf, uf, u10.copy(),(u10['ua']), 'model')
	#df_with_count is full dataframe with extra info (original length,includes time, ua, ssw and count)
	
	############
	#length filter
	############
	lf = 'no'
	#length filtering, if "yes" runs script with length filter, if "no" original processing method
	quantile = .5
	#quantile above which you'd like to extract events 
	climaafter_with_meta, post_ssw_dates, lf_dates = get_major_events_ahead(quantile, lf, dates, df_with_count.copy())
	#use this first if we're going to be filtering by length
	#lf_dates will be the return dates if we filter with length to feed the pre SSW dataes

	climab4_with_meta, pre_ssw_dates, lf2_dates = get_events_behind(lf_dates, df_with_count.copy())

	x = len(lf_dates)
	lf2_dates = lf2_dates.to_frame()
	lf2_dates['event_id'] = pd.to_datetime(lf2_dates.event_id)
	ssws = lf2_dates['event_id']

	####################################################################################################################
	### Create list of all dates and u and distance from events 
	####################################################################################################################
	after_dates = climaafter_with_meta.drop(columns = {'ssw','length','day','count'}).rename(columns = {'dist_to_event':'dist_to_event_2'})
	after_dates['time'] = pd.to_datetime(after_dates.time)
	after_df = after_dates['time']
	after_dates = after_dates.set_index('time')

	before = climab4_with_meta.drop(columns = {'ssw','count'}).rename(columns = {'u_anom':'u_anomb4', 'event_id':'event_id_2'})
	before = before[before["time"].str.contains("-05-")==False]
	before['time'] = pd.to_datetime(before.time)
	before_dates = before['time']
	before = before.set_index('time').rename(columns = {'ua':'ub4'})
	before['dist_to_event'] = before['dist_to_event'] * -1 

	both = [before,after_dates]
	merged = pd.concat(both, axis = 1)
	merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
	merged['event_id'] = merged['event_id'].fillna(merged['event_id_2'])
	merged['u_anom'] = merged['u_anom'].fillna(merged['u_anomb4'])
	merged['ua'] = merged['ua'].fillna(merged['ub4'])

	merged = merged.drop(columns = {'dist_to_event_2','u_anomb4', 'event_id_2'}).reset_index().drop(columns = 'ub4').rename(columns ={'index':'time'})
	    #dist_to_event_df = merged.drop(columns = {'ua','u_anom'}).set_index('time')

	########## get length column #############
	meta = climaafter_with_meta.drop(columns = {'ua','u_anom','ssw','count','event_id','day','dist_to_event'})
	meta = meta.merge(major_df, on = 'time')
	meta = meta.drop(columns = {'index','ssw','sign','count','pos_count','drop_idx','ssw_actual'}).rename(columns = {'time':'ssw day 0'})

	return major_df, climab4_with_meta, before_dates, x, ssws, meta, after_df, after_dates, merged

# ssw_wac_info, b4_waccm, dates_waccm, n_wac, ssw_waccm, wac_meta, after_dates_wac, after_df_wac, wac_merged = before_dates(u10_wacm,'no') 
# ssw_cam_info, b4_cam, dates_cam, n_cam, ssw_cam, cam_meta, after_dates_cam, after_df_cam, cam_merged = before_dates(u10_cam, 'no')

ssw_ERA_info, b4_ERA, dates_ERA, n_ERA, ssw_ERA, ERA_meta, after_dates_ERA, after_df_ERA, ERA_merged = before_dates(u10_ERA, "no")
b4_ERA['time'] = pd.to_datetime(b4_ERA['time'])
################################################################################################################################################
def detrend_dim(da, dim_name, deg=1):
    # detrend along a single dimension
    p = da.polyfit(dim_name, deg=deg)
    fit = xr.polyval(da[dim_name], p.msl_polyfit_coefficients)
    return da - fit

######### ERA ###########################################################################################################################################################
dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/t2m/'
day_agg = sorted(glob(f'{dpath}*ERA5.*.nc'))
df = xr.open_mfdataset(day_agg, combine = 'by_coords')
#df = df.to_array()

df['day'] = df['time.dayofyear']
df = df.resample(time = 'D').mean()

#selects nov - may
NH_mask = (df['latitude'] >= 10)
#mask to get only the northern latitudes 
#l_mask = (df['longitude'] == 359.75)
df = df.where(NH_mask, drop = True)
#chop the df so it's only the NH

df = df.rolling(time = 10).mean()
#create a rolling window of times 

msl_avg = df.groupby('time.dayofyear').mean('time')

anomalies = df.groupby('time.dayofyear') - msl_avg
#anomlies are calculated by subtracting the climatology from each independant dayp

anomalies = anomalies.drop_vars('dayofyear')#.to_dataset('variable')
#get rid of unessary variable		

#anom_dt = detrend_dim(anomalies, 'time', deg=1)

anom_pre = anomalies.where(anomalies['time'].isin(b4_ERA['time']), drop = True)#.to_dataframe()

anom_post = anomalies.where(anomalies['time'].isin(after_dates_ERA), drop = True)#.to_dataframe()

################################# Get random array #################################################
era = "before"
field = "t2m"
model = "ERA"

def is_winter(month):
    return (month >= 11) | (month <= 3)


def get_randos(model,tf):
	anom_winter = anomalies.sel(time=is_winter(anomalies['time.month']))

	if tf == "after": 
		shape = len(anom_post.time)
		rand = np.random.randint(0, high=len(anom_winter.time+1), size = shape)

	else: 
		shape = len(anom_pre.time)
		rand = np.random.randint(5000, high=len(anom_winter.time+1), size = shape+1)
		
	rand = xr.DataArray(rand, dims = 'time')
	rand_anom = anom_winter.isel(time=rand)
	print(rand_anom)
	return rand_anom


rand_anom = get_randos(model,era)#.mean(dim = 'dim_0').drop_vars('day')
print(rand_anom)

var1 = rand_anom['t2m'].values
print(var1)
print('###nan count!!!!!')
print(np.count_nonzero(np.isnan(var1)))


rand_anom = rand_anom.to_netcdf('/home/hollythomas/data/for_sig/rand_{}_{}_before.nc/'.format(model, field)) 
#SSW_anom = anom_post.to_netcdf('/home/hollythomas/data/for_sig/event_{}_{}_{}.nc'.format(model, field, era))
exit(1)

plot = anom_pre.mean(dim = 'time')#.drop_vars('day')
#plot = anom_pre.mean(dim = 'time')

numpy = plot.msl.values
zonal_clean = numpy - numpy.mean(axis = 0).squeeze()#[...,None]

plot['msl'] = xr.DataArray(zonal_clean, dims = ['latitude','longitude'])
#plot['t2m'] = xr.DataArray(zonal_clean, dims = ['latitude','longitude'])

# print("msl shape")
# print(plot.msl.values.shape)

plot = plot * .01

levels = [-4, -3.5, -3, -2.5, -2, -1.5, -1, 0, 1, 1.5, 2, 2.5, 3]
# #for psl

#levels = [-2, -1.75, -1.5, -1.25, -1, -.75, -.5, -.25, 0, .25, .5, 1, 1.25, 1.5, 1.75, 2]
#for t2m


fig = plt.figure()
ax = fig.add_subplot(projection = ccrs.Orthographic(central_longitude= 0., central_latitude=90.))

cs = ax.contourf(plot.longitude, plot.latitude, plot.msl, levels,
	extend = 'both', 
	cmap = 'coolwarm', 
	transform= ccrs.PlateCarree())

cmap2 = 'gray'
levels2 = [0.5, 1., 2]

ax.coastlines()
ax.gridlines()
plt.tight_layout()
cbar = fig.colorbar(cs, ticks=[-4, -3.5, -3, -2.5, -2, -1.5, -1, 0, 1, 1.5, 2, 2.5, 3], pad =0.009)
#for psl
#cbar = fig.colorbar(cs, ticks=[ -2,  -1,  0,  1,  2], pad =0.009)
#for t2m
plt.show()
exit(1)