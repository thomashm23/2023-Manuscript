import xarray as xr
import netCDF4 as nc
import scipy as sp
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
from glob import glob
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.cm as cm
from matplotlib.ticker import AutoMinorLocator
from scipy.signal import argrelextrema
from cftime import DatetimeNoLeap
from cftime import num2date
from datetime import timedelta
import seaborn; seaborn.set()
import dask as dask
import sys
sys.path.append('/home/hollythomas/python/functions')
from get_events import rolling_count, intial_filter, get_major_events_ahead, get_events_behind
from get_months import is_winter
from itertools import cycle 
import matplotlib.tri as tri
xr.set_options(enable_cftimeindex=True)
dask.config.set(**{'array.slicing.split_large_chunks': False})
import iris 
import iris.plot as iplt
import iris.quickplot as qplt
import pylab
from scipy.stats import norm
import statistics 
from scipy import stats
from matplotlib.colors import BoundaryNorm
from matplotlib.ticker import ScalarFormatter
######################################################
u10 = xr.open_dataset('/home/hollythomas/data/ERA/u/u.u10.60N.avg.ERA.1959-2021.nc')
u10 = pd.read_pickle('/home/hollythomas/data/ERA/u/u10wanom_60N_ERA_1959-2021.pkl').reset_index()
#get previously complied array of u10 60N zonal average 

u10['time'] = u10['time'].astype(str).str[0:10].astype(str)
#format time

def get_dates(df,uf):
	uf = uf
	# do we want to filter by strength of reversal 

	tf = 'no'

	quantile = .5
	#remeber this is backwards since the values are (-) 

	df_with_count, dates, major_df = intial_filter(quantile, tf, uf, u10.copy(),(u10['u']), "reanalysis")
	#df_with_count is full dataframe with extra info (original length,includes time, ua, ssw and count)
	#dates are the dates that fulfill CP07 qlaifications
	#major_df is informationally dense df of ssws including time between events, and count - more here than necessary 
	############
	lf = 'no'
	#length filtering, if "yes" runs script with length filter, if "no" original processing method
	quantile = .5
	#quantile above which you'd like to extract events 

	climaafter_with_meta, post_ssw_dates, lf_dates = get_major_events_ahead(quantile, lf, dates, df_with_count.copy())
	#use this first if we're going to be filtering by length
	#lf_dates will be the return dates if we filter with length to feed the pre SSW dates

	climab4_with_meta, pre_ssw_dates, lf2_dates = get_events_behind(lf_dates, df_with_count.copy())

	x = len(lf2_dates)
	lf2_dates = lf2_dates.to_frame()
	lf2_dates['event_id'] = pd.to_datetime(lf2_dates.event_id)
	ssws = lf2_dates['event_id']


	###################################################################################################################
	## Create list of all dates and u and distance from events 
	###################################################################################################################
	after_dates = climaafter_with_meta.drop(columns = {'ssw','length','day','count'}).rename(columns = {'dist_to_event':'dist_to_event_2'})
	after_dates['time'] = pd.to_datetime(after_dates.time)
	after_df = after_dates['time']
	after_dates = after_dates.set_index('time')

	before = climab4_with_meta.drop(columns = {'ssw','count'}).rename(columns = {'u_anom':'u_anomb4', 'event_id':'event_id_2'})
	before['time'] = pd.to_datetime(before.time)
	before_dates = before['time']

	before = before.set_index('time').rename(columns = {'u':'ub4'})
	before['dist_to_event'] = before['dist_to_event'] * -1 

	both = [before,after_dates]
	
	merged = pd.concat(both, axis = 1)#.drop(columns = {'index','time'})
	merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
	merged['event_id'] = merged['event_id'].fillna(merged['event_id_2'])
	merged['u_anom'] = merged['u_anom'].fillna(merged['u_anomb4'])

	merged['u'] = merged['u'].fillna(merged['ub4'])
	merged = merged.drop(columns = {'dist_to_event_2','u_anomb4','event_id_2'}).reset_index().drop(columns = 'ub4').rename(columns ={'index':'time'})

	meta = climaafter_with_meta.drop(columns = {'u','u_anom','ssw','count','event_id','day','dist_to_event'})
	meta = meta.merge(major_df, on = 'time')
	meta = meta.drop(columns = {'index','ssw','sign','count','pos_count','drop_idx','ssw_actual'}).rename(columns = {'time':'ssw day 0'})
	
	return merged, major_df, x, climab4_with_meta, before_dates, meta, after_df, after_dates

merged, major_df, x, climab4_with_meta, before_dates, ERA_meta, after_dates, after_df = get_dates(u10, "no")
#major_df_f, climab4_with_meta_f, before_dates_f = get_dates(u10, "yes")

# ####################################################################################################################
# ###### ZG ANOMALY #########
# ####################################################################################################################
# ################## WACCM ##################

field = 'z'
#levs = [1000,700,500,250,100,50,10]
levs = [100,50,10]

def get_z(field, lev, plev):
	dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/{}/z{}'.format(field,lev) 
	#dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/ua/gn'
	
	dfs = sorted(glob(f'{dpath}/*197*.nc'))

	# glob retrieves files that match this pattern, sorts them 

	dask.config.set(**{'array.slicing.split_large_chunks': False})

	df = xr.open_mfdataset(dfs, combine='by_coords')
	#get only the polar cap 
	PC_mask = (df['latitude'] >= 50) 
	df = df.where(PC_mask, drop = True)

	df['z'] = (df['z'] / 9.80665)

	df = df.resample(time = 'D').mean()
	# resample the tempresolution to daily 

	df['time'] = df['time'].astype("datetime64[ns]")


	weights = np.cos(np.deg2rad(df.latitude))
	weights.name = "weights"
	df = df.weighted(weights)
	#weight the data frame

	df = df.mean(('longitude','latitude')) 
	#calculate the mean of the polar cap 
	
	df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")

	df_ssw = df.where(df.time.isin(merged['time']), drop = True)
	dim_len = df_ssw.dims['time']
	df_ssw =  df_ssw.expand_dims(dim={"p_lev": np.full(df_ssw.dims['time'], plev)}, axis=0)
	
	return df_ssw

df_levs = []
for lev in levs:
    df_lev = get_z(field, str(lev), lev)
    df_levs.append(df_lev)

df = xr.concat(df_levs, dim='p_lev')
print(df)
exit(1)
# df_10 = get_z(field, '10', 10)
# df_50 = get_z(field, '50', 50)
# df_100 = get_z(field, '100', 100)
# # df_250 = get_z(field, '250', 250)
# # df_500 = get_z(field, '500', 500)
# # df_700 = get_z(field, '700', 700)
# # df_1000 = get_z(field, '1000', 1000)

# df = xr.concat([df_10, df_50, df_100], dim = 'p_lev')
# print(df)
# exit(1)
# df = xr.concat([df_10, df_50, df_100, df_250, df_500, df_700, df_1000], dim = 'p_lev')
# # print(df)
# # exit(1)
# #df = xr.concat([df_10, df_50, df_100], dim = 'p_lev')

# rand_wac = rand_wac.to_dataframe().to_pickle('/home/hollythomas/data/for_sig/hov_rand_WAC_zg.pkl') 

df = df.drop(columns = "dayofyear").reset_index()
#change array to a data frame which should be a column of anomalies and dates, the column name is the p level

df = df.merge(merged, on = "time").drop(columns = {'u_anom','u','event_id'})
df['p_lev'] = df['p_lev'].astype(int)
df = df.groupby(['dist_to_event','p_lev']).mean().reset_index()

fig, ax = plt.subplots(figsize=(15, 3))
levels = [-20, -15, -10, -9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10., 15, 20, 25, 30., 35, 40., 50., 60., 70., 85, 100., 150, 200, 300, 450.]
#creating a non linear array of level to map to, there are so many that when it's linear it looks horrible 

cmap = plt.get_cmap('RdBu_r')
norm = BoundaryNorm(levels, ncolors=cmap.N, clip=True)

tcf = ax.tricontourf(df.dist_to_event, df.p_lev, df.z, levels, 
    linewidths=0.5, 
    cmap='RdYlBu_r',
    norm=norm,
    extend='both')

ylab = [1000, 500, 250, 100, 50, 20, 10]


ax.invert_yaxis()
ax.axvline(x = 0, color='r', linewidth=2)
fig.colorbar(tcf, pad =0.009)
ax.set_yscale('log')
ax.set_yticks(ylab)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
ax.set_title('ERA Polar Cap Geopotential Height Anomaly N ={}'.format(x))
ax.set_ylabel('Pressure (hPa)')
ax.set_xlabel('Days from Event (day 0)')
plt.tight_layout()
#cbar = plt.colorbar(orientation='vertical')
plt.show()

exit(1)
# # ####################################################################################################################
# # ###### HOVMULLER #########
# # ####################################################################################################################
df_ssw['plev'] = df_ssw['plev'] * .01 #.replace([10000.0, 5000.0, 1000.0], [100, 50, 10])
df_ssw = df_ssw.merge(cam_merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
df_ssw = df_ssw.groupby(['dist_to_event','plev']).mean().reset_index()

levels = list(range(-40,250))
fig, ax = plt.subplots(figsize=(15, 3))
tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, linewidths=0.5, cmap='RdYlBu_r')
ax.axvline(x = 0, color='r', linewidth=2)
ax.invert_yaxis()
fig.colorbar(tcf)
ax.set_title('CAM Polar Cap Temp Anomaly (K) N ={}'.format(n_wac))
ax.set_ylabel('Pressure (hPa')
ax.set_xlabel('Days from Event (day 0)')
#ax.tight_layout()
#cbar = plt.colorbar(orientation='vertical')
plt.show()

exit(1)
