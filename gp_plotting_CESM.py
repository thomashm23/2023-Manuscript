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
u10_waccm = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_wanom_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk').reset_index()
u10_cam = pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_wanom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk').reset_index()

u10_cam['time'] = u10_cam['time'].astype(str).str[0:10].astype(str)
u10_waccm['time'] = u10_waccm['time'].astype(str).str[0:10].astype(str)

def before_dates(df,uf):
	u10 = df
	#input whichever of the two u files into the function to generate files 
	
	uf = uf
	
	# do we want to filter by strength of reversal?
	tf = 'no'

	quantile = .5
	#remeber this is backwards since the values are (-) 
	df_with_count, dates, major_df = intial_filter(quantile, tf, uf, u10.copy(),(u10['ua']), 'model')

	#df_with_count is full dataframe with extra info (original length,includes time, ua, ssw and count)
	#dates are the dates that fulfill CP07 qlaifications
	#major_df is informationally dense df of ssws including time between events, and count - more here than necessary 
	
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
	#need to eliminate non cotinuous values from end of events that start in december (if only have DJFMAM, some december events will have a few days of may - this gets rid of those)
	
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

ssw_wac_info, b4_waccm, dates_waccm, n_wac, ssw_waccm, wac_meta, after_dates_wac, after_df_wac, wac_merged = before_dates(u10_waccm,'no') 
ssw_cam_info, b4_cam, dates_cam, n_cam, ssw_cam, cam_meta, after_dates_cam, after_df_cam, cam_merged = before_dates(u10_cam, 'no')

# ####################################################################################################################
# ###### ATM VERTICAL FIELD ANOMALY #########
# ####################################################################################################################
# ################## WACCM ##################
# model = 'CESM2'
# #model = 'CESM2-WACCM'
# field = "zg"

# def time_shift(model, sim, shift):
#     dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/{}/gn/'.format(model, sim, field)
#     dfile = sorted(glob(f'{dpath}{field}_day_{model}_amip_r{sim}i1p1f1_gn_*.nc'))
#     #get all files corresponding from the each amip simulation
#     df = xr.open_mfdataset(dfile)
#     #open them
#     tlen = shift * (df.sizes['time'])
#     #create value for the shift. Shift value will reflect the number of timeseris lengths I want to shift the data 
#     #(ie, 1 will shift everything to begin the day after the original df ended)
#     df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
#     #This shifts everything the length defined by the shift value 
#     df['time'] = df['time'].astype("datetime64[ns]")
#     #just making sure its the right format
#     #df = df.to_dataframe()
#     return df

# df = time_shift(model, 1, 0)
# df2 = time_shift(model, 2, 1)
# df3 = time_shift(model, 3, 2)
# #this lines are the function input.

# df = xr.concat([df,df2,df3], dim = 'time').drop_vars(['lat_bnds','lon_bnds', 'time_bnds'])
# #put em together! Concat along the time dimension, so that it becomes one long continuous time series. 

# #df['ta'] = df['ta'].astype("float")
# #just make it the right format

# PC_mask = (df['lat'] >= 50)
# #get all the domain above 50! 
# #PL_mask = (df['plev'] == 1e+04)

# df = df.where(PC_mask, drop = True)
# #df = df.where(PL_mask, drop = True)
# ###### CHECK POINT ##########
# #at this point we have a 195 year time series of dimensions (time: 71193, plev: 8, lat: 43, lon:288)

# var = df.zg

# weights = np.cos(np.deg2rad(var.lat))
# #For a rectangular grid the cosine of the latitude is proportional to the grid cell area
# #np.deg2rad(var.lat) creates an array of the cosine of the latitudianlly derived radian value. This is a numpy aray of the grid cell areas

# df = var.weighted(weights)
# #weighting funtion accepts an array of cell area values and weights the df accordingly 

# df = df.mean(("lon", "lat"), skipna=True)
# #calculate the mean accross lat and lon dimenions, should now be time, plev

# df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
# #get the average anomaly of the polar cap 

# df_ssw = df.where(df.time.isin(cam_merged['time']), drop = True)
# #time df down to just days within the 10 day window around ssws 

# df_ssw = df_ssw.to_dataframe().to_pickle('/home/hollythomas/data/CESM_CAM/CAMREAL_zg_anom_80daywindow_ssws_1950-2021.pkl')#.rename(columns = {'t':'10'}).drop(columns = "dayofyear")

# exit(1)
df_ssw = pd.read_pickle('/home/hollythomas/data/CESM_CAM/CAMREAL_zg_anom_80daywindow_ssws_1950-2021.pkl')
df_ssw = df_ssw.drop(columns = {'dayofyear'}).reset_index()
#this is because time and plev are both index values, connot plot correctly in this instance. 

# # ####################################################################################################################
# # ###### HOVMULLER #########
# # ####################################################################################################################
df_ssw['plev'] = df_ssw['plev'] * .01 #.replace([10000.0, 5000.0, 1000.0], [100, 50, 10])
#convert to hPa 
df_ssw['zg'] = df_ssw['zg'] 
#stay tuned 

df_ssw = df_ssw.merge(wac_merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
#this will merge our datafarme with the dataframe containing the distance to event column. 

df_ssw = df_ssw.groupby(['dist_to_event','plev']).mean().reset_index()
print(df_ssw)
exit(1)
#group each point by the day from the event at the pressure level. The result = every pressure level x value at 35 days before the event will be averaged for plotting
fig, ax = plt.subplots(figsize=(15, 3))
levels = [-20, -15, -10, -9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10., 15, 20, 25, 30., 35, 40., 50., 60., 70., 85, 100., 150, 200, 300, 450.]
#creating a non linear array of level to map to, there are so many that when it's linear it looks horrible 
cmap = plt.get_cmap('RdBu_r')

norm = BoundaryNorm(levels, ncolors=cmap.N, clip=True)

tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, 
	linewidths=0.5, 
	cmap='RdYlBu_r',
	norm=norm,
	extend='both')

ylab = [1000, 500, 250, 100, 50, 20, 10]

ax.axvline(x = 0, color='r', linewidth=2)
ax.invert_yaxis()
ax.set_yscale('log')
ax.set_yticks(ylab)
ax.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
fig.colorbar(tcf, pad=0.009)
#ax.set_title('WACCM6 Polar Cap Geopoential Height Anomaly (m) N ={}'.format(n_cam))
ax.set_ylabel('Pressure (hPa')
ax.set_xlabel('Distance (days) from SSW event (0)')
#ax.tight_layout()
#cbar = plt.colorbar(orientation='vertical')
plt.tight_layout()
plt.show()

# levels = list(range(-40,250))
# fig, ax = plt.subplots(figsize=(15, 3))
# tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, linewidths=0.5, cmap='RdYlBu_r')
# ax.axvline(x = 0, color='r', linewidth=2)
# ax.invert_yaxis()
# fig.colorbar(tcf)
# ax.set_title('CAM Polar Cap GPH Anomaly, (m/s) N ={}'.format(n_wac))
# ax.set_ylabel('Pressure (hPa')
# ax.set_xlabel('Days from Event (day 0)')
# #ax.tight_layout()
# #cbar = plt.colorbar(orientation='vertical')
# plt.show()

exit(1)



