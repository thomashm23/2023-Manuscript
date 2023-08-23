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
######################################################
######GET U AT LEVELSS##########

# def get_uanom(lev,shift):
# dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r1i1p1f1/day/ua/gn' 
# #dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/ua/gn'

# dfs = sorted(glob(f'{dpath}/*.nc'))
# # # glob retrieves files that match this pattern, sorts them 
# dask.config.set(**{'array.slicing.split_large_chunks': False})

# df = xr.open_mfdataset(dfs, combine='by_coords') 
# # # create the dataset by opening the multiple files and and combine the series by coordinates 
# tlen = 0#(df.sizes['time'])
# #get length of time dimension to shift full length
# df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
# #shift time dimension its full length for r2, and 2*tlen for r3
# #df_winter = df.ua.sel(time=is_winter(df.ua['time.month']))
# #is_winter currently set to December - April 

# #SANITY CHECK  - does this truncate time? 
# # print('-------------DF_ALL TIME-------------\n')
# # print(df.time)
# # print('-------------DF_WINTER-------------\n')
# # print(df_winter.time)
# # print('-------------DONE-------------\n')
# lat60 = df.ua.sel(lat = 60, method ='nearest')
# # identify and return dataarray consisting of ua values at the lat closest to 60 
# uats = lat60[:,7,:].mean(axis=-1)
# # create the array of ua wind averaged across longitudes, Select 10mb PLEV 
# uads = (uats.to_dataset(dim = None)).to_dataframe(dim_order = None).to_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_zonal_day_CESM2-WACCM_amip_r1i1p1f1.plk')
# # # convert xarray dataarray to pandas dat
# # print(uads)
# exit(1)

# df = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_zonal_day_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk').drop(columns = {'lat','plev'}).to_xarray()
# df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
# df = df.to_dataframe().rename(columns ={'ua':'u_anom'}).drop(columns = 'dayofyear')
# df = df.to_pickle('/home/hollythomas/data/CESM-WACCM/u/u10_60N_anom_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk')
# exit(1)
# df = pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_zonal_day_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk').drop(columns = {'lat','plev'})

# df_anom = pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/u10_60N_anom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk')

# df = df.merge(df_anom, left_index=True, right_index=True) 

# df = df.to_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_wanom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk')

# ######GET DATES##########
# ############ initial filtering for event strengths #######################
# u10 = (pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_wanom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk').reset_index())
# #u10 = (pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_wanom_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk').reset_index())

# #u10 = (pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_zonal_day_CESM2-WACCM_amip_r1i1p1f1.plk').reset_index().drop(columns = {'lat','plev'}))
# #u10 = (pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_zonal_day_CESM2-CAM_amip_r1i1p1f1.plk').reset_index().drop(columns = {'lat','plev'}))
# #get previously complied array of u10 60N zonal average 

# u10['time'] = u10['time'].astype(str).str[0:10].astype(str)

# uf = 'no'
# # do we want to filter by strength of reversal 
# tf = 'no'

# quantile = .5
# #remeber this is backwards since the values are (-) 

# df_with_count, dates, major_df = intial_filter(quantile, tf, uf, u10.copy(),(u10['ua']))
# #df_with_count is full dataframe with extra info (original length,includes time, ua, ssw and count)
# #dates are the dates that fulfill CP07 qlaifications
# #major_df is informationally dense df of ssws including time between events, and count - more here than necessary 

# ############
# #length filter
# ############
# lf = 'no'
# #length filtering, if "yes" runs script with length filter, if "no" original processing method
# quantile = .5
# #quantile above which you'd like to extract events 
# climaafter_with_meta, post_ssw_dates, lf_dates = get_major_events_ahead(quantile, lf, dates, df_with_count.copy())

# #use this first if we're going to be filtering by length
# #lf_dates will be the return dates if we filter with length to feed the pre SSW dataes

# climab4_with_meta, pre_ssw_dates, lf2_dates = get_events_behind(lf_dates, df_with_count.copy())
# x = len(lf_dates)
# ####################################################################################################################
# ### Create list of all dates and u and distance from events 
# ####################################################################################################################
# after_dates = climaafter_with_meta.drop(columns = {'ssw','length','day','count','event_id'}).rename(columns = {'dist_to_event':'dist_to_event_2'})
# after_dates['time'] = pd.to_datetime(after_dates.time)
# after_df = after_dates['time']
# after_dates = after_dates.set_index('time')

# before = climab4_with_meta.drop(columns = {'ssw','count','event_id'}).rename(columns = {'u_anom':'u_anomb4'})
# before['time'] = pd.to_datetime(before.time)
# before_dates = before['time']
# before = before.set_index('time').rename(columns = {'ua':'ub4'})
# before['dist_to_event'] = before['dist_to_event'] * -1 

# both = [before,after_dates]
# merged = pd.concat(both, axis = 1)#.drop(columns = {'index','time'})
# merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
# merged['u_anom'] = merged['u_anom'].fillna(merged['u_anomb4'])
# merged['ua'] = merged['ua'].fillna(merged['ub4'])

# merged = merged.drop(columns = {'dist_to_event_2','u_anomb4'}).reset_index().drop(columns = 'ub4').rename(columns ={'index':'time'})
# dist_to_event_df = merged.drop(columns = {'ua','u_anom'}).set_index('time')

##########################################################################################
# EP Flux Plotting 
##########################################################################################
# model = 'CESM2'

# def time_shift(model, sim, shift):
#     dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/EdayZ/EPflux/gn/'.format(model, sim)
#     dfile = sorted(glob(f'{dpath}EPflux_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))
#     df = xr.open_mfdataset(dfile, combine='by_coords')
#     tlen = shift * (df.sizes['time'])
#     df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
#     df['time'] = df['time'].astype("datetime64[ns]")
#     return df

# df1 = time_shift(model, 1, 0)
# df2 = time_shift(model, 2, 1)
# df3 = time_shift(model, 3, 2)

# df = xr.merge([df1,df2,df3], compat = 'override')
# df = df.where(df.time.isin(merged['time']), drop = True)#.drop_vars(['dudt','epfy'])

# # epf = df.epfp[:,:,:]
# # div = df.epfdiv[:,:,:]

# epf = df.to_dataframe().reset_index().set_index('time')

# epf = epf.merge(dist_to_event_df, left_index = True, right_index = True)

# epf = epf.groupby(['dist_to_event','lat','plev']).mean().reset_index().set_index('dist_to_event')


# epf40 = epf.loc[0]

# epf20 = epf.loc[-20]

# epf10 = epf['epfp'].loc[-10]
# epf0 = epf['epfp'].loc[0]

# #fig, axs = plt.subplots(nrows = 2, ncols = 2)
# lat = epf40['lat'].unique()
# print(len(lat))
# plev = epf40['plev'].unique()
# print(len(plev))
# plev = np.flip(plev)
# X,Y = np.meshgrid(lat,plev)
# ##quiver##
# U = epf40['epfy']
# V = epf40['epfp']
# ###countour###
# Z = epf40['epfdiv']
# z = np.array(Z)
# z = z.reshape((len(plev), len(lat)))

# fig,ax = plt.subplots()
# ax.quiver(X,Y,U,V)
# ax.contour(X, Y, z, linestyles = 'dashed', linewidth = .01)
# ax.set_ylim(100000,0)
# plt.show()
# exit(1)

# for day, ax in zip(distances, axs.ravel()):
#     epf['epfp',''].loc[day].quiver(ax=ax)
#     ax.set_title(ticker.upper())
#     ax.get_legend().remove()
#     ax.set_xlabel("")

# plt.show()

##############################################################
############ Meridional Heat Flux ##########
#############################################################
#u10_wacm = (pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_zonal_day_CESM2-WACCM_amip_r1i1p1f1.plk').reset_index().drop(columns = {'lat','plev'}))
#u10_cam = (pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_zonal_day_CESM2-CAM_amip_r1i1p1f1.plk').reset_index().drop(columns = {'lat','plev'}))

u10_wacm = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_wanom_CESM2-WACCM_amip_allsims_gn_19500101-21450103.plk').reset_index()
u10_cam = pd.read_pickle('/home/hollythomas/data/CESM_CAM/u/ua10_60N_wanom_CESM2-CAM_amip_allsims_gn_19500101-21450103.plk').reset_index()
###### this is what 
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

ssw_wac_info, b4_waccm, dates_waccm, n_wac, ssw_waccm, wac_meta, after_dates_wac, after_df_wac, wac_merged = before_dates(u10_wacm,'no') 
ssw_cam_info, b4_cam, dates_cam, n_cam, ssw_cam, cam_meta, after_dates_cam, after_df_cam, cam_merged = before_dates(u10_cam, 'no')


##################################
###### GET OVERALL DIST #############
###################################
# #model = 'CESM2'
# model = 'CESM2-WACCM'
# sim = 1

# # dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/filtered/bp_2.5-6_days.VT100/'.format(model, sim)
# # dfile = sorted(glob(f'{dpath}bp_2.5-6_days.U100_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))

# dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/filtered/lp_10_days.VT100/'.format(model, sim)
# dfile = sorted(glob(f'{dpath}lp_10_days.VT100_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))
# df = xr.open_mfdataset(dfile, combine='by_coords')
# # tlen = shift * (df.sizes['time'])
# # df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
# # df['time'] = df['time'].astype("datetime64[ns]")
# #df = df.VT100[:,144:176,:]
# df = df.VT100[:,144:145,120:121]
# print(df)
# #get 45N -75N
# weights = np.cos(np.deg2rad(df.lat))
# weights.name = "weights"
# df_weighted = df.weighted(weights)
# #weight the dataframe to consider latitude area effect 
# weighted_mean = df_weighted.mean(('lon','lat'))
# #calculate field average
# daily_anoms = weighted_mean.groupby('time.dayofyear') - weighted_mean.groupby("time.dayofyear").mean("time")
# #get EHF anomalies
# #daily_anoms = daily_anoms.where(daily_anoms['time'].isin(dates), drop = True)
# #get df of just the dates associated with "weak" vortex events
# df = daily_anoms.rolling(time = 40).mean()

# df_overall_dist = df.to_dataframe().reset_index().set_index('time').drop(columns = {'dayofyear'}).dropna()
# #convert to df

####################################################################################################################
################################ VT100 Histogram ##################################
####################################################################################################################

def time_shift_MHF(model, sim, shift, b4, dates, overall):
    # dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/filtered/bp_2.5-6_days.VT100/'.format(model, sim)
    # dfile = sorted(glob(f'{dpath}bp_2.5-6_days.VT100_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))
 
    # dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/filtered/lp_10_days.VT100/'.format(model, sim)
    # dfile = sorted(glob(f'{dpath}lp_10_days.VT100_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))

    df = xr.open_dataset('/home/hollythomas/data/{}/vt100/vt100_day_{}_amip_allsims_19790101-21450103.nc'.format(model, model))
    # tlen = shift * (df.sizes['time'])
    # df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
    df['time'] = df['time'].astype("datetime64[ns]")
 
    ###### NEW METHOD #############
    #df = df.rolling(time = 40).mean()

    weights = np.cos(np.deg2rad(df.lat))
    weights.name = "weights"
    df_weighted = df.weighted(weights)
    #weight the dataframe to consider latitude area effect 
    weighted_mean = df_weighted.mean(('lon','lat'))
    #calculate field average
    daily_anoms = weighted_mean.groupby('time.dayofyear') - weighted_mean.groupby("time.dayofyear").mean("time")
    #get EHF anomalies
    
    if overall == "yes": 
        ###### OLD METHOD
        df = daily_anoms.rolling(time = 40).mean()

        df_overall_dist = daily_anoms.to_dataframe().reset_index().set_index('time').drop(columns = {'dayofyear'}).dropna()
        #convert to df
        df = df_overall_dist
        #df = df.rename("vt100")
        df = df.rename(columns = {'__xarray_dataarray_variable__':'VT100'})
    else:     
        daily_anoms = daily_anoms.where(daily_anoms['time'].isin(dates), drop = True)
        #get df of just the dates associated with "weak" vortex events
   
        df = daily_anoms.to_dataframe().reset_index().set_index('time').drop(columns = {'dayofyear'})
        #convert to df

        bf = b4.set_index('time').drop(columns = {'count','ssw','ua'})
        #merge with df that has info on event ids
        df = df.merge(bf, left_index = True, right_index = True)#.drop(columns = "dist_to_event")
     
        df['dist_to_event'] = df['dist_to_event'] * -1
        #putem' together
        #df = df.rename("vt100")
        df = df.groupby(['event_id']).mean()

        df = df.rename(columns = {'__xarray_dataarray_variable__':'VT100'})
        #get the average for the 40 days leading up to each event 
    return df

df_cam = time_shift_MHF("CESM_CAM", 1, 0, b4_cam, dates_cam, 'no').reset_index()#.set_index('dist_to_event')
df_waccm = time_shift_MHF('CESM-WACCM',1,0,b4_waccm, dates_waccm, 'no').reset_index()#.set_index('dist_to_event')

df_cam_ovr = time_shift_MHF("CESM_CAM", 1, 0, b4_cam, dates_cam, 'yes')
df_waccm_ovr = time_shift_MHF("CESM-WACCM", 1, 0, b4_cam, dates_cam, 'yes')


# df_cam.groupby('event_id')['VT100'].plot(legend=True)
# df_waccm.groupby('event_id')['VT100'].plot(legend=True)

# cam_threshold = df_cam_ovr.VT100.quantile(q = .95)
# waccm_threshold = df_waccm_ovr.VT100.quantile(q = .95)

# count_cam = (df_cam['VT100'] > (df_cam_ovr.VT100.quantile(q = .95))).sum()
# count_waccm = (df_waccm['VT100'] > (df_waccm_ovr.VT100.quantile(q = .95))).sum()

# per_over_cam =  100*(count_cam/len(df_cam))
# print(per_over_cam)
# per_over_waccm = 100*(count_waccm/len(df_waccm))
# print(per_over_waccm)

muW, stdW = norm.fit(df_waccm.VT100.values)
muC, stdC = norm.fit(df_cam.VT100.values)

muALLc, stdALLc = norm.fit(df_cam_ovr.VT100.values)
muALLw, stdALLw = norm.fit(df_waccm_ovr.VT100.values)
#plt.hist(W_anoms_df.values, density=True, alpha=.6, color ='b')

xmin, xmax = plt.xlim(left = -30, right = 22)
#xmin, xmax = plt.xlim(left = -.75, right = .75)
#gives us xrange 
x = np.linspace(xmin, xmax, 100)
#gives us x range to feed into f(X) for the pdf of the two distributions
pW = norm.pdf(x, muW, stdW)

#calculate the pdf of WACCM dates
pC = norm.pdf(x, muC, stdC)

# calculate pdf of CAM dates
pCa = norm.pdf(x, muALLc, stdALLc)

pWa = norm.pdf(x, muALLw, stdALLw )

fig = plt.figure()
ax = fig.add_subplot()

min_ylim, max_ylim = plt.ylim()

##### WACCM ########
# colors = ['WACCM Events, n= {}'.format(n_wac)]
# _ = ax.hist(df_waccm.VT100.values, density=True, alpha=.6, color ='b', label = colors)
# _ = ax.plot(x, pW, 'k', linewidth = 1, color = 'b')
# _ = plt.axvline(df_waccm.VT100.values.mean(), color='b', linestyle='dashed', linewidth=2)
# #_ = plt.text(df_waccm.VT100.values.mean()*1.1, max_ylim*0.9, ' Cam Mean: {:.2f}'.format(df_waccm.VT100.values.mean()))

# waccm = ['All winter days']
# _ = ax.hist(df_waccm_ovr.VT100.values, bins = 40, density= True, color='purple', alpha = .3, label = waccm)
# _ = plt.axvline(df_waccm_ovr.VT100.values.mean(), color='purple', linestyle='dashed', linewidth=2)
# _ = ax.plot(x, pWa, 'k', linewidth = 1, color = 'purple')
# #_ = ax.legend(['WACCM Events, n = {}'.format(n_wac),'WACCM All days'])#"CAM General", 'WACCM General'])


####### CAM ##########
# again = ['CAM Events, n={}'.format(n_cam)]
# _ = ax.hist(df_cam.VT100.values, density= True, color='red', alpha = .3, label = again)
# _ = ax.plot(x, pC, 'k', linewidth = 1, color = 'r')
# _ = plt.axvline(df_cam.VT100.values.mean(), color='r', linestyle='dashed', linewidth=2)
# #_ = plt.text(df_cam.VT100.values.mean()*1.1, max_ylim*0.9, ' Cam Mean: {:.2f}'.format(df_cam.VT100.values.mean()))
# cam_label = ["All winter days"]
# _ = ax.hist(df_cam_ovr.VT100.values, bins = 40, density= True, color='purple', alpha = .3, label = cam_label)
# _ = ax.plot(x, pCa, 'k', linewidth = 1, color = 'purple')
# _ = plt.axvline(df_cam_ovr.VT100.values.mean(), color='purple', linestyle='dashed', linewidth=2)


# # Customize the appearance
# ax.set_facecolor('white')  # Set the background color to white
# ax.spines['top'].set_visible('black')
# ax.spines['right'].set_visible('black')
# ax.spines['bottom'].set_color('black')  # Set the color of the bottom spine to black
# ax.spines['left'].set_color('black')    # Set the color of the left spine to black



# #_ = ax.legend(prop={'size': 10})
# _ = ax.set_xlabel('Meridional Heat Flux Anomaly')
# _ = ax.set_ylabel('PDF f(x)')
# #_ = ax.get_yaxis().set_visible(False)
# _ = plt.xlim([-20,17])
# _ = plt.ylim(top= 0.3, bottom = 0.0)

# ax.set_facecolor('white')
# ax.spines['top'].set_visible(True) and ax.spines['right'].set_visible(True)
# ax.spines['bottom'].set_color('black') and ax.spines['left'].set_color('black')

# plt.show()
# #plt.savefig('/home/hollythomas/data/figures/ms_thesis/cam_vt100')
# exit(1)


####################################################################################################################
################################ ua/vt100 coordinatio  ##################################
####################################################################################################################

# cam_meta['time'] = pd.to_datetime(cam_meta.time)
# wac_meta['time'] = pd.to_datetime(wac_meta.time)

# cam_grpd = cam_meta.groupby([(cam_meta.time.dt.month),(cam_meta.time.dt.day)]).mean()
# wac_grpd = cam_meta.groupby([(wac_meta.time.dt.month),(wac_meta.time.dt.day)]).mean()
# print(cam_grpd.head(365).to_string())
# print(wac_grpd.head(365).to_string())
# exit(1)

# # fig = cam_grpd.ua.plot() # where the 'Date' is the column with date.

# # wac_grpd.ua.plot(ax=fig)
# # plt.show()
# # exit(1)

# wac_meta = wac_meta.set_index('time')


# cam_meta = cam_meta.merge(df_cam_ovr, left_index = True, right_index = True)
# wac_meta = wac_meta.merge(df_waccm_ovr, left_index = True, right_index = True)


# corr_cam = cam_meta['ua'].corr(cam_meta['VT100'])
# corr_wac = wac_meta['ua'].corr(wac_meta['VT100'])


####################################################################################################################
################################ Variability plot ##################################
####################################################################################################################

# ua = (pd.read_pickle('/home/hollythomas/data/CESM-WACCM/u/ua10_60N_zonal_day_CESM2-WACCM_amip_r1i1p1f1.plk').reset_index().drop(columns = {'lat','plev'}))
# ua['time'] = ua['time'].astype(str).str[0:10]
# ua['time'] = pd.to_datetime(ua.time)
# ua['ssw'] = False
# ua['ssw'] = ua['time'].isin(ssw_waccm)
# ua = ua.set_index('time')
# VT100 = df_waccm_ovr
# ua = ua.merge(VT100, left_index = True, right_index = True).reset_index()
# ua = ua.iloc[0:2000]

# fig = plt.figure()
# ax = ua.plot.line(x = 'time', y= ['ua','VT100'], figsize = (29,5))
# #ax = ua.plot(ua[ua.ssw==True].time, ua[ua.ssw==True].ua, marker='o')
# plt.show()

# ####################################################################################################################
# ###### ZG ANOMALY #########
# ####################################################################################################################
# field = 'zg'
# model = 'CESM2'

# def time_shift(model, sim, shift):
#     dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/{}/gn/'.format(model, sim, field)
#     dfile = sorted(glob(f'{dpath}{field}_day_{model}_amip_r{sim}i1p1f1_gn_*.nc'))
#     df = xr.open_mfdataset(dfile)
#     tlen = shift * (df.sizes['time'])
#     df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
#     df['time'] = df['time'].astype("datetime64[ns]")
#     #df = df.to_dataframe()
#     return df

# df1 = time_shift(model, 1, 0)
# df2 = time_shift(model, 2, 1)
# df3 = time_shift(model, 3, 2)


# # df2 = putemtogether(r2dpath, r2dpathstrat,1)
# # df3 = putemtogether(r3dpath, r3dpathstrat,2)

# df = xr.merge([df1,df2,df3], compat = 'override')
# df['time'] = df['time'].astype("datetime64[ns]")
# #df['ta'] = df['ta'].astype("float")
# #just make it the right format
# PC_mask = (df['lat'] >= 50)
# PL_mask = (df['plev'] <= 1e+04)
# df = df.where(PC_mask, drop = True)
# df = df.where(PL_mask, drop = True)

# wgt = np.cos(np.deg2rad(df['lat']))
# df = df.weighted(wgt).mean(("lon", "lat"), skipna=True)
# #calculate the mean accross lat and lon dimenions, should now be time, plev
# df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
# #get the average anomaly of the polar cap 
# df_ssw = df.where(df.time.isin(merged['time']), drop = True)#.drop_vars(['lat_bnds','lon_bnds'])
# #time df down to just days within the 10 day window around ssws 
# df_ssw = df_ssw.to_dataframe()#.to_pickle('/home/hollythomas/data/CESM_CAM/ta_anom_80daywindow_ssws_1950-2021.pkl')#.rename(columns = {'t':'10'}).drop(columns = "dayofyear")
# df_ssw = df_ssw.drop(columns = {'dayofyear'}).reset_index()
# # df1= df_ssw.unstack()
# # df1.columns = df1.columns.map('{0[0]}_{0[1]}'.format)
# # df1 = df1.reset_index().rename(columns = {'ta_10000.0':'100','ta_5000.0':'50','ta_20.0':'20', 'ta_1000.0':'10', 'ta_5.0':'5', 'ta_7.0':'7', 'ta_3.0':'3', 'ta_1.0':'1'})
# # df1 = df1.merge(merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
# # df1 = df1.groupby('dist_to_event').mean()
# # new_cols = [str(100),str(50),str(20),str(10),str(7),str(5),str(3),str(1)]
# # df1=df1[new_cols]
# # # fig = df1.plot.line()
# plt.title('CESM-CAM SSW(u'+str(i)+ ' key)Strat Temp (K) anomaly N ={}'.format(x))
# # plt.show()    

####################################################################################################################
###### ATM VERTICAL FIELD ANOMALY #########
####################################################################################################################
################## WACCM ##################
# #model = 'CESM2'
# model = 'CESM2-WACCM'
# field = "zg"



# def time_shift(model, sim, shift):
#     dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/{}/gn/'.format(model, sim, field)
#     dfile = sorted(glob(f'{dpath}{field}_day_{model}_amip_r{sim}i1p1f1_gn_*.nc'))
#     df = xr.open_mfdataset(dfile)
#     tlen = shift * (df.sizes['time'])
#     df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
#     df['time'] = df['time'].astype("datetime64[ns]")
#     #df = df.to_dataframe()
#     return df

# # path1 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r1i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r1i1p1f1_gn_19500101-20150116.nc'
# # path2 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r2i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r2i1p1f1_gn_19500101-20150101.nc'
# # path3 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r3i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r3i1p1f1_gn_19500101-20150101.nc'

# # df = time_shift(model, 1, 0, path1)
# # df2 = time_shift(model, 2, 1, path2)
# # df3 = time_shift(model, 3, 2, path3)

# df = time_shift(model, 1, 0)
# df2 = time_shift(model, 2, 1)
# df3 = time_shift(model, 3, 2)


# df = xr.concat([df,df2,df3], dim = 'time')
# df['time'] = df['time'].astype("datetime64[ns]")

# #df['ta'] = df['ta'].astype("float")
# #just make it the right format

# PC_mask = (df['lat'] >= 50)
# #PL_mask = (df['plev'] <= 1e+04)

# df = df.where(PC_mask, drop = True)
# #df = df.where(PL_mask, drop = True)

# # weights = np.cos(np.deg2rad(df['lat']))
# # df = df.weighted(weights)

# var = df.zg
# weights = np.cos(np.deg2rad(var.lat))
# df = var.weighted(weights)

# df = df.mean(("lon", "lat"), skipna=True)
# #calculate the mean accross lat and lon dimenions, should now be time, plev
# df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
# #get the average anomaly of the polar cap 
# df_ssw = df.where(df.time.isin(wac_merged['time']), drop = True)#.drop_vars(['lat_bnds','lon_bnds'])
# #time df down to just days within the 10 day window around ssws 

# df_ssw = df_ssw.to_dataframe().to_pickle('/home/hollythomas/data/CESM_/ta_anom_80daywindow_ssws_1950-2021.pkl')#.rename(columns = {'t':'10'}).drop(columns = "dayofyear")

# df_ssw = df_ssw.drop(columns = {'dayofyear'}).reset_index()

# df1= df_ssw.unstack()
# df1.columns = df1.columns.map('{0[0]}_{0[1]}'.format)
# df1 = df1.reset_index().rename(columns = {'ta_10000.0':'100','ta_5000.0':'50','ta_20.0':'20', 'ta_1000.0':'10', 'ta_5.0':'5', 'ta_7.0':'7', 'ta_3.0':'3', 'ta_1.0':'1'})
# df1 = df1.merge(merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
# df1 = df1.groupby('dist_to_event').mean()
# new_cols = [str(100),str(50),str(20),str(10),str(7),str(5),str(3),str(1)]
# df1=df1[new_cols]
# # fig = df1.plot.line()
# plt.title('CESM-CAM SSW(u'+str(i)+ ' key)Strat Temp (K) anomaly N ={}'.format(x))
# # plt.show()	

# # ####################################################################################################################
# # ###### HOVMULLER #########
# # ####################################################################################################################

df_ssw = pd.read_pickle('/home/hollythomas/data/for_sig/CAM_zg_for_hov.pkl').reset_index()
df_ssw['plev'] = df_ssw['plev'] * .01 #.replace([10000.0, 5000.0, 1000.0], [100, 50, 10])
df_ssw['zg'] = df_ssw['zg'] 
df_ssw['{}'.format(field)] = df_ssw['{}'.format(field)] 
df_ssw = df_ssw.merge(cam_merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
df_ssw = df_ssw.groupby(['dist_to_event','plev']).mean().reset_index()
fig, ax = plt.subplots(figsize=(15, 3))
levels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10., 15, 20, 25, 30., 35, 40., 50., 60., 70., 85, 100., 150, 200, 300, 450.]
#levels = [-3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10., 12., 15.]
cmap = plt.get_cmap('RdBu_r')
norm = BoundaryNorm(levels, ncolors=cmap.N, clip=True)

tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, 
	linewidths=0.5, 
	cmap='RdYlBu_r',
	norm=norm,
	extend='both')

ax.axvline(x = 0, color='r', linewidth=2)
ax.invert_yaxis()
fig.colorbar(tcf)
ax.set_title('CAM Polar Cap Temperature Anomaly, N ={}'.format(n_cam))
ax.set_ylabel('Temperature (K)')
ax.set_xlabel('Days from Event (day 0)')
#ax.tight_layout()
#cbar = plt.colorbar(orientation='vertical')
# plt.colorbar(tcf,ax=ax)
# ax.remove()
plt.show()
exit(1)
# # levels = list(range(-40,250))
# # fig, ax = plt.subplots(figsize=(15, 3))
# # tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, linewidths=0.5, cmap='RdYlBu_r')
# # ax.axvline(x = 0, color='r', linewidth=2)
# # ax.invert_yaxis()
# # fig.colorbar(tcf)
# # ax.set_title('CAM Polar Cap GPH Anomaly, (m/s) N ={}'.format(n_wac))
# # ax.set_ylabel('Pressure (hPa')
# # ax.set_xlabel('Days from Event (day 0)')
# # #ax.tight_layout()
# # #cbar = plt.colorbar(orientation='vertical')
# # plt.show()

# exit(1)

####################################################################################################################
###### FIELD ANOMALY #########
####################################################################################################################
#dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/tas/gn/'

model = 'CESM2'
#model = 'CESM2-WACCM'
field = "tas"


def time_shift(model, sim, shift):
    dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/{}/gn/'.format(model, sim, field)
    dfile = sorted(glob(f'{dpath}{field}_day_{model}_amip_r{sim}i1p1f1_gn_*.nc'))
    df = xr.open_mfdataset(dfile, combine = 'by_coords')
    tlen = shift * (df.sizes['time'])
    df['time'] = xr.CFTimeIndex(df.time.get_index('time') + timedelta(days=tlen))
    df['time'] = df['time'].astype("datetime64[ns]")
    #df = df.to_dataframe()
    return df

# path1 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r1i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r1i1p1f1_gn_19500101-20150116.nc'
# path2 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r2i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r2i1p1f1_gn_19500101-20150101.nc'
# path3 = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2-WACCM/amip/r3i1p1f1/day/psl/gn/psl_day_CESM2-WACCM_amip_r3i1p1f1_gn_19500101-20150101.nc'

# df = time_shift(model, 1, 0, path1)
# df2 = time_shift(model, 2, 1, path2)
# df3 = time_shift(model, 3, 2, path3)


df = time_shift(model, 1, 0)
df2 = time_shift(model, 2, 1)
df3 = time_shift(model, 3, 2)

# df = xr.concat([df,df2,df3], dim = 'time')

df = df.sel(time = is_winter(df['time.month'])).drop_vars(['time_bnds','lat_bnds','lon_bnds'])
#selects nov - may

df['day'] = df['time.dayofyear']
#get numerical day values

NH_mask = (df['lat'] >= 10)
df = df.where(NH_mask, drop = True)
#mask out everything over 10 N

#plev_mask = (df['plev'] == 50000)
#df = df.where(plev_mask, drop = True)

# anom_pre = df.where(df.time.isin(pre_dates), drop = True)
# anom_post = df.where(df.time.isin(post_dates), drop = True)

df = df.rolling(time=10).mean()
#create rolled average (10 day windows)

df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")
#calculate rolled winder average and anomaly values

b4_anoms = df.where(df.time.isin(dates_cam), drop = True).drop_vars('day')
#after_anoms = df.where(df.time.isin(after_df), drop = True).drop_vars('day')

b4_plt = b4_anoms.mean(dim = 'time')

#after_plt = after_anoms.mean(dim = 'time')

# numpy = b4_plt.zg.values

# #var-var.values.mean(axis=-1)[...,None]
# #print(numpy.shape)

# mean = numpy.mean(axis = 1)
# zonal_clean = numpy - mean[...,None]
# b4_plt['zg'] = xr.DataArray(zonal_clean, dims = ['lat','lon'])

b4_plt = b4_plt * .01

fig = plt.figure(figsize=[7,5])

# 111 means 1 row, 1 col and index 1
ax = fig.add_subplot(projection = ccrs.Orthographic(central_longitude= 0, central_latitude=90))

b4_plt['zg'].plot(ax=ax,  cmap='coolwarm',
                    transform=ccrs.PlateCarree(), vmin = -2, vmax = 2, extend = 'both')

ax.coastlines()
ax.gridlines()
#fig.colorbar(mappable = ax)
ax.set_title('CAM 40d pre zg 500 (K), N = {}'.format(n_))
plt.show()
#upper 50th quant
exit(1)
##########################
##### SIG TEST ###########
#########################
# def significance_tests(xarray_a, xarray_b, t_test=False, levene_test=False,
#                        equal_variance = False, nan_policy= 'omit', mask_not_sig = True, 
#                        level_of_sig = 0.05, center='mean'):
#     """
#     This function operates on a per-pixel basis and contains two types of significance tests:
    
#     1.  A two-sided t-test for the null hypothesis that two independent samples have identical average (expected) values. 
#         This test assumes that the populations have unequal variances by default (i.e. Welsh T-test), changing the 'equal_variance'
#         variable will change this to an ordinary two-sided t-test. 
#         See "scipy.stats.ttest_ind" for more info:
#         https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.ttest_ind.html
    
#     2. Levene statistic tests the null hypothesis that all input samples are from populations with equal variances.
#        See "scipy.stats.levene" for more info:
#        https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.levene.html
#     The user must specify which statistic to run, by default both tests are set to False. 
#     Function will return two xarrrays, one conatining the statistic, and the other containing the p-values.
#     The default is NOT to mask the pixels with p-values > the specified level of significance (default p-valies is 0.05).
#     If conducting levene stats, be aware that large datasets could take a long time to compute.
    
#     Inputs:
#     xarray_a = an xarray dataArray containing observations from your first period of interest
#     xarray_b = an xarray dataArray containing observations from your second period of interest
#     t_test = Boolean. If True, conducts a per-pixel t-test
#     levene_test = Boolean. If True, conducts a per-pixel levene-test
#     mask_not_sig = Boolean. If True, mask out the values that don't achieve the desired level of significance
#     level_of_sig = Float.  The level of confidence you wish to have if masking. Usually 0.05 (default) or 0.1
#     equal_variance, nan_policy, center = see scipy.stats documentation
    
#     Last modified: June 2018
#     Author: Chad Burton
#     """
    
#     #convert into numpy ndarray arrays
#     arr_1 = xarray_a.values
#     arr_2 = xarray_b.values
#     #Get coordinates from the original xarray
#     lat  = xarray_a.coords['lat']
#     lon = xarray_a.coords['lon']
    
#     if (t_test==False and levene_test==False):
#         print('Please specificy which statistic you want to run by including either "t_test=True" or "levene_test=True" in your function call')
    
#     if (t_test==True and levene_test==True):
#         print('One statistical test at a time please!')
    
#     if t_test:
#         #run the t-test
#         print('starting T-test')
#         t_stat, p_values = stats.ttest_ind(arr_1, arr_2, equal_var = equal_variance, nan_policy = nan_policy)
#         if mask_not_sig == True:
#             t_stat[p_values>level_of_sig]=np.nan
#         #Write arrays into a x-array
#         t_stat_xr = xr.DataArray(t_stat, coords = [lat, lon], dims = ['lat', 'lon'], name='t_stats')
#         p_val_xr = xr.DataArray(p_values, coords = [lat, lon], dims = ['lat', 'lon'], name='p_value') 
#         print('finished T-test')
#         return t_stat_xr, p_val_xr
            
#     if levene_test:
#         #create empty arrays to put the levene-stat results in
#         t1, x1, y1 = arr_1.shape
#         t2, x2, y2 = arr_2.shape
#         assert x1 == x2 and y1 == y2
#         levene_f = np.zeros((x1, y1))
#         levene_p = np.zeros((x1, y1))
#         #loop through each cell of arr1 and arr2 to conduct the levene test
#         print('Starting for loop...this could take a while')
#         for x in range(x1):
#             for y in range(y1):
#                 arr_3 = arr_1[:, x, y] #for each x,y position, create a 1D array of the timeseries
#                 arr_4 = arr_2[:, x, y]
#                 arr_3 = arr_3[~np.isnan(arr_3)] #deal with the nans
#                 arr_4 = arr_4[~np.isnan(arr_4)]
#                 levene_f[x,y], levene_p[x,y] = stats.levene(arr_3, arr_4, center=center) #run the test     
#         #Mask out values with insignificant trends (ie. p-value > 0.05) if user wants
#         if mask_not_sig == True:   
#             levene_f[levene_p>level_of_sig]=np.nan
#         levene_stat_xr = xr.DataArray(levene_f, coords = [lat, lon], dims = ['lat', 'lon'], name='levene_stats')
#         p_val_levene_xr = xr.DataArray(levene_p, coords = [lat, lon], dims = ['lat', 'lon'], name='p_value_levene') 
#         print('finished levene test')
#         return levene_stat_xr, p_val_levene_xr


# tstats, pvals = significance_tests(df.psl, b4_anoms.psl, t_test= True, levene_test=False,
#                        equal_variance = False, nan_policy= 'omit', mask_not_sig = True, 
#                        level_of_sig = 0.05, center='mean')


##########################################################################
############### U ANOM PLOTTING ##########################################
##########################################################################
def make_ua_anom_plt(b4_df, after_df, model):
    before = b4_df
    before['time'] = pd.to_datetime(before.time)
    before = before.set_index(before['time']).rename(columns = {'ua':'ub4', 'u_anom':'u_anomb4', 'event_id':'event_id_2'}).drop(columns = {'ssw','count'})
    before['dist_to_event'] = before['dist_to_event'] * -1

    after = after_df.reset_index()
    after['time'] = pd.to_datetime(after.time)
    after = after.set_index(after['time'])
  

    both = [before,after]
    merged = pd.concat(both, axis = 1).drop(columns = {'time'})
    merged['event_id'] = merged['event_id'].fillna(merged['event_id_2'])
    merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
    merged['ua'] = merged['ua'].fillna(merged['ub4'])
    merged['u_anom'] = merged['u_anom'].fillna(merged['u_anomb4'])
    merged = merged.drop(columns = {'u_anomb4'})
    merged = merged
    merged = merged.drop(columns = {'dist_to_event_2'}).reset_index().drop(columns = {'ub4','event_id_2'})
    merged = merged[(merged['time'].dt.month != 5)]
    merged = merged.rename(columns = {'ua': model})
    merged = merged.rename(columns = {'u_anom': "u10 anom {}".format(model)})
    u_anom_0 = merged[merged['dist_to_event'] == 0]
    #u_anom_mean = u_anom_0.u_anom.mean()
    merged = merged.set_index('dist_to_event')
    return merged

wac_merged = make_ua_anom_plt(b4_waccm, after_df_wac, 'CESM2-WACCM')
cam_merged = make_ua_anom_plt(b4_cam, after_df_cam, 'CESM2-CAM')


ERA_merged = pd.read_pickle('/home/hollythomas/python/pickles/ERA_ua_merged.pkl')
ERA_merged = ERA_merged.rename(columns = {"u_anom":'u10 anom ERA5'})

# ERA_merged_for_mean = ERA_merged[ERA_merged['dist_to_event'] == 0]
# ERA_anom_mean = ERA_merged_for_mean.u_anom.mean()
# print(ERA_anom_mean)
# exit(1)

ERA_mean = ERA_merged.groupby('dist_to_event')['u10 anom ERA5'].mean()
wac_mean = wac_merged.groupby('dist_to_event')['u10 anom CESM2-WACCM'].mean()
cam_mean = cam_merged.groupby('dist_to_event')['u10 anom CESM2-CAM'].mean()



both = pd.concat([cam_mean,wac_mean,ERA_mean],axis=1)
fig = both.plot(y=["u10 anom ERA5",'u10 anom CESM2-WACCM','u10 anom CESM2-CAM'], figsize=(15,4), xlim = [-40,40], color = ['g','b','r'])#, linewidth = .75, color = (0.1, 0.2, 0.5))
plt.show()
exit(1)

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































