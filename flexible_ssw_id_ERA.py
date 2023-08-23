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
######################################################
######GET U DF##########
# sys.path.append('/home/hollythomas/python/functions')
# from get_months import is_winter

# # levs = [1,3,5,10,20,50,100]
# # for i in levs:  
# dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/u/u10'#.format(i)

# #location of ua wind file 

# dfs = sorted(glob(f'{dpath}/*.nc'))
# # glob retrieves files that match this pattern, sorts them 
# # sorted 
# dask.config.set(**{'array.slicing.split_large_chunks': False})
# df2 = xr.open_mfdataset(dfs, combine='by_coords') 
# df = df2.resample(time = '1D').mean() 
# winter = df.sel(time = is_winter(df['time.month']))
# # create the dataset by opening the multiple files and and combine the series by coordinates 
# # open_mfdataset opens multiple file data sets

# # df.ua['time.month'] returns the month array (1,1,1,1.....12,12,12) associate with ua datetime64
# #applying is_winter function takes the months specified in function 
# lat60 = winter.u.sel(latitude = 60, method ='nearest')
# # # identify and return dataarray consisting of ua values at the lat closest to 60 

# uats = lat60[:,:].mean(axis=-1)
# # # # create the array of ua wind averaged across longitudes 
#u10 = (uats.to_dataset(dim = None)).to_dataframe(dim_order = None).reset_index()

# u_o = xr.open_dataset('/home/hollythomas/data/ERA/u/u.u10.60N.avg.ERA.1959-2021.nc').to_dataframe().drop(columns={'latitude'})
# print(u_o)
# u_anom = pd.read_pickle('/home/hollythomas/data/ERA/u/ua_u10_anomaly_ERA_1959-2021.pkl').drop(columns={'dayofyear'}).rename(columns = {'u':'u_anom'})
# print(u_anom)

# df = u_o.merge(u_anom, left_index=True, right_index=True)
# df = df.to_pickle('/home/hollythomas/data/ERA/u/u10wanom_60N_ERA_1959-2021.pkl')
# exit(1)


########## initial filtering for event strengths #######################
# u10 = xr.open_dataset('/home/hollythomas/data/ERA/u/u.u10.60N.avg.ERA.1959-2021.nc')
u10 = pd.read_pickle('/home/hollythomas/data/ERA/u/u10wanom_60N_ERA_1959-2021.pkl').reset_index()
#get previously complied array of u10 60N zonal average 
u10['time'] = u10['time'].astype(str).str[0:10].astype(str)

def get_dates(df,uf):
    uf = uf
    # do we want to filter by strength of reversal 

    tf = 'no'

    quantile = .5
    #remeber this is backwards since the values are (-) 

    df_with_count, dates, major_df = intial_filter(quantile, tf, uf, u10.copy(),(u10['u']), 'no')
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

    #original = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/sswdates_CESM2-WACCM_amip_r1i1p1f1_gn_19500101-20150101.plk')
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

    merged['u'] = merged['u'].fillna(merged['ub4'])
    merged = merged.drop(columns = {'dist_to_event_2','u_anom','event_id_2'}).reset_index().drop(columns = 'ub4').rename(columns ={'index':'time'})

    meta = climaafter_with_meta.drop(columns = {'u','u_anom','ssw','count','event_id','day','dist_to_event'})
    meta = meta.merge(major_df, on = 'time')
    meta = meta.drop(columns = {'index','ssw','sign','count','pos_count','drop_idx','ssw_actual'}).rename(columns = {'time':'ssw day 0'})
    return merged, major_df, x, climab4_with_meta, before_dates, meta, after_df, after_dates

merged, major_df, x, climab4_with_meta, before_dates, ERA_meta, after_dates, after_df = get_dates(u10, "no")

#major_df_f, climab4_with_meta_f, before_dates_f = get_dates(u10, "yes")
# # ####################################################################################################################
# # ###### ATM VERTICAL FIELD ANOMALY #########
# # ####################################################################################################################
# # ################## WACCM ##################

# field = 'z'
# levs = [1000,700,500,250,100,50,10]

# for i in levs:
#     dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/{}/z{}'.format(field,i) 
#     #dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/CESM2/amip/r1i1p1f1/day/ua/gn'

#     dfs = sorted(glob(f'{dpath}/*.nc'))
#     # glob retrieves files that match this pattern, sorts them 
#     dask.config.set(**{'array.slicing.split_large_chunks': False})

#     df = xr.open_mfdataset(dfs, combine='by_coords')
#     #get only the polar cap 

#     print('-----------------------------------------------iteration marker----------------------------------------')
#     print('original')

#     df = df.z[:,:165,:]
#     df.z = df.z / 9.8
   
#     df = df.resample(time = 'D').mean()
#     # resample the tempresolution to daily 

#     df['time'] = df['time'].astype("datetime64[ns]")


#     weights = np.cos(np.deg2rad(df.latitude))
#     weights.name = "weights"
#     df = df.weighted(weights)
#     #weight the data frame

#     df = df.mean(('longitude','latitude')) 
#     #calculate the mean of the polar cap 

#     df = df.groupby("time.dayofyear") - df.groupby("time.dayofyear").mean("time")

#     df_ssw = df.where(df.time.isin(merged['time']), drop = True)

#     df_new = df_ssw.to_dataframe('{}'.format(i)).drop(columns = "dayofyear")
#     #change array to a data frame which should be a column of anomalies and dates, the column name is the p level

#     new_col = i

#     df_new = df_new.rename(columns= {'z_df':new_col}).to_pickle('/home/hollythomas/data/ERA/z' + str(i) +'_50Ncapanom.pkl')
#     #this is the df of only the dates we're concerned about, at 60N and upwards 
#     #print(df.describe())
#     #print(df)

#     print(df_new)

# # df1= df_ssw.unstack()
# # df1.columns = df1.columns.map('{0[0]}_{0[1]}'.format)
# # df1 = df1.reset_index().rename(columns = {'ta_10000.0':'100','ta_5000.0':'50','ta_20.0':'20', 'ta_1000.0':'10', 'ta_5.0':'5', 'ta_7.0':'7', 'ta_3.0':'3', 'ta_1.0':'1'})
# # df1 = df1.merge(merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
# # df1 = df1.groupby('dist_to_event').mean()
# # new_cols = [str(100),str(50),str(20),str(10),str(7),str(5),str(3),str(1)]
# # df1=df1[new_cols]
# # # fig = df1.plot.line()
# # plt.title('CESM-CAM SSW(u'+str(i)+ ' key)Strat Temp (K) anomaly N ={}'.format(x))
# # # plt.show()  


df10 = pd.read_pickle('/home/hollythomas/data/ERA/z10_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
df50 = pd.read_pickle('/home/hollythomas/data/ERA/z50_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
df100 = pd.read_pickle('/home/hollythomas/data/ERA/z100_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
df250 = pd.read_pickle('/home/hollythomas/data/ERA/z250_50Ncapanom.pkl')
df500 = pd.read_pickle('/home/hollythomas/data/ERA/z500_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
df700 = pd.read_pickle('/home/hollythomas/data/ERA/z700_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
df1000 = pd.read_pickle('/home/hollythomas/data/ERA/z1000_50Ncapanom.pkl')#.drop(columns = "dist_to_event")
#df100 = pd.read_pickle('/home/hollythomas/data/ERA/ta/amip/t100_60Ncapavg_amip_r1_1959-2021.pkl')#.drop(columns = "dist_to_event")

dfs = [df10,df50,df100,df250,df500,df700,df1000]
df = ft.reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True), dfs).reset_index()#.to_pickle('/home/hollythomas/data/ERA/ta/amip/ta_polarcap(60N)avg_amip_r1_1959-2021.pkl')
#this will merge 
#df = df.groupby('dist_to_event').median()
df = df[df['time'].isin(merged['time'])]
df = df.merge(merged, on = "time").drop(columns = {'u_anomb4','u'})
df = df.groupby('dist_to_event').mean()
df = df.stack(level=- 1, dropna=True).drop(columns= 'u_anomb4')
df = df.to_frame().reset_index()
df = df.rename(columns={'level_1':'plev'})
df = df.rename(columns={0: "z" })

levels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10., 15, 20, 25, 30., 35, 40., 50., 60., 70., 85, 100., 150, 200, 300, 450., 500, 800,]
fig, ax = plt.subplots(figsize=(15, 3))

cmap = plt.get_cmap('RdBu_r')
norm = BoundaryNorm(levels, ncolors=cmap.N, clip=True)

tcf = ax.tricontourf(df.dist_to_event, df.plev, df.z, levels, 
    linewidths=0.5, 
    cmap='RdYlBu_r',
    norm=norm,
    extend='both')

ax.invert_yaxis()
ax.axvline(x = 0, color='r', linewidth=2)
fig.colorbar(tcf)
ax.set_title('ERA Geopotential Height Anomaly N ={}'.format(x))
ax.set_ylabel('Pressure (hPa)')
ax.set_xlabel('Days from Event (day 0)')
#ax.tight_layout()
#cbar = plt.colorbar(orientation='vertical')
plt.show()

exit(1)

# # # ####################################################################################################################
# # # ###### HOVMULLER #########
# # # ####################################################################################################################
# df_ssw['plev'] = df_ssw['plev'] * .01 #.replace([10000.0, 5000.0, 1000.0], [100, 50, 10])
# df_ssw = df_ssw.merge(cam_merged, how = 'inner', on = 'time').set_index('time').drop(columns = {'ua'})#.to_pickle('/home/hollythomas/data/ERA/t10_anom_80daywindow_ssws_1959-2021.pkl')
# df_ssw = df_ssw.groupby(['dist_to_event','plev']).mean().reset_index()

# levels = list(range(-40,250))
# fig, ax = plt.subplots(figsize=(15, 3))
# tcf = ax.tricontourf(df_ssw.dist_to_event, df_ssw.plev, df_ssw.zg, levels, linewidths=0.5, cmap='RdYlBu_r')
# ax.axvline(x = 0, color='r', linewidth=2)
# ax.invert_yaxis()
# fig.colorbar(tcf)
# ax.set_title('CAM Polar Cap Temp Anomaly (K) N ={}'.format(n_wac))
# ax.set_ylabel('Pressure (hPa')
# ax.set_xlabel('Days from Event (day 0)')
# #ax.tight_layout()
# #cbar = plt.colorbar(orientation='vertical')
# plt.show()

# exit(1)



####################################################################################################################
###### FIELD ANOMALY #########
####################################################################################################################
# pre_df = pre_ssw_dates.to_dataframe()
# pre_df.columns = ['time']
# pre_df['time'] = pre_df['time'].dt.strftime("%Y-%m-%d")

# post_df = post_ssw_dates.to_dataframe()
# pre_df.columns = ['time']
# post_df['time'] = post_df['time'].dt.strftime("%Y-%m-%d")

# pre_series = pd.Series(pre_ssw_dates)
# pre_dates = pd.to_datetime(pre_series, format='%Y-%m-%d')
# pre_dates = pre_dates.dt.strftime('%Y-%m-%d')

# post_series = pd.Series(post_ssw_dates)
# post_dates = pd.to_datetime(post_series, format='%Y-%m-%d')
# post_dates = post_dates.dt.strftime('%Y-%m-%d')

# dpath = '/home/hollythomas/REPO/CMIP6/CMIP6/{}/amip/r{}i1p1f1/day/zg/gn/'.format(model, sim)
# dfile = sorted(glob(f'{dpath}zg_day_{model}_amip_r{sim}i1p1f1_gn*.nc'))
# df = xr.open_mfdataset(dfile, combine='by_coords')

# dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/z/z500/'
# # day_agg = sorted(glob(f'{dpath}z.500hPa.6hr.ERA5*.nc'))
# df = xr.open_mfdataset(day_agg, combine = 'by_coords')
#################
# dpath = '/home/hollythomas/REPO/reanalysis/ERA5/6hourly/t2m/'
# day_agg = sorted(glob(f'{dpath}*199*.nc'))
# df = xr.open_mfdataset(day_agg, combine = 'by_coords')

# #df = df.sel(time = is_winter(df['time.month']))
# #selects nov - may

# df['day'] = df['time.dayofyear']
# df = df.resample(time = 'D').mean()
# #get numerical day values
# #opens all t2m dfs
# df = df.sel(time = is_winter(df['time.month'])) 
# #selects nov - may
# NH_mask = (df['latitude'] >= 10)
# #mask to get only the northern latitudes 
# #l_mask = (df['longitude'] == 359.75)
# df = df.where(NH_mask, drop = True)
# #chop the df so it's only the NH
# # df.coords['time'] = df.time.dt.floor('1D')
# # #make every timestamp JUST yy/mm/dd and remove hour stamp
# # df  = df.groupby('time').mean()
# # #groupby and average the time so it's daily rather than 6hourly 
# df = df.resample(time = '1D').mean()

# df = df.rolling(time = 10).mean()
# #create a rolling window of times 
# clim = df.groupby('time.dayofyear').mean('time')
# #get the daily climatology for each day of the year
# anomalies = df.groupby('time.dayofyear') - clim
# #anomlies are calculated by subtracting the climatology from each independant day
# anomalies = anomalies.drop_vars('dayofyear')
# #get rid of unessary variable
# #anomalies = anomalies.to_dataframe().reset_index()				

# #anomalies['time'] = anomalies["time"].dt.strftime("%Y-%m-%d")
# #change the time format so the formats match and we can select dates that are associated with SSWs 
# anom_pre = anomalies.where(anomalies.time.isin(before_dates), drop = True)

# #get the dataframe where the dates are associated in the "pre dates" list
# #b4_anoms = b4_anoms.merge(before, on = 'time')

# #anom_post = anomalies.where(anomalies.time.isin(post_dates), drop = True)
# #same as above
# #after_anoms = after_anoms.merge(after, on = 'time')


# b4_plt = anom_pre.mean(dim = 'time')#.to_dataframe()

# #after_plt = anom_post.mean(dim = 'time')

# # numpy = b4_plt.msl.values
# # print(numpy)
# # exit(1)
# # # var-var.values.mean(axis=-1)[...,None]
# # # print(numpy.shape)
# # zonal_clean = numpy - numpy.mean(axis = 0)[...,None]
# # # print(zonal_clean.shape)
# # # exit(1)
# # b4_plt['msl'] = xr.DataArray(zonal_clean, dims = ['lat','lon'])

# #b4_plt = b4_plt * .01

# fig = plt.figure(figsize=[7,5])

# # 111 means 1 row, 1 col and index 1
# ax = fig.add_subplot(projection = ccrs.Orthographic(central_longitude= 0, central_latitude=90))

# b4_plt['t2m'].plot(ax=ax,  cmap='coolwarm',
#                     transform=ccrs.PlateCarree(), extend = 'both')
# ax.coastlines()
# ax.gridlines()
# ax.set_title('ERA 40d pre z500 Anomaly (Pa), N = {}'.format(x))
# plt.show()
# exit(1)
######################################################################################
################### Heat Flux Anomaly 
######################################################################################

def get_flux_anom(b4, dates, overall):
    # dpath = '/home/hollythomas/REPO/reanalysis/ERA5/daily/filtered/stormtrack/VT100/lp_10_days/'
    # dfile = sorted(glob(f'{dpath}lp_10_days.VT*.nc'))

    df = xr.open_mfdataset('/home/hollythomas/data/ERA/vt100/new_vt100_day_ERA_19790101-20211211.nc')
    df['time'] = df['time'].astype("datetime64[ns]")

    #df = df.rolling(time = 40).mean()
    #calculate rolling average with 40 day window
    
    weights = np.cos(np.deg2rad(df.latitude))
    weights.name = "weights"
    df_weighted = df.weighted(weights)
    #weight the dataframe to consider latitude area effect 
    weighted_mean = df_weighted.mean(('longitude','latitude'))
    #calculate field average
    daily_anoms = weighted_mean.groupby('time.dayofyear') - weighted_mean.groupby("time.dayofyear").mean("time")
    #get EHF anomalies
 

    if overall == "yes": 
        df = daily_anoms.rolling(time = 40).mean()
        #rolling mean to integrate 40 day periods 
        
        df = df.to_dataframe().reset_index().set_index('time').drop(columns = {'dayofyear'}).dropna()
        #convert to df
    
        #print(df['vt100'].isna().sum())
        df = df.rename(columns = {'__xarray_dataarray_variable__':'VT100'})
       
    else:    
        daily_anoms = daily_anoms.where(daily_anoms['time'].isin(dates), drop = True)
        
        #get df of just the dates associated with "weak" vortex events
        df = daily_anoms.to_dataframe().reset_index().set_index('time').drop(columns = {'dayofyear'})
       
        bf = b4.set_index('time').drop(columns = {'count','ssw','u'})
        #merge with df that has info on event ids
        df = df.merge(bf, left_index = True, right_index = True)#.drop(columns = "dist_to_event")
       
        df['dist_to_event'] = df['dist_to_event'] * -1
        #putem' together
        
        df = df.groupby(['event_id']).mean()
        #get the average for the 40 days leading up to each event 
       
        df = df.rename(columns = {'__xarray_dataarray_variable__':'VT100'})
        
    return df

df_ERA = get_flux_anom(climab4_with_meta, before_dates, 'no').reset_index()#.set_index('dist_to_event')

# df_ERA_f = get_flux_anom(climab4_with_meta_f, before_dates_f, 'no').reset_index()

df_ERA_ovr = get_flux_anom(climab4_with_meta, before_dates, 'yes')

df_ERA_plt = df_ERA.drop(columns = {'dist_to_event','u_anom','event_id'})
#df_ERA_plt_f = df_ERA_f.drop(columns = {'dist_to_event','u_anom','event_id'})

# ERA_threshold = df_ERA_ovr.VT100.quantile(q = .95)
# count_ERA = (df_ERA['VT100'] > (df_ERA_ovr.VT100.quantile(q = .95))).sum()

# per_over =  100*(count_ERA/len(df_ERA))

muE, stdE = norm.fit(df_ERA_plt.VT100.values)

# muEF, stdEf = norm.fit(df_ERA_plt_f.VT100.values)

muALLe, stdALLe = norm.fit(df_ERA_ovr.VT100.values)

# #plt.hist(W_anoms_df.values, density=True, alpha=.6, color ='b')

xmin, xmax = plt.xlim(left = -20, right = 17)
#xmin, xmax = plt.xlim(left = -.75, right = .75)
#gives us xrange 
x = np.linspace(xmin, xmax, 100)
#gives us x range to feed into f(X) for the pdf of the two distributionsd
pE = norm.pdf(x, muE, stdE)
#calculate the pdf of WACCM dates
# pEf = norm.pdf(x,muEF,stdEf)
pEa = norm.pdf(x, muALLe, stdALLe)


fig, ax = plt.subplots()

# Data and histograms
for_label = ['ERA Events, n = 40']
_ = ax.hist(df_ERA_plt.VT100.values, density=True, alpha=.6, color='g', label=for_label)
_ = ax.plot(x, pE, 'k', linewidth=1, color='g')
_ = plt.axvline(df_ERA_plt.VT100.values.mean(), color='g', linestyle='dashed', linewidth=2)

for_label2 = ['All winter days']
_ = ax.hist(df_ERA_ovr.values, density=True, color='purple', alpha=.3, label=for_label2)
_ = ax.plot(x, pEa, 'k', linewidth=1, color='purple')
_ = plt.axvline(df_ERA_ovr.VT100.values.mean(), color='purple', linestyle='dashed', linewidth=2)

# Customize the appearance
ax.set_facecolor('white')  # Set the background color to white
ax.spines['top'].set_visible('black')
ax.spines['right'].set_visible('black')
ax.spines['bottom'].set_color('black')  # Set the color of the bottom spine to black
ax.spines['left'].set_color('black')    # Set the color of the left spine to black

# Set labels and limits
ax.set_xlabel('Meridional Heat Flux Anomaly')
ax.set_ylabel('PDF f(x)')
plt.ylim(top=0.3, bottom=0.0)

# Show the legend
#ax.legend()

# Show or save the plot
plt.show()


ax.set_facecolor('white')
ax.spines['top'].set_visible(True) and ax.spines['right'].set_visible(True)
ax.spines['bottom'].set_color('black') and ax.spines['left'].set_color('black')
exit(1)





fig = plt.figure()
ax = fig.add_subplot()


# ##### WACCM ########
for_label = ['ERA Events, n = 40']
_ = ax.hist(df_ERA_plt.VT100.values, density=True, alpha=.6, color ='g', label = for_label)
_ = ax.plot(x, pE, 'k', linewidth = 1, color = 'g')
_ = plt.axvline(df_ERA_plt.VT100.values.mean(), color='g', linestyle='dashed', linewidth=2)

# _ = ax.hist(df_ERA_plt_f.VT100.values, density=True, alpha=.6, color ='r')
# _ = ax.plot(x, pEf, 'k', linewidth = 1, color = 'r')

for_label2 = ['All winter days']
_ = ax.hist(df_ERA_ovr.values, density= True, color='purple', alpha = .3, label = for_label2)
_ = ax.plot(x, pEa, 'k', linewidth = 1, color = 'purple')
_ = plt.axvline(df_ERA_ovr.VT100.values.mean(), color='purple', linestyle='dashed', linewidth=2)

#_ = ax.legend(prop={'size': 10})
#_ = ax.legend(['ERA all Events, n = 28','ERA Strong events, n = 14'] )
_ = ax.set_xlabel('Meridional Heat Flux Anomaly')
_ = ax.set_ylabel('PDF f(x)')

plt.ylim(top= 0.3, bottom = 0.0)
plt.show()
#plt.savefig('/home/hollythomas/data/figures/ms_thesis/ERA_vt100')
exit(1)
####################################################################################
########################### UA plotting ######################################
####################################################################################
def make_ua_anom_plt(b4_df, after_df, model):
    before = b4_df
    before['time'] = pd.to_datetime(before.time)
    before = before.set_index(before['time']).rename(columns = {'u':'ub4', 'u_anom':'u_anomb4', 'event_id':'event_id_2'}).drop(columns = {'ssw','count'})
    before['dist_to_event'] = before['dist_to_event'] * -1

    after = after_df.reset_index()
    after['time'] = pd.to_datetime(after.time)
    after = after.set_index(after['time'])
  

    both = [before,after]
    merged = pd.concat(both, axis = 1).drop(columns = {'time'})
    merged['event_id'] = merged['event_id'].fillna(merged['event_id_2'])
    merged['dist_to_event'] = merged['dist_to_event'].fillna(merged['dist_to_event_2'])
    merged['u'] = merged['u'].fillna(merged['ub4'])
    merged['u_anom'] = merged['u_anom'].fillna(merged['u_anomb4'])
    merged = merged.drop(columns = {'u_anomb4'})
    merged = merged
    merged = merged.drop(columns = {'dist_to_event_2'}).reset_index().drop(columns = {'ub4','event_id_2'})
    merged = merged[(merged['time'].dt.month != 5)]
    merged = merged.rename(columns = {'u': model})
  #  merged = merged.set_index('dist_to_event')
    return merged

ERA_merged = make_ua_anom_plt(climab4_with_meta, after_df, 'u_ERA')
pkl = ERA_merged.to_pickle('/home/hollythomas/python/pickles/ERA_ua_merged.pkl')
ERA_mean = ERA_merged.groupby('dist_to_event')['u_ERA'].mean()

fig = ERA_mean.plot(figsize=(15,4), xlim = [-40,40], linewidth = .75, color = (0.1, 0.2, 0.5), label = 'u wind speed & direction')
plt.show()
exit(1)













































