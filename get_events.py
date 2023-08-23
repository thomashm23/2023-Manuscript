import xarray as xr
import netCDF4 as nc
import scipy as sp
import numpy as np
import pandas as pd
from itertools import cycle 

def check_for_events_to_drop(df):
	df_ma = df[df.month.isin([1,2,3,4,5])]
	#df_ma = df[df.month.isin([3,4])]
	out = []	
	try: # if [3].isin(df_ma.month) & df_ma.ssw.eq(1):
		#index = df_ma.index[(df_ma.ssw.eq(1)) & (df_ma.month == 3)].tolist()
		index = df_ma.index[(df_ma.ssw.eq(1)) & (df_ma.month !=4)].tolist()
		#find the last SSW occurance of the year 
		for i in range(len(index)): 
			#print(i)
			value = index[i]
			# from the list of incies generated in the above line, get the last
			sub = df_ma.loc[value:]
			#subset the df so that is it is just the last date and the next x days until the end of the year 
			if 10.0 in sub.pos_count.values: 
			 	out.append((True, value))
			 	#if the column "pos count" contains 10 that event can be kept as it is NOT a final warming
			 	#if the column does NOT contain 10 that means there have not been 10 positive days and it is a terminal event 
			else:
				out.append((False, value))
		#print('return:', out)
		return out
	except: # else:
		out.append((False, None))
		#print('return:', out)
		return out
	

	

	# try: # if [3].isin(df_ma.month) & df_ma.ssw.eq(1):
	# 	index = df_ma.index[(df_ma.ssw.eq(1)) & (df_ma.month == 3)]
	# 	#find the last SSW occurance of the year 
	# 	index = index[-1]
	# 	# from the list of incies generated in the above line, get the last
	# 	sub = df_ma.loc[index:]
	# 	#subset the df so that is it is just the last date and the next x days until the end of the year 
	# 	if 10.0 in sub.pos_count.values: 
	# 	 	return True, index
	# 	 	#if the column "pos count" contains 10 that event can be kept as it is NOT a final warming
	# 	else:
	# 	 	return False, index
	# 	 	#if the column does NOT contain 10 that means there have not been 10 positive days and it is a terminal event 
	# except: # else:
	# 	return False, None
	# 	#there are no march events this year so skip it 


def intial_filter(q,tf,uf,df,var, mode):
	# df.reset_index()
	df['ssw'] = (var < 0).astype(int).diff() == 1.0
	# if the value of u is negative, it is a reversal 
	df['sign'] = (var <0)
	#bool for where the value sis positive and where the value is negative 
	df['count'] = df['sign'].apply(rolling_count)
	#counts until the sign value is reversed 
	col_is_pos = (var > 0)
	#indexer for where the winds are greater than 0 
	df['between'] = df['count']
	#create clumn that is a duplicate of the 'count' column
	df['between'] = df.between.shift(1)
	#shift the duplicate column up one so that the between row value corresonding to the "true" ssw events is 
	df['pos_count'] = df['count']
	#create a duplicate column to count 
	df.loc[(var < 0), 'pos_count'] = None
	#drop the days in poscount where the u values is negative, making the pos_count only relevant to days in which u winds are westerly 
	df.loc[col_is_pos,'count'] = None
	#remove the values in the count column that are postive for groupby operation that will provide length 
	df = df.reset_index()
	
	df['year'] = df['time'].astype(str).str[0:4].astype(int)
	df['month'] = df['time'].astype(str).str[5:7].astype(int)
	#df['time'] = df['time'].astype(str).str[0:10].astype(str)
	
	df_grp = df.groupby('year').apply(lambda grp: check_for_events_to_drop(grp)).reset_index(name='keep')#.rename(columns={0: 'keep', 1: 'drop_idx'})
	#apply the check function to ensure the last events are NOT terminal events 

	df_grp['keep'].astype(bool)

	df_grp = df_grp[df_grp['keep'].astype(bool)]
	
	df_grp = df_grp.explode('keep')

	df_grp['keep'] = df_grp['keep'].astype(str)

	df_grp['keep'] = df_grp['keep'].str.replace('[( )]','')

	df_grp[['keep','drop_idx']] = df_grp['keep'].str.split(',', expand=True)#.rename(columns={0: 'keep', 1: 'drop_idx'})

	df_grp = df_grp[df_grp['drop_idx'].notna()]

	df_grp['drop_idx'] = df_grp['drop_idx'].astype(int)

	#df_grp['keep'] = df_grp['keep'].astype(bool)

	number = range(len(df))
	
	df['drop_idx'] = number
	
	if mode == 'model':
		df_4drop =  pd.merge(df_grp, df, on = ['drop_idx'], how = 'inner').drop(columns = {"drop_idx", 'ua', 'ssw','sign','between','month', 'count'})
	else: 
		df_4drop =  pd.merge(df_grp, df, on = ['year','drop_idx'], how = 'inner').drop(columns = {"drop_idx", 'u', 'ssw','sign','between','pos_count','month', 'count'})

	df_4drop = df_4drop[df_4drop['keep'] == 'False']

	bad_dates = df_4drop['time']

	ssw_list = df[(df['ssw'] == True)].reset_index()
	# # create df where of only the dates where it does from positive to negative 
	
	prelim_event_dates = ssw_list['time']
	# #this is where I start to change things
	
	df['ssw_actual'] = df['time'].isin(prelim_event_dates)

	major_events = df.loc[(df["ssw"] == True) & (df['month'] != 4) & (df['month'] != 5) & (df['between'] >= 20) & (~df['time'].isin(bad_dates))]

	# ta_df = pd.read_pickle('/home/hollythomas/data/CESM-WACCM/t/ta_WACCM_anom_Pcap_amip_allsims.pkl')
	# #ta_df = pd.read_pickle('/home/hollythomas/data/CESM_CAM/t/ta_CAM_Pcapavg_amip_allsims.pkl')

	# ta_df['time'] = ta_df['time'].astype(str)

	# major_events = major_events.merge(ta_df, on = "time")
	# print(major_events.u_anom.describe())

	if uf == 'yes': 
		u_threshold = major_events.u_anom.quantile(q = q)
		#u_threshold = major_events.u.quantile(q = q)

		major_events = major_events.where(major_events.u_anom <= u_threshold)
		#major_events = major_events.where(major_events.u <= u_threshold)
		
		major_events = major_events[major_events['u_anom'].notna()]
		#major_events = major_events[major_events['u'].notna()]
		
	else:
		major_events = major_events

	# if tf == 'yes': 
	# 	t_threshold = major_events.ta.quantile(q = q)
	# 	#u_threshold = major_events.u.quantile(q = q)

	# 	major_events = major_events.where(major_events.ta >= t_threshold)
	# 	#major_events = major_events.where(major_events.u <= u_threshold)
		
	# 	major_events = major_events[major_events['ta'].notna()]
	# 	#major_events = major_events[major_events['u'].notna()]
	# else:
	# 	major_events = major_events

	df = df.drop(columns = ['drop_idx', 'sign','between','pos_count','year','month', 'ssw_actual'])
	dates = major_events['time']

	# df = df.drop(columns = ['ssw_actual'])
	return df, dates, major_events

def get_major_events_ahead(q,lf,dates,df):
	df = df.reset_index()
	
	df['ssw'] = df['time'].isin(dates)
	#create new colum that sets the "ssw actual" column = true where the corresponding date is in the list of dates.
	#bool for where the value sis positive and where the value is negative
	df['event_id'] = np.where(df['ssw'], df['time'].astype(str), None)
	# sets the new "event id" to the value in the time column where the events exist, otherwise sets it to None

	df['event_id'] = df['event_id'].fillna(method='ffill', limit = 40)
	#create a new column that is the event ID as described by the event day 0 date 
	
	new_df = df[df['event_id'].notna()]#.drop(columns =  {'ssw_actual','ssw'})
	#creates a new df out of ONLY rows populated in the event id column, thus ONLY the days relevant to the climatology 

	new_df['length'] = new_df.groupby(['event_id'])['count'].transform('max')
	#this will tell us how long the events last
	

	if lf == 'yes': 
		length_threshold = new_df.length.quantile(q = q)
		#passing_dates = new_df['time'].where([new_df.length >= length_threshold] & [new_df.ssw == True])
		passing_dates = new_df.time[(new_df['length'] >= length_threshold) & (new_df['ssw']==True)]
		#new_df = new_df.where(new_df.length >= length_threshold).dropna()
		new_df = new_df[new_df['event_id'].isin(passing_dates)]
		
	else:
		new_df = new_df

	new_df['day'] = df['time'].astype(str).str[8:10].astype(int)
	#just create a new column with days of the month 

	#new_df['min_between'] = new_df.groupby(['event_id_bwards'])['between'].transform('max')

	#new_df = new_df[~(new_df['event_id'].astype('str')).isin((filter_df['time'].astype('str')))]

	new_df['dist_to_event'] = new_df.groupby('event_id').cumcount()
	#this just tells us the distance (in days) to the event 

	clima_dates = new_df['time']
	
	lf_dates = new_df['event_id'].drop_duplicates()
	new_df = new_df.drop(columns = {'level_0', 'index'})
	#this will give us the 

	#major_events = major_events.drop(columns = {'ssw','count','month','day','between','ssw_actual'})

	#new_df = new_df.drop(columns = {'ssw','count','between'})
	
	#dates = major_events['time']

	return new_df, clima_dates, lf_dates

def get_events_behind(dates,df):
	df = df.reset_index()

	df['ssw'] = df['time'].isin(dates)
	#create new colum that sets the "ssw actual" column = true where the corresponding date is in the list of dates.
	#print(df.head(1000).to_string())
	df['event_id'] = np.where(df['ssw'], df['time'].astype(str), None)
	# sets the new "event id" to the value in the time column where the events exist, otherwise sets it to None
	
	df['event_id'] = df['event_id'].fillna(method='bfill', limit=40)

	new_df = df[df['event_id'].notna()].drop(columns = {'level_0', 'index'})
	#creates a new df out of ONLY rows populated in the event id column

	new_df['dist_to_event'] = new_df.groupby('event_id').cumcount(ascending = False)
	
	clima_dates = new_df['time']

	lf_dates = new_df['event_id'].drop_duplicates()
	
	return new_df, clima_dates, lf_dates


# def get_event_data(df, var):
# 	df = df.reset_index()
# 	df['ssw_actual'] = df['time'].isin(dates)
# 	#create new colum that sets the "ssw actual" column = true where the corresponding date is in the list of dates.

# 	df['event_id'] = np.where(df['ssw_actual'], df['time'].astype(str), None)
# 	# sets the new "event id" to the value in the time column where the events exist, otherwise sets it to None

# 	df['event_id'] = df['event_id'].fillna(method='ffill', limit = 20).fillna(method='bfill', limit=20)
# 	# fills the event id column with the event id date back 20rows and forward 20 rows (need to make sure on this re. actural range)

# 	new_df = df[df['event_id'].notna()]#.drop(columns =  {'ssw_actual','ssw'})
# 	#creates a new df out of ONLY rows populated in the event id column
# 	# new_df['sign'] = (new_df['variable'] <0)
# 	# new_df['count'] = new_df['sign'].apply(rolling_count)
# 	#new_df['event_length'] = new_df.groupby(['event_id','count'])['count'].transform('max')

# 	days = list(range(-10,11))
# 	#creates list of -20 to 20 
# 	to_fromevent = cycle(days)
# 	#itertools cycle will just run through this cycle over and over until the values are filled 
# 	new_df['dist_from_event_1'] = [next(to_fromevent) for fruit in range(len(new_df))]
# 	#fills a new column adjent with values -20 to 20 to give you a day index to and from the event (ie -20 = 20 days before the event)
	
# 	new_df['dist_to_event'] = new_df.groupby('event_id').cumcount() -10
# 	#this is anothe way to acccomplish the above task, still trying to figure out which is best. Basically counts upward from events in grouped object so dit_to_event will be 0-40, then by subtracting 20 
# 	# we fill those values with -20:20
# 	#neither of these are working though because we have overlapping events

# 	return new_df

def is_winter(month):
	return (month == 12) | ((month >= 1) & (month <= 4))

def is_jan(month):
	return(month == 1)

def is_winter_noap(month):
	return (month == 12) | ((month >= 1) & (month <= 3))

def is_feb(month):
	return(month == 2)

def is_march(month):
	return(month == 3)

def is_dec(month):
	return(month == 12)

#Function to return months december, january, february, march
# function name = is_winter
# parameter = month 
def rolling_count(val):
    if val == rolling_count.previous:
        rolling_count.count +=1
    else:
        rolling_count.previous = val
        rolling_count.count = 1
    return rolling_count.count
rolling_count.count = 0 #static variable
rolling_count.previous = None #static variable