import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

'''Run this code using command python trips_per_day_and_month_viz.py while you are in the directory where the following files are stored:
by_date_pickup_yellow.out
by_date_pickup_green.out
by_date_pickup_fhv.out
by_date_citibike.out'''

#Load each company's file

yellow_by_date_pickup = \
pd.read_table('by_date_pickup_yellow.out', header = -1, skiprows=0)
yellow_by_date_pickup.columns = ['Date', 'Yellow Pickups']

green_by_date_pickup = \
pd.read_table('by_date_pickup_green.out', header = -1, skiprows=0)
green_by_date_pickup.columns = ['Date', 'Green Pickups']

fhv_by_date_pickup = \
pd.read_table('by_date_pickup_fhv.out', header = -1, skiprows=0)
fhv_by_date_pickup.columns = ['Date', 'FHV Pickups']

citibike = \
pd.read_table('/Users/carolineroper/Documents/Big Data/by_date_citibike.out', header = -1, skiprows=0)
citibike.columns = ['Date', 'Bike Trips']

#Merge each company's file

all_companies_pickups = pd.merge(yellow_by_date_pickup, green_by_date_pickup,\
                        on=['Date'])

all_companies_pickups = pd.merge(all_companies_pickups, fhv_by_date_pickup,\
                        on=['Date'])

all_companies_pickups.columns = ['Date', 'Yellow', 'Green', 'FHV']

all_companies_pickups['Date'] = pd.to_datetime(all_companies_pickups['Date'])

#Create the daily figure

plt.figure(figsize=(12,6))
plt.gca().set_color_cycle(['#f2cd32', '#38ba89', '#226ba3'])

plt.plot(all_companies_pickups.Date, all_companies_pickups.Yellow)
plt.plot(all_companies_pickups.Date, all_companies_pickups.Green)
plt.plot(all_companies_pickups.Date, all_companies_pickups.FHV)

plt.title('Pickups Over Time by Cab Company')

plt.legend(loc=1)

plt.savefig('Pickups_Per_Day.png')

#Add in CitiBike

citibike.Date = pd.to_datetime(citibike['Date'])

all_transportation_start = pd.merge(all_companies_pickups, citibike,\
                        on=['Date'])

all_transportation_start.columns = ['Date', 'Yellow', 'Green', 'FHV', 'CitiBike']

plt.figure(figsize=(12,6))
plt.gca().set_color_cycle(['#f2cd32', '#38ba89', '#226ba3', '#e70000'])

plt.plot(all_transportation_start.Date, all_transportation_start.Yellow)
plt.plot(all_transportation_start.Date, all_transportation_start.Green)
plt.plot(all_transportation_start.Date, all_transportation_start.FHV)
plt.plot(all_transportation_start.Date, all_transportation_start.CitiBike)

plt.title('Trips Over Time by Transporation Method')

plt.legend(loc=1)

plt.savefig('Trips_Over_Time_Transportation_Method.png')

#Create monthly aggregation

all_companies_pickups['year'], all_companies_pickups['month']\
= all_companies_pickups['Date'].dt.year.astype(str), all_companies_pickups['Date'].dt.month.astype(str)

all_companies_pickups['month'] = all_companies_pickups.month.str.zfill(2)
monthly = pd.pivot_table(all_companies_pickups, values=['Yellow', 'Green', 'FHV'], index=['year', 'month'],aggfunc=np.sum)
monthly = monthly.reset_index(level=['year', 'month'])
monthly['month_labels'] = monthly.year + '-' + monthly.month

#Create the monthly figure

plt.figure(figsize=(12,6))
plt.gca().set_color_cycle(['#f2cd32', '#38ba89', '#226ba3'])

plt.plot(np.arange(24), monthly.Yellow)
plt.plot(np.arange(24), monthly.Green)
plt.plot(np.arange(24), monthly.FHV)

plt.xticks(np.arange(24), monthly.month_labels, rotation=25)

plt.title('Pickups Over Time by Cab Company')

plt.legend(loc=1)

plt.savefig('Pickups_Per_Month.png')

#Create monthly aggregation with CitiBike

all_transportation_start['year'], all_transportation_start['month']\
= all_transportation_start['Date'].dt.year.astype(str), all_transportation_start['Date'].dt.month.astype(str)

all_transportation_start['month'] = all_transportation_start.month.str.zfill(2)
monthly_all = pd.pivot_table(all_transportation_start, values=['Yellow', 'Green', 'FHV', 'CitiBike'], index=['year', 'month'],aggfunc=np.sum)
monthly_all = monthly_all.reset_index(level=['year', 'month'])
monthly_all['month_labels'] = monthly_all.year + '-' + monthly_all.month

#Create monthly plot with CitiBike

plt.figure(figsize=(12,6))
plt.gca().set_color_cycle(['#f2cd32', '#38ba89', '#226ba3', '#e70000'])

plt.plot(np.arange(24), monthly_all.Yellow)
plt.plot(np.arange(24), monthly_all.Green)
plt.plot(np.arange(24), monthly_all.FHV)
plt.plot(np.arange(24), monthly_all.CitiBike)

plt.xticks(np.arange(24), monthly_all.month_labels, rotation=25)

plt.title('Trips Over Time by Transportation Method')

plt.legend(loc=1)

plt.savefig('Trips_Per_Month_All.png')
