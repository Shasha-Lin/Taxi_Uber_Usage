import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#Load each company's file

yellow_by_date_pickup = \
pd.read_table('/Users/carolineroper/Documents/Big Data/by_date_pickup_yellow.out', header = -1, skiprows=0)
yellow_by_date_pickup.columns = ['Date', 'Yellow Pickups']

green_by_date_pickup = \
pd.read_table('/Users/carolineroper/Documents/Big Data/by_date_pickup_green.out', header = -1, skiprows=0)
green_by_date_pickup.columns = ['Date', 'Green Pickups']

fhv_by_date_pickup = \
pd.read_table('/Users/carolineroper/Documents/Big Data/fhv_pickups_15_16.out', header = -1, skiprows=0)
fhv_by_date_pickup.columns = ['Date', 'FHV Pickups']

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

