import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

location = pd.read_table('/Users/carolineroper/Documents/Big Data/by_location_from_to.out', header = -1, skiprows=0)
location.columns = ['Pickup Location', 'Dropoff Location', 'Rides']

location_lookup = \
pd.read_csv('/Users/carolineroper/Documents/Big Data/taxi+_zone_lookup.csv')

location_merged = pd.merge(location, location_lookup[['LocationID', 'Borough', 'Zone']],\
                        left_on=['Pickup Location'],\
                        right_on=['LocationID'])

location_merged = pd.merge(location_merged, location_lookup[['LocationID', 'Borough', 'Zone']],\
                        left_on=['Dropoff Location'],\
                        right_on=['LocationID'])

location_merged = location_merged[['Pickup Location', 'Dropoff Location', 'Rides', 'Borough_x', 'Zone_x', 'Borough_y', 'Zone_y']]

location_merged.columns \
= ['Pickup Location', 'Dropoff Location', 'Rides', 'Pickup Borough', 'Pickup Zone', 'Dropoff Borough', 'Dropoff Zone']

by_borough = \
pd.pivot_table(location_merged, \
               values='Rides', \
               index=['Pickup Borough'], \
               columns=['Dropoff Borough'], \
               aggfunc=np.sum).fillna(0)

plt.figure(figsize=(8,8))
plt.title('Rides by Borough'.format(size = 20))
plt.xlabel('Pickup', size = 14)
plt.ylabel('Dropoff', size = 14)
plt.yticks(verticalalignment='center')
plt.xticks(rotation=-20)

sns.heatmap(by_borough,
            annot=False,
            fmt='.0f',
            linewidths=.5,
            square=True,
            cbar_kws={"orientation":'vertical'}
            )
plt.savefig('heatmap_borough_all.png')

by_borough_skip_manhattan = by_borough
by_borough_skip_manhattan.loc['Manhattan', 'Manhattan'] = 0

plt.figure(figsize=(8,8))
plt.title('Rides by Borough Excluding Manhattan-Manhattan Rides'.format(size = 20))
plt.xlabel('Pickup', size = 14)
plt.ylabel('Dropoff', size = 14)
plt.yticks(verticalalignment='center')
plt.xticks(rotation=-20)

sns.heatmap(by_borough_skip_manhattan,
            annot=False,
            fmt='.0f',
            linewidths=.5,
            square=True,
            cbar_kws={"orientation":'vertical'}
            )

plt.savefig('heatmap_borough_exc_manhattan.png')
