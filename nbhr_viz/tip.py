import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

dictionary = pd.read_csv('taxi+_zone_lookup.csv')
data = pd.read_csv( 'trip_cost.csv', names = ['PU','DO','$','tip','week_trips','wend_trips'])

def look_up(loc_id):
    return dictionary[dictionary['LocationID'] == loc_id]['Zone'].values[0]

def look_up2(loc_id):
    return dictionary[dictionary['LocationID'] == loc_id]['Borough'].values[0]

data['PU'] = data['PU'].str[1:]
data['wend_trips'] = data['wend_trips'].str[:-1]

data['wend_trips'] = pd.to_numeric(data['wend_trips'], errors='coerce')
data['PU'] = pd.to_numeric(data['PU'], errors='coerce')
data['DO'] = pd.to_numeric(data['DO'], errors='coerce')

data['occurences'] = data['week_trips'] + data['wend_trips']

data = data[ (data['DO'] != 0) & (data['PU'] != 0)
            & (data['DO'] != 265) & (data['PU'] != 265)
            & (data['DO'] != 264) & (data['PU'] != 264)  ]


data['PU_zone'] = data['PU'].apply(lambda x: look_up2(x))
data['DO_zone'] = data['DO'].apply(lambda x: look_up2(x))

data['PU_nhbr'] = data['PU'].apply(lambda x: look_up(x))
data['DO_nhbr'] = data['DO'].apply(lambda x: look_up(x))
data['fun_ratio'] = data['wend_trips']/(data['week_trips']+data['wend_trips'])

data['trips'] = data['week_trips'] + data['wend_trips']
data['avg_fare'] = data['$'] / data['trips']
data['avg_tip'] = data['tip'] / data['trips']

# %%

import difflib

def f(word):
    '''
    input: string
    output: most likely neighborhood
    '''
    if '/' in word:
        part1 = word.split('/')[0]
        part2 = word.split('/')[1]
        g1 = difflib.get_close_matches(part1, prices['New York, NY - All Homes'])
        g2 = difflib.get_close_matches(part2, prices['New York, NY - All Homes'])
        #if g1 and not :
        if g1 and not g2:
            return g1[0]
        if g2 and not g1:
            return g2[0]
        if not g2 and not g1:
            return 'NULL'

        gm1 = difflib.SequenceMatcher(None, part1, g1[0])
        gm2 = difflib.SequenceMatcher(None, part2, g2[0])
        if gm1.ratio() > gm2.ratio():
            return g1[0]
        else:
        #    print(word, gm1.ratio(), g1, gm2.ratio(), g2)
            return g2[0]

    if difflib.get_close_matches(word, prices['New York, NY - All Homes']):
        return difflib.get_close_matches(word, prices['New York, NY - All Homes'])[0]
    return 'NULL'

zone_prices = {
'Manhattan':1200000, 'Queens':489000, 'Brooklyn':772000, 'Staten Island': 467400, 'Bronx': 328750, 'EWR': 0}

prices = pd.read_excel('new-york-ny.xls')

def price(nhbr):
    if nhbr == 'NULL':
        return 0
    return prices[prices['New York, NY - All Homes'] == nhbr]['Unnamed: 3'].values[0]

data['PU_guess'] = data['PU_nhbr'].apply(lambda w: f(w))
data['PU_val'] = data['PU_guess'].apply(lambda n: price(n))

data.ix[ (type(data['PU_val']) != int)|(data['PU_nhbr'] == 'NULL'), 'PU_val' ] = data['PU_zone'].apply(lambda m: zone_prices[m])

# %%

data['DO_guess'] = data['DO_nhbr'].apply(lambda w: f(w))
data['DO_val'] = data['DO_guess'].apply(lambda n: price(n))

data.ix[ (type(data['DO_val']) != int)|(data['DO_nhbr'] == 'NULL'), 'DO_val' ] = data['DO_zone'].apply(lambda m: zone_prices[m])

# %%
%matplotlib inline
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="white")

d = data[['DO_val', 'PU_val', 'avg_tip', 'avg_fare', 'wend_trips', 'week_trips']]

# Compute the correlation matrix
corr = d.corr()

# Generate a mask for the upper triangle
mask = np.zeros_like(corr, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(11, 9))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(220, 10, as_cmap=True)

ax.set_xlabel("X Label",fontsize=50)
ax.set_ylabel("Y Label",fontsize=50)
sns.set_context("poster")
# Draw the heatmap with the mask and correct aspect ratio
asdf = sns.heatmap(corr,  mask=mask, cmap=cmap,
            square=True,annot=True,  ax=ax)

plt.savefig("cost_m.png", figsize=(40, 32))

# viz done, auxiliary code below

# %%
tally = data['PU_nhbr'].value_counts().to_frame('name')
tally['guess'] = tally.index.map(lambda w: f(w))
tally.iloc[:20, :]


data = data[ (data['PU_val'] != 0)| (data['DO_val'] != 0)]

# %%
#zillow home value ind seems least sparse
for col in prices.columns:
    print(col, (prices[col] == '---').sum(axis=0))
