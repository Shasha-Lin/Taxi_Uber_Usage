import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

dictionary = pd.read_csv('taxi+_zone_lookup.csv')
data = pd.read_csv( 'yellow_2016.csv', names = ['PU','DO','$','tip','week_trips','wend_trips'])

def look_up(loc_id):
    return dictionary[dictionary['LocationID'] == loc_id]['Zone'].values[0]

data['PU'] = data['PU'].str[1:]
data['wend_trips'] = data['wend_trips'].str[:-1]

data['wend_trips'] = pd.to_numeric(data['wend_trips'], errors='coerce')
data['PU'] = pd.to_numeric(data['PU'], errors='coerce')
data['DO'] = pd.to_numeric(data['DO'], errors='coerce')

data['occurences'] = data['week_trips'] + data['wend_trips']

data = data[ (data['DO'] != 0) & (data['PU'] != 0)
            & (data['DO'] != 265) & (data['PU'] != 265)
            & (data['DO'] != 264) & (data['PU'] != 264)  ]

data['PU_nhbr'] = data['PU'].apply(lambda x: look_up(x))
data['DO_nhbr'] = data['DO'].apply(lambda x: look_up(x))
data['fun_ratio'] = data['wend_trips']/(data['week_trips']+data['wend_trips'])

# %%

data['PU_nhbr'].value_counts()


# %%

data['PU_guess'] = str()

# %%
# def d(i):
#     filt = ['East', 'South', 'North', 'West']
#     for w in filt:
#         i = i.replace(w,"")
#     return i
#
# data.PU_nhbr = data.PU_nhbr.apply(lambda word: d(word))

import difflib
prices = pd.read_excel('new-york-ny.xls')

for n in data['PU_nhbr'].values:
    if '/' in n:
        part1 = n.split('/')[0]
        part2 = n.split('/')[1]
        g1 = difflib.get_close_matches(part1, prices['New York, NY - All Homes'])[0]
        gm1 = difflib.SequenceMatcher(None, part1, g1)
        g2 = difflib.get_close_matches(part2, prices['New York, NY - All Homes'])[0]
        gm2 = difflib.SequenceMatcher(None, part2, g2)
        if gm1.ratio > gm2.ratio:
            
        else:

        continue

    g = difflib.get_close_matches(n, prices['New York, NY - All Homes'])[0]

# %%

prices['New York, NY - All Homes']
#prices.columns
