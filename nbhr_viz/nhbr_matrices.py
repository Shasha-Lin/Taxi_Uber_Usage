import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns;

#WARNING: YOU MUST HAVE 'taxi+_zone_lookup.csv' from 'http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml'
#YOU MUST HAVE 'nhbr_data2.csv', the output from nhbr_data_PySpark.ipynb

dictionary = pd.read_csv('taxi+_zone_lookup.csv')
data = pd.read_csv( 'nhbr_data2.csv', names = ['PU','DO','$','passengers','week_trips','wend_trips'])

def look_up(loc_id):
    return dictionary[dictionary['LocationID'] == loc_id]['Zone'].values[0]

data = pd.read_csv( 'nhbr_data2.csv', names = ['PU','DO','$','passengers','week_trips','wend_trips']
                  )
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

sns.set()
default = sns.cubehelix_palette(light=1, as_cmap=True)

def matrix(string, colors=default, avg=False, centered=False):
    display = data.sort_values(by=string, ascending=False)  # , ascending=[1, 0])
    if string == 'fun_ratio':
        display = data.sort_values(by='occurences', ascending=False)
    display = display.iloc[0:150, :]

    flights = display.pivot("PU_nhbr", "DO_nhbr", string).fillna(0)
    for index, row in flights.iterrows():
        for col in row.index:
            value = data[(data['DO_nhbr'] == col) & (
                data['PU_nhbr'] == index)][string].values[0]
            # passenger per trip or $ per trip
            if avg:
                value = value / data[(data['DO_nhbr'] == col) & (
                    data['PU_nhbr'] == index)]['occurences']
            flights = flights.set_value(
                index, col, value)

    # to stop centering the heatmap at
    # median value, comment out center argument
    fig, ax = plt.subplots(figsize=(10, 10))
    if centered:
        ax = sns.heatmap(flights, cmap=colors, center=display[string].median())
    else:
        ax = sns.heatmap(flights, cmap=colors)  # , center = display[string].median())#,cmap=cmap)
    plt.title('2016 Jul-Dec, total %s' % (string))

    plt.ylabel('Pick Up Location')
    plt.xlabel('Drop Off Location')
    plt.xticks(rotation=90)
    plt.yticks(rotation=0)
    if avg:
        plt.title('2016 Jul-Dec, avg %s per trip' % (string))
    output = ax.get_figure()
    output.savefig('%s, AVG = %s, Centered = %s.png' % (string, avg, centered))

matrix('occurences')
matrix('$')
matrix('$', avg=True)
matrix('passengers')
matrix('passengers', avg=True)
matrix('fun_ratio', 'RdBu_r')
