import pandas as pd
import glob
import os

''''Change directory_name to place where results of aggregation.py are stored.'''

directory_name = '/Users/carolineroper/Documents/Big Data/all_validation_results_final/'

list_files = os.listdir(directory_name)

empty_files = []
all_files = {}

for file in list_files:
    try:
        all_files[file] = pd.read_table(directory_name + file, \
        sep = ",",\
        header = -1,\
        na_filter= False)
    except:
        empty_files.append(file)

total_results = pd.DataFrame()

for file in list_files:
     total_results = pd.concat([total_results, all_files[file]], axis=0)

total_results.columns = ['column_name', 'base_type', 'semantic_type', 'validity', 'number']

#Pivots data by type

types = pd.pivot_table(total_results, index = ['column_name', 'base_type', 'semantic_type'], aggfunc='sum')
types['number'] = types['number'].map('{:,}'.format)
types.reset_index(inplace=True)
#removes "green_" or "yellow_" from "column_name"
types['column_name'] = types['column_name'].map(lambda x: x.split('_')[1:]).map(lambda x: '_'.join(x))

print(types.to_latex(longtable=True))

#Pivots data by "valid/invalid/null"

validity = pd.pivot_table(total_results, index = 'column_name', columns = ['validity'], aggfunc='sum').fillna(0)
validity.reset_index(inplace=True)
validity.columns = validity.columns.droplevel()
validity.columns = ['Column Name', 'Invalid', 'Null', 'Valid']

#Calculates percent valid/invalid/null

validity['Percent Valid'] = validity['Valid']/(validity['Valid'] + validity['Invalid'] + validity['Null'])
validity['Percent Invalid'] = validity['Invalid']/(validity['Valid'] + validity['Invalid'] + validity['Null'])
validity['Percent Null'] = validity['Null']/(validity['Valid'] + validity['Invalid'] + validity['Null'])

#Formats number valid/invalid/null

validity['Valid'] = validity['Valid'].astype(int).apply(lambda x: '{:,}'.format(x))
validity['Invalid'] = validity['Invalid'].astype(int).apply(lambda x: '{:,}'.format(x))
validity['Null'] = validity['Null'].astype(int).apply(lambda x: '{:,}'.format(x))

#Formats percent valid/invalid/null

validity['Percent Valid'] = pd.Series([round(val, 4) for val in validity['Percent Valid']])
validity['Percent Valid'] = pd.Series(["{0:.2f}%".format(val * 100) for val in validity['Percent Valid']], index = validity.index)

validity['Percent Invalid'] = pd.Series([round(val, 4) for val in validity['Percent Invalid']])
validity['Percent Invalid'] = pd.Series(["{0:.2f}%".format(val * 100) for val in validity['Percent Invalid']], index = validity.index)

validity['Percent Null'] = pd.Series([round(val, 4) for val in validity['Percent Null']])
validity['Percent Null'] = pd.Series(["{0:.2f}%".format(val * 100) for val in validity['Percent Null']], index = validity.index)
#removes "green_" or "yellow_" from "column_name"
validity['Column Name'] = validity['Column Name'].map(lambda x: x.split('_')[1:]).map(lambda x: '_'.join(x))

validity = validity[['Column Name', 'Valid', 'Invalid', 'Null', 'Percent Valid', 'Percent Invalid', 'Percent Null']]

print(validity.to_latex(longtable=True))

