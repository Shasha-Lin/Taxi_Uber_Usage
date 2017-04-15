from __future__ import print_function
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re

def validate_location_id(value):
    missing = ['0', 'NULL', 'NaN', '']
    if 'LocationID' in semantic_type(value):
        return 'VALID'
    elif value in missing:
        return 'MISSING'
    else:
        return 'INVALID\OUTLIER'

sc = SparkContext()
sc.addFile('common_functions.py')

from common_functions import *

lines = sc.textFile('new_schema/yellow_*.csv')
lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1).filter(lambda x: x[1] != 'tpep_pickup_datetime')

long_lines = lines.filter(lambda x: len(x) >= 8)
short_lines = lines.filter(lambda x: len(x) < 8)

results = long_lines.map(lambda x: x[7]).map(lambda x: (x, base_type(x), semantic_type(x), validate_location_id(x)))
results_missing = short_lines.map(lambda x: ('Unknown', 'None', 'MISSING'))

all_results = results.union(results_missing)

all_results.saveAsTextFile('v_PULocationID_yellow.out')

sc.stop()