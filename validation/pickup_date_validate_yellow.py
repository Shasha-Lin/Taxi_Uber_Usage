from __future__ import print_function
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re

def validate_date(date):
    missing = ['0', 'NULL', 'NaN', '']
    try:
        date_parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if date_parsed.year in (2015, 2016):
            return 'VALID'
        else:
            return 'INVALID\OUTLIER'
    except ValueError:
        if date in missing:
            return 'MISSING'
        else:
            return 'INVALID\OUTLIER'

sc = SparkContext()
sc.addFile('common_functions.py')

from common_functions import *

lines = sc.textFile('new_schema/yellow_*.csv,old_schema/yellow_*.csv')
lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1).filter(lambda x: x[1] != 'tpep_pickup_datetime')
pickup_dates = lines.map(lambda x: x[1]).map(lambda x: (x, base_type(x), semantic_type(x), validate_date(x)))

pickup_dates.saveAsTextFile('v_pickup_dates_yellow.out')

sc.stop()