from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

'''Run using command spark-submit pickup_long_lat_citi.py'''

def decoding(row):
    for item in row:
        item = item.decode('utf-8').lower()
    return row

def remove_apostrophe(row):
    for item in row:
        item = item.replace('"', '')
    return row

def validate_pickup_latitude(input_lat):
    """function modified from validation_combined.py"""
    try:
        input_lat = float(input_lat)
        if input_lat == 0:
            output = 'NULL'
        elif input_lat < 39.5 or input_lat > 43:
            output = 'INVALID'
        else:
            output = 'VALID'
    except ValueError:
        output = 'INVALID'
    return output

def validate_pickup_longitude(input_long):
    """function modified from validation_combined.py"""
    try:
        input_long = float(input_long)
        if input_long == 0:
            output = 'NULL'
        elif input_long < -75 or input_long > -71:
            output = 'INVALID'
        else:
            output = 'VALID'
    except ValueError:
        output = 'INVALID'
    return output

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile('citibike/2015*.csv,citibike/201601*.csv,citibike/201602*.csv,citibike/201603*.csv,citibike/201604*.csv,citibike/201605*.csv,citibike/201606*.csv')
    lines = lines.map(lambda l: l.encode("ascii","ignore").split(',')).\
    map(lambda l: (l[6].replace('"', ''), l[5].replace('"', '')))\
    .filter(lambda x: x[0] != 'Start Station Longitude')\
    .filter(lambda x: x[0] != 'start station longitude')\
    .filter(lambda x: validate_pickup_longitude(x[0]) == 'VALID')\
    .filter(lambda x: validate_pickup_latitude(x[1]) == 'VALID')

    print = print

    counts = lines.map(lambda x: ((round(float(x[0]),4), round(float(x[1]),4)),1))

    globals()['print'](counts.take(5))

    counts = counts.reduceByKey(add).sortByKey(ascending=False).map(lambda x: str(x[0][0]) + '\t' + str(x[0][1]) + '\t' + str(x[1]))

    counts.saveAsTextFile('by_pickup_long_lat_citibike.out')

    sc.stop()

