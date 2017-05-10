from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

'''Run using command spark-submit pickup_by_long_lat.py'''

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
    lines = sc.textFile('old_schema/green_*.csv,old_schema/yellow_*.csv')
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1)
    filtered_lines = lines.filter(lambda x: x[1] != 'tpep_pickup_datetime')\
    .filter(lambda x: x[1] != 'lpep_pickup_datetime')\
    .filter(lambda x: validate_pickup_longitude(x[5]) == 'VALID')\
    .filter(lambda x: validate_pickup_latitude(x[6]) == 'VALID')

    print = print

    globals()['print'](filtered_lines.take(5))

    counts = filtered_lines.map(lambda x: ((round(float(x[5]),4), round(float(x[6]),4)),1))\
    .reduceByKey(add).sortByKey(ascending=False).map(lambda x: str(x[0][0]) + '\t' + str(x[0][1]) + '\t' + str(x[1]))

    counts.saveAsTextFile('by_pickup_long_lat.out')

    sc.stop()





