from csv import reader
from pyspark import SparkContext
from operator import add
import sys
from datetime import datetime

'''Run using command spark-submit by_location.py'''

if __name__ == "__main__":
    sc = SparkContext()
    lines_g = sc.textFile('new_schema/green_*.csv')
    lines_g = lines_g.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1)

    counts_g = lines_g.filter(lambda x: x[1] != 'lpep_pickup_datetime').map(lambda x: ((x[5],x[6]),1)).reduceByKey(add).map(lambda x: str(x[0][0]) + '\t' + str(x[0][1]) + '\t' + str(x[1]))

    lines_y = sc.textFile('new_schema/yellow_*.csv')
    lines_y = lines_y.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1)

    counts_y = lines_y.filter(lambda x: x[1] != 'tpep_pickup_datetime').map(lambda x: ((x[7],x[8]),1)).reduceByKey(add).map(lambda x: str(x[0][0]) + '\t' + str(x[0][1]) + '\t' + str(x[1]))

    counts = counts_g.union(counts_y)

    counts.saveAsTextFile('by_borough.out')

    sc.stop()