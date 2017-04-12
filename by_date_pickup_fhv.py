from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile('fhv*.csv')
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1)

    counts = lines.filter(lambda x: x[1] != 'Pickup_date').map(lambda x: (x[1][:-9],1)).reduceByKey(add).sortByKey(ascending=True).map(lambda x: str(x[0]) + '\t' + str(x[1]))

    counts.saveAsTextFile('by_date_pickup.out')

    sc.stop()