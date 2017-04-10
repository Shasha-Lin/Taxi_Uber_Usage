from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)) 

    counts = lines.map(lambda x: (x[2][:-9],1)).reduceByKey(add).sortByKey(ascending=True).map(lambda x: str(x[0]) + '\t' + str(x[1]))

    counts.saveAsTextFile('by_date.out')

    sc.stop()