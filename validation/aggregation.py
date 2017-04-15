import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
results = lines.map(lambda x: ((x[0], x[1], x[2]), 1)).reduce(add)

results.saveAsTextFile('results.out')

sc.stop()