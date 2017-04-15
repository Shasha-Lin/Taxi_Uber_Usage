import sys
from operator import add
from pyspark import SparkContext

'''This script takes a CSV created by a data validation script of form Value, Basetype, Semantictype, Valid/Invalid/Outlier
and outputs a summary of number of values by unique combination of Basetype, Semantictype, Valid/Invalid/Outlier'''

sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)
lines = lines.map(lambda x: x.split(','))
results = lines.map(lambda x: ((x[1], x[2], x[3]), 1)).reduceByKey(add).\
map(lambda x: (str(x[0][0]) + ',' + str(x[0][1]) + ',' + str(x[0][2]) + ',' + str(x[1])))

results.saveAsTextFile('results.out')

sc.stop()