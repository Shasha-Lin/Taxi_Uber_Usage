import sys
from operator import add
from pyspark import SparkContext

'''This script takes a CSV created by a data validation script of form Value, Basetype, Semantictype, Valid/Invalid/Outlier
and outputs a summary of number of values by unique combination of Basetype, Semantictype, Valid/Invalid/Outlier'''

sc = SparkContext()

name = sys.argv[1].split('/')[-1].split('.')[0]

lines = sc.textFile(sys.argv[1], 1)
lines = lines.map(lambda x: x.split('\t'))
results = lines.map(lambda x: ((x[1], x[2], x[3]), 1)).reduceByKey(add).\
map(lambda x: (name + ',' + str(x[0][0]) + ',' + str(x[0][1]) + ',' + str(x[0][2]) + ',' + str(x[1])))
#added name to be able to concatenate all the files and still have the total results

results.saveAsTextFile("aggregation/total_%s.out"%(name))

sc.stop()
