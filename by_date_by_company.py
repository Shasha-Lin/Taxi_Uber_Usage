from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
'''Run using command spark-submit by_date_by_company.py <input path for the bases> <input path or fhv files>'''

def get_datetime(x):
	fhv_datetime = x[1].split('/')
	return datetime(2000 + int(fhv_datetime[2]), int(fhv_datetime[0]), int(fhv_datetime[1]))

if __name__ == "__main__":
    sc = SparkContext()
    bases = sc.textFile(sys.argv[1]).mapPartitions(lambda x: reader(x)).map(lambda x: str(x[0])).collect()
    fhv = sc.textFile(sys.argv[2]).mapPartitions(lambda x: reader(x))
    fhv = fhv.filter(lambda x: x[0][0] != 'D').filter(lambda x: str(x[0]) in bases).map(lambda x: (str(x[1].split(' ')[0]), 
    	int(x[-1]))).reduceByKey(add).sortByKey(ascending = True)
    fhv.saveAsTextFile('by_date_by_company.out')

    sc.stop()