from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

'''Run using command spark-submit by_date_pickup_citibike_simple.py'''

def recreate_date(x):
	if '/' in x:
		x = x.split('/')
		month = x[0]
		day = x[1]
		year = x[2]
		new_date = year + '-' + month.zfill(2)  + '-' + day.zfill(2) 
		return new_date
	else:
		return x

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile('/user/cer446/citibike/*')
	lines = lines.map(lambda x: x.split(','))\
	.map(lambda x: x[1])\
	.map(lambda x: x.replace('"', ''))\
	.map(lambda x: x.split(' ')[0])\
	.filter(lambda x: x != 'Start')\
	.filter(lambda x: x != 'starttime')\
	.map(lambda x: (recreate_date(x), 1))\
	.reduceByKey(add)\
	.sortByKey(ascending=True)\
	.map(lambda x: str(x[0]) + '\t' + str(x[1])).saveAsTextFile('by_date_citibike.out')