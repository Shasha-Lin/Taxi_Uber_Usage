from csv import reader
from pyspark import SparkContext
import sys
from datetime import datetime

sc = SparkContext()

#for green, distince is column 10 before schema change, column 8 after
#for yellow, distance is column 4 before and after schema change

#the final results ought to be short

lines_y = sc.textFile('old_schema/yellow_*.csv,new_schema/yellow_*.csv')

lines_y = lines_y.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1).filter(lambda x: x[1] != 'tpep_pickup_datetime')

counts_y = lines_y.map(lambda x: (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S').weekday(),
                   datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S').hour,\
                   datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S') - datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S'),\
                   float(x[4]))\
                  ).filter(lambda x: x[2].seconds > 0) #difference in between times must be positive

speeds_y = counts_y.map(lambda x: ((x[1], 1 if x[0] in (5,6) else 0), x[3]/(x[2].seconds/60.0/60.0))) #hour, weekendflg, speed, divide by zero error

sumCount_y = speeds_y.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))

averageByKey_y = sumCount_y.map(lambda (label, (value_sum, count)): (label, value_sum / count)).map(lambda x: str(x[0][0]) + '\t' +  str(x[0][1]) + '\t' + str(x[1]))

averageByKey_y.saveAsTextFile("avg_speed_hour.out") #currently not outputing the unioned results anyway

sc.stop()

#the keys look right, but the values don't at all. I suspect it's because there are some data quality issues with either distance or with the times