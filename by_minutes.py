from csv import reader
from pyspark import SparkContext
from calendar import weekday

sc = SparkContext()

lines = sc.textFile('/user/cer446/new_schema/y*.csv,/user/cer446/old_schema/y*.csv')

header = lines.first()
lines = lines.filter(lambda line: line != header)

weekend = [5,6]

lines = lines.mapPartitions(lambda l: reader(l)) \
        .filter(lambda l: l[1] != 'tpep_pickup_datetime')\
        .map(lambda l: (str(l[1][-8:-3]), (1,0)
if weekday(
int(l[2].split('-')[0]),
int(l[2].split('-')[1]),
int(l[2].split('-')[2][:2])  )
not in weekend else (0,1) ))\
        .reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]) ) \
        .sortBy(lambda l: l[0] )  \
        .map(lambda l: [str(l[0]), int(l[1][0]), int(l[1][1])  ])\

lines.saveAsTextFile("avg_tra_day.out")
sc.stop()
