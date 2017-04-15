from csv import reader
from pyspark import SparkContext
import sys
import re
from itertools import islice
from calendar import weekday


sc = SparkContext()

lines = sc.textFile('/user/cer446/new_schema/y-*.csv')

header = lines.first()
lines = lines.filter(lambda line: line != header)

weekend = [5,6]

tip = 13
fare = 10
total_amount = 16
pu = 7
do = 8
passengers = 3

#filter entries with no  loc_id
#first rdd format: ( pick up loc_id ' ' DO loc_id, (total amount paid, # passengers, 1  )   )
# reduce by PU, DO key
#output rdd format:
# [ PU loc_id, DO loc_id, total amount paid, # passengers,  # entries in this PU-DO combo     ]
# call flatten on this and put into pandas

lines = lines.mapPartitions(lambda l: reader(l)) \
        .map(lambda l:
( l[pu]  +' '+ l[do], (l[total_amount], l[passengers], 1, 0 )
if weekday(2016, int(l[2].split('-')[1]), int(l[2].split('-')[2][:2]) ) not in weekend
else (l[total_amount], l[passengers], 0, 1)     ))    \
        .reduceByKey(lambda v1, v2:
(float(v1[0])+float(v2[0]), float(v1[1])+float(v2[1]),
float(v1[2])+float(v2[2]), float(v1[3])+float(v2[3])      )) \
        .map(lambda l: [
int(l[0].split()[0]), int(l[0].split()[1]),
float(l[1][0]), int(l[1][1]), int(l[1][2]), int(l[1][3])  ] )\

lines.saveAsTextFile("yellow_2016")
sc.stop()
