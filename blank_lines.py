from __future__ import print_function
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re

sc = SparkContext()

lines = sc.textFile('new_schema/yellow_*.csv,old_schema/yellow_*.csv,new_schema/green_*.csv,new_schema/green_*.csv')
#problem with filtering out lenx >= 1 is what if some of these are missing? Maybe write a separate query where we find the missing lines
lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) == 0)
#there are 12 blank lines - do we need to know which file they're from and do we count them as "missing" values?
#I vote no on missing values
lines.saveAsTextFile('filtered_out_lines.out')

sc.stop()