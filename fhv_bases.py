from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from functools import partial

"""returns the base number for uber/lyft/via bases
run by spark-submit fhv_bases.py /user/cer446/current_black_car_bases.csv uber/lyft/via > filename.txt"""

def decoding(row):
	for item in row:
		item = item.decode('utf-8').lower()
	return row

def get_bases(row, company):
	if company in row[2].lower():
		return row[0]
	elif 'llc' in row[2].lower():
		if company in row[3].lower():
			return row[0]

if __name__ == "__main__":
	get_company_bases = partial(get_bases, company = sys.argv[2])
	sc = SparkContext()
	fhv_bases =  sc.textFile(sys.argv[1])
	bases = fhv_bases.map(lambda x: x.strip().split(',')).map(decoding).map(get_company_bases).collect()
	for base in bases:
		if base is not None:
			print("%s" % (base))

	sc.stop()