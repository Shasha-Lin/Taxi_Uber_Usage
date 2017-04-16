from __future__ import print_function
from csv import reader
import sys
from operator import add
from pyspark import SparkContext
from datetime import datetime
#import numpy as np
import sys
sc = SparkContext()
sc.addFile('common_functions.py')
from common_functions import *

"""This script outputs 3 things for each column: base type, semantic type, valid/invalid/null

This script should be applied to columns: VendorID, Passenger_count, Pickup_longitude, 
Pickup_latitude, Dropoff_longitude, Dropoff_latitude, 
Payment_type, Fare_amount, Extra, MTA_tax, 
Tip_amount, Tolls_amount, improvement_surcharge, Total_amount, Ehail_fee

"""

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: RequiredOutput <file> <column> <output directory>\n file should be any green/yellow cab tables between 2015 Jan and 2016 June \n\
            column should be the name of the columnof interest: e.g. Pickup_longitude \n\
            output directory is where you would like to see your output in the hdfs: e.g. user/sl4964", file=sys.stderr)
        exit(-1)

    current_module = sys.modules[__name__] #used to fetch the correct function
    column_list = ['vendorid', 'passenger_count', 'pickup_longitude', 
    'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'ratecodeid', 
    'fare_amount', 'extra', 'mta_tax', 'payment_type',  'store_and_fwd_flag', 
    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'ehail_fee', 'trip_type']

    
    watershed = datetime(2016, 7, 5, 0, 0)
    #In and after 2016-07, file schema changed. The day 5 is arbitrary, and should not matter. 
    yg = sys.argv[1].split('_')[0]
    month = sys.argv[1].split('_')[-1].split('.')[0].split('-')
    file_time = file = datetime(int(month[0]), int(month[1]), 5)
    #Use different dictionary for data files with different schemas.
    if yg == 'green':
        if file_time < watershed:
            column_dictionary = {'VendorID': 0, 'Pickup_longitude': 5, 'Pickup_latitude': 6, 'Dropoff_longitude': 7, 'Dropoff_latitude': 8, 
            'Passenger_count': 9, 'Fare_amount': 11, 'Extra', 12, 'MTA_tax', 13, 'Tip_amount': 14, 'Tolls_amount':15, 'Ehail_fee':16, 
            'improvement_surcharge':17, 'Total_amount': 18, 'Payment_type': 19, 'Trip_type':20, 'RateCodeID': 4, 'Store_and_fwd_flag': 3}
        else:
            column_dictionary = {'VendorID': 0, 'lpep_pickup_datetime': 1, 'lpep_dropoff_datetime': 2, 'Store_and_fwd_flag':3, 
            'RatecodeID':4, 'PULocationID':5, 'DOLocationID':6, 'passenger_count':7, 'trip_distance':8, 'fare_amount':9, 
            'extra':10, 'mta_tax': 11, 'tip_amount': 12, 'tolls_amount': 13, 'ehail_fee': 14, 'improvement_surcharge':15, 
            'total_amount': 16, 'payment_type':17, 'trip_type': 18}
    if yg == 'yellow':
        if file_time < watershed:
            column_dictionary = {'VendorID':0, 'tpep_pickup_datetime':1, 'tpep_dropoff_datetime':2, 'passenger_count':3, 
            'trip_distance':4, 'pickup_longitude':5,'pickup_latitude':6,'RateCodeID':7,'store_and_fwd_flag':8,'dropoff_longitude':9, 
            'dropoff_latitude':10,'payment_type':11,'fare_amount':12,'extra':13,'mta_tax':14,'tip_amount':15,'tolls_amount':16, 
             'improvement_surcharge':17, 'total_amount':18}
        else:
            column_dictionary = {'VendorID':0, 'tpep_pickup_datetime':1,'tpep_dropoff_datetime':2,'passenger_count':3, 
            'trip_distance':4,'RatecodeID':5,'store_and_fwd_flag':6,'PULocationID':7,'DOLocationID':8,'payment_type':9, 
            'fare_amount':10,'extra':11,'mta_tax':12,'tip_amount':13,'tolls_amount':14,'improvement_surcharge':15, 'total_amount':16}

    taxi_data = sc.textFile(sys.argv[1])
    column_name = sys.argv[2]
    column_number = column_dictionary[column_name]
    missing = ['0', 'NULL', 'NaN', '', 'NAN', 'nan', 'None']
    ratecodeid_missing = ['99']
    def pickup_longitude(output):
        """generates output for Pickup_longitude"""
        try:
        	output[0] = float(output[0])
        except ValueError:
        	output[2] = 'INVALID'
        if output[0] == 0:
            output[2] = 'NULL'
        elif output[0]  < -75 or output[0]  > -71:
        	output[2] = 'INVALID'
        else:
        	output[2] = 'VALID'
        return output

    def pickup_latitude(output):
        """generates output for Pickup_latitude"""
		try:
        	output[0] = float(output[0])
        except ValueError:
        	output[2] = 'INVALID'
        if output[0] == 0:
            output[2] = 'NULL'
        elif output[0] < 39.5 or output[0] > 43:
        	output[2] = 'INVALID'
        else:
        	output[2] = 'VALID'
        return output

    def dropoff_longitude(output):
        """generates output for Dropoff_longitude"""
        return Pickup_longitude(output)

    def dropoff_latitude(output):
        """generates output for Dropoff_latitude"""
        return Pickup_latitude(output)

    def vendorid(output):
    	"""generates output for VendorID"""
    	try:
    		output[0] = int(output[0])
    	except:
    		output[2] = 'NULL'
    	if output[0] in [1, 2]:
    		output[2] = 'VALID'
    	else:
			output[2] = 'INVALID'
		return output 

	def passenger_count(output, rang = range(1, 10)):
		'''generates output for Passenger_count'''
		try:
    		output[0] = int(output[0])
    	except:
    		output[2] = 'NULL'
    	if output[0] in rang:
    		output[2] = 'VALID'
    	else:
    		output[2] = 'INVALID'
    	return output

    def payment_type(output):
		'''generates output for Payment_type'''
		return Passenger_count(output, rang = range(1, 7))

    def fare_amount(output):
    	'''generates output for Fare_amount'''
    	try:
    		output[0] = float(output[0])
    	except:
    		if output[0] in missing:
    			output[2] = 'NULL'
    		else:
    			output[2] = 'INVALID'
    	if output[0] < 0:
    		output[2] = 'INVALID'
    	else:
    		output[2] = 'VALID'
    	return output

    def extra(output[0], values = [.5, 1.0]):
    	'''generates output for Extra'''
    	try:
    		output[0] = float(output[0])
    	except:
    		if output[0] in missing:
    			output[2] = 'NULL'
    		else:
    			output[2] = 'INVALID'
    	if output[0] not in values:
    		output[2] = 'INVALID'
    	else:
    		output[2] = 'VALID'
    	return output

    def mta_tax(output):
    	return Extra(output, values = [.5])

    def improvement_surcharge(output):
    	return Extra(output, values = [.3])

    def tip_amount(output):
    	return Fare_amount(output)

    def trip_type(output)
    	return Passenger_count(output, rang = range(1, 3))

    def ehail_fee(output):
    	return Fare_amount(output)

    def tolls_amount(output):
    	return Fare_amount(output)

    def total_amount(output):
    	return Fare_amount(output)

    def store_and_fwd_flag(output):
    	if output[0] in missing:
    		output[2] = 'NULL'
    	elif output[0] in ('Y', 'N'):
    		output[2] = 'VALID'
    	else:
    		output[2] = 'INVALID'
    	return output

    def ratecodeid(output):
		if (output[0] in missing) or (output[0] in ratecodeid_missing):
			output[2] = 'NULL'
		try:
			output[0] = float(output[0])
		except:
			output[2] = 'INVALID'
		if output[0] in range(1, 7):
			output[2] = 'VALID'
    	else:
    		output[2] = 'INVALID'


    output = taxi_data.mapPartitions(lambda x: reader(x)).map(lambda x: x[column_number]).\
    map(lambda x: [x, base_type(x), semantic_type(x), 0]).\
    map(lambda x: pass if x[0].lower() in column_list else getattr(current_module, sys.argv[2].lower())(x)).\
    map(lambda x: '\t'.join())
    

    output.saveAsTextFile("%s/%s_%s.out"%(sys.argv[3], sys.argv[1].split('/')[-1].split('.')[0], sys.argv[2]))
    sc.stop()
