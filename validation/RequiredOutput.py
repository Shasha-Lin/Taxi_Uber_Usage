from __future__ import print_function
from csv import reader
import sys
from operator import add
from pyspark import SparkContext
import numpy as np
import sys

"""This script outputs 3 things for each column: base type, semantic type, valid/invalid/null

This script should be applied to columns: VendorID, Passenger_count, Pickup_longitude, 
Pickup_latitude, Dropoff_longitude, Dropoff_latitude, 
Payment_type, Fare_amount, Extra, MTA_tax, 
Tip_amount, Tolls_amount, improvement_surcharge, Total_amount, Ehail_fee

"""

current_module = sys.modules[__name__]
column_list = ['VendorID', 'Passenger_count', 'Pickup_longitude', 
'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude', 
'Payment_type', 'Fare_amount', 'Extra', 'MTA_tax', 
'Tip_amount', 'Tolls_amount', 'improvement_surcharge', 'Total_amount', 'Ehail_fee']

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: RequiredOutput <file> <column> <output directory>\n file should be any green/yellow cab tables between 2015 Jan and 2016 June \n\
            column should be the name of the columnof interest: e.g. Pickup_longitude \n\
            output directory is where you would like to see your output in the hdfs: e.g. user/sl4964", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    taxi_data = sc.textFile(sys.argv[1])
    column_dictionary = {'Pickup_longitude': 5, 'Pickup_latitude': 6, 'Dropoff_longitude': 7, 'Dropoff_latitude': 8}
    #gets input csv into SDD object
    column_name = sys.argv[2]
    column_number = column_dictionary[column_name]
    
    def Pickup_longitude(x):
        """generates output for Pickup_longitude"""
        output = ['NUMBER', 'Pickup_longitude', 0]
        x = x[column_number]

        try:
        	x = float(x)
        except ValueError:
        	output[2] = 'INVALID'
        if x == 0:
            output[2] = 'NULL'
        elif x < -75 or x > -71:
        	output[2] = 'INVALID'
        else:
        	output[2] = 'VALID'
        return output

    def Pickup_latitude(x):
        """generates output for Pickup_latitude"""
        output = ['NUMBER', 'Pickup_latitude', 0]
        x = x[column_number]
        try:
        	x = float(x)
        except ValueError:
        	output[2] = 'INVALID'
        if x == 0:
            output[2] = 'NULL'
        elif x < 39.5 or x > 43:
        	output[2] = 'INVALID'
        else:
        	output[2] = 'VALID'
        return output

    def Dropoff_longitude(x):
        """generates output for Dropoff_longitude"""
        return Pickup_longitude(x)

    def Dropoff_latitude(x):
        """generates output for Dropoff_latitude"""
        return Pickup_latitude(x)

    def VendorID(x):
    	"""generates output for VendorID"""
    	output = ['NUMBER', 'VendorID', 0]
    	try:
    		x = int(x)
    	except:
    		output[2] = 'NULL'
    	if x in [1, 2]:
    		output[2] = 'VALID'
    	else:
			output[2] = 'INVALID' 

	
    output = taxi_data.mapPartitions(lambda x: reader(x)).map(lambda x: pass if x in column_list else getattr(current_module, sys.argv[2])(x))
    

    output.saveAsTextFile("%s/%s_%s.out"%(sys.argv[3], sys.argv[1].split('/')[-1].split('.')[0], sys.argv[2]))
    sc.stop()
