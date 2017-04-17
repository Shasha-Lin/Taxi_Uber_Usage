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
"""

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: RequiredOutput <yellow or green> <column> <output directory>\n\
            yellow or green should be the collection of files you want to see results from \n\
            column should be the name of the columnof interest: e.g. Pickup_longitude \n\
            output directory is where you would like to see your output in the hdfs: e.g. user/sl4964", file=sys.stderr)
        exit(-1)

    current_module = sys.modules[__name__] #used to fetch the correct function
    column_list = ['pulocationid','dolocationid','lpep_pickup_datetime', 'tpep_pickup_datetime','lpep_dropoff_datetime','tpep_dropoff_datetime','trip_distance']

    old_green= {'VendorID': 0, 'lpep_pickup_datetime:':1, 'Lpep_dropoff_datetime':2, 'Pickup_longitude': 5, 'Pickup_latitude': 6, 'Dropoff_longitude': 7, 'Dropoff_latitude': 8, 
            'Passenger_count': 9, 'Trip_distance': 10, 'Fare_amount': 11, 'Extra': 12, 'MTA_tax': 13, 'Tip_amount': 14, 'Tolls_amount':15, 'Ehail_fee':16, 
            'improvement_surcharge':17, 'Total_amount': 18, 'Payment_type': 19, 'Trip_type':20, 'RateCodeID': 4, 'Store_and_fwd_flag': 3}

    new_green = {'VendorID': 0, 'lpep_pickup_datetime': 1, 'lpep_dropoff_datetime': 2, 'Store_and_fwd_flag':3, 
            'RatecodeID':4, 'PULocationID':5, 'DOLocationID':6, 'passenger_count':7, 'trip_distance':8, 'fare_amount':9, 
            'extra':10, 'mta_tax': 11, 'tip_amount': 12, 'tolls_amount': 13, 'ehail_fee': 14, 'improvement_surcharge':15, 
            'total_amount': 16, 'payment_type':17, 'trip_type': 18}

    old_yellow = {'VendorID':0, 'tpep_pickup_datetime':1, 'tpep_dropoff_datetime':2, 'passenger_count':3, 
            'trip_distance':4, 'pickup_longitude':5,'pickup_latitude':6,'RateCodeID':7,'store_and_fwd_flag':8,'dropoff_longitude':9, 
            'dropoff_latitude':10,'payment_type':11,'fare_amount':12,'extra':13,'mta_tax':14,'tip_amount':15,'tolls_amount':16, 
             'improvement_surcharge':17, 'total_amount':18}

    new_yellow = {'VendorID':0, 'tpep_pickup_datetime':1,'tpep_dropoff_datetime':2,'passenger_count':3, 
            'trip_distance':4,'RatecodeID':5,'store_and_fwd_flag':6,'PULocationID':7,'DOLocationID':8,'payment_type':9, 
            'fare_amount':10,'extra':11,'mta_tax':12,'tip_amount':13,'tolls_amount':14,'improvement_surcharge':15, 'total_amount':16}


    def pulocationid(value):
        if 'LocationID' in semantic_type(value):
            if int(value) in (264, 265):
                return 'MISSING'
            else:
                return 'VALID'
        elif value in missing:
            return 'MISSING'
        else:
            return 'INVALID\OUTLIER'

    def dolocationid(value):
        if 'LocationID' in semantic_type(value):
            if int(value) in (264, 265):
                return 'MISSING'
            else:
                return 'VALID'
        elif value in missing:
            return 'MISSING'
        else:
            return 'INVALID\OUTLIER'

    def lpep_pickup_datetime(date):
        try:
            date_parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if date_parsed.year in (2015, 2016):
                return 'VALID'
            else:
                return 'INVALID\OUTLIER'
        except ValueError:
            if date in missing:
                return 'MISSING'
            else:
                return 'INVALID\OUTLIER'

    def tpep_pickup_datetime(date):
        try:
            date_parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if date_parsed.year in (2015, 2016):
                return 'VALID'
            else:
                return 'INVALID\OUTLIER'
        except ValueError:
            if date in missing:
                return 'MISSING'
            else:
                return 'INVALID\OUTLIER'

    def lpep_dropoff_datetime(date):
        try:
            date_parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if date_parsed.year in (2015, 2016):
                return 'VALID'
            else:
                return 'INVALID\OUTLIER'
        except ValueError:
            if date in missing:
                return 'MISSING'
            else:
                return 'INVALID\OUTLIER'

    def tpep_dropoff_datetime(date):
        try:
            date_parsed = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            if date_parsed.year in (2015, 2016):
                return 'VALID'
            else:
                return 'INVALID\OUTLIER'
        except ValueError:
            if date in missing:
                return 'MISSING'
            else:
                return 'INVALID\OUTLIER'

    def trip_distance(value):
        if 'Distance' in semantic_type(value):
            if float(value) > 0:
                return 'VALID'
            else:
                return 'MISSING'
        else:
            if value in missing:
                return 'MISSING'
            else:
                return 'INVALID\OUTLIER'



    missing = ['0', 'NULL', 'NaN', '', 'NAN', 'nan', 'None', 'none', 'Unknown', 'unknown']

    column_name = sys.argv[2]

    old_column_number = -999
    new_column_number = -999

    if sys.argv[1] == 'yellow':
        try:
            old_column_number = old_yellow[column_name]
        except KeyError:
            pass
        try:
            new_column_number = new_yellow[column_name]
        except KeyError:
            pass
    if sys.argv[1] == 'green':
        try:
            old_column_number = old_green[column_name]
        except KeyError:
            pass
        try:
            new_column_number = new_green[column_name]
        except KeyError:
            pass

    if old_column_number >= 0:
        if sys.argv[1] == 'yellow':
            taxi_data_old = sc.textFile('/user/cer446/old_schema/yellow_*.csv')
        if sys.argv[1] == 'green':
            taxi_data_old = sc.textFile('/user/cerr6/old_schema/green_*.csv')

        output_old = taxi_data_old.mapPartitions(lambda x: reader(x)).map(lambda x: x[old_column_number]).\
        map(lambda x: [x, base_type(x), semantic_type(x), 0]).filter(lambda x: x[0].lower() not in column_list).\
        map(getattr(current_module, sys.argv[2].lower())).\
        map(lambda x: str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]) + ',' + str(x[3]))

    if new_column_number >= 0:
        if sys.argv[1] == 'yellow':
            taxi_data_new = sc.textFile('/user/cer446/new_schema/yellow_*.csv')
        if sys.argv[1] == 'green':
            taxi_data_new = sc.textFile('/user/cer446/new_schema/green_*.csv')

        output_new = taxi_data_new.mapPartitions(lambda x: reader(x)).map(lambda x: x[new_column_number]).\
        map(lambda x: [x, base_type(x), semantic_type(x), 0]).filter(lambda x: x[0].lower() not in column_list).\
        map(getattr(current_module, sys.argv[2].lower())).\
        map(lambda x: str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]) + ',' + str(x[3]))

    if old_column_number >= 0 and new_column_number >=0:
        results = output_old.union(output_new) #if both results exist, union them
    if old_column_number == -999 and new_column_number >= 0:
        results = output_new #if only the new result exists, use that
    if old_column_number >= 0 and new_column_number == -999:
        results = output_old #if only the old results exist, use that

    results.saveAsTextFile("%s/%s_%s.out"%(sys.argv[3], sys.argv[1], sys.argv[2]))

    sc.stop()



