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
    column_list = ['pulocationid','dolocationid','lpep_pickup_datetime', 'tpep_pickup_datetime','lpep_dropoff_datetime','tpep_dropoff_datetime','trip_distance',
    'vendorid', 'passenger_count', 'pickup_longitude', 
    'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'ratecodeid', 
    'fare_amount', 'extra', 'mta_tax', 'payment_type',  'store_and_fwd_flag', 
    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'ehail_fee', 'trip_type']

    old_green= {'vendorid': 0, 'lpep_pickup_datetime:':1, 'lpep_dropoff_datetime':2, 'pickup_longitude': 5, 'pickup_latitude': 6, 'dropoff_longitude': 7, 'dropoff_latitude': 8, 
            'passenger_count': 9, 'trip_distance': 10, 'fare_amount': 11, 'extra': 12, 'mta_tax': 13, 'tip_amount': 14, 'tolls_amount':15, 'ehail_fee':16, 
            'improvement_surcharge':17, 'total_amount': 18, 'payment_type': 19, 'trip_type':20, 'ratecodeid': 4, 'store_and_fwd_flag': 3}

    new_green = {'vendorid': 0, 'lpep_pickup_datetime': 1, 'lpep_dropoff_datetime': 2, 'store_and_fwd_flag':3, 
            'ratecodeid':4, 'pulocationid':5, 'dolocationid':6, 'passenger_count':7, 'trip_distance':8, 'fare_amount':9, 
            'extra':10, 'mta_tax': 11, 'tip_amount': 12, 'tolls_amount': 13, 'ehail_fee': 14, 'improvement_surcharge':15, 
            'total_amount': 16, 'payment_type':17, 'trip_type': 18}

    old_yellow = {'vendorid':0, 'tpep_pickup_datetime':1, 'tpep_dropoff_datetime':2, 'passenger_count':3, 
            'trip_distance':4, 'pickup_longitude':5,'pickup_latitude':6,'ratecodeid':7,'store_and_fwd_flag':8,'dropoff_longitude':9, 
            'dropoff_latitude':10,'payment_type':11,'fare_amount':12,'extra':13,'mta_tax':14,'tip_amount':15,'tolls_amount':16, 
             'improvement_surcharge':17, 'total_amount':18}

    new_yellow = {'vendorid':0, 'tpep_pickup_datetime':1,'tpep_dropoff_datetime':2,'passenger_count':3, 
            'trip_distance':4,'ratecodeid':5,'store_and_fwd_flag':6,'pulocationid':7,'dolocationid':8,'payment_type':9, 
            'fare_amount':10,'extra':11,'mta_tax':12,'tip_amount':13,'tolls_amount':14,'improvement_surcharge':15, 'total_amount':16}

    missing = ['0', 'NULL', 'NaN', '', 'NAN', 'nan', 'None', 'none', 'Unknown', 'unknown']
    ratecodeid_missing = ['99']

    def pulocationid(value):
        try:
            LocationID = int(value[0])
            if LocationID in (264, 265):
                value[-1] = 'NULL'
            elif LocationID in range(1,264):
                value[-1] = 'VALID'
            else:
                value[-1] = 'INVALID'
        except ValueError:
            if value[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value


    def dolocationid(value):
        try:
            LocationID = int(value[0])
            if LocationID in (264, 265):
                value[-1] = 'NULL'
            elif LocationID in range(1,264):
                value[-1] = 'VALID'
            else:
                value[-1] = 'INVALID'
        except ValueError:
            if value[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def lpep_pickup_datetime(value):
        try:
            value_parsed = datetime.strptime(value[0], '%Y-%m-%d %H:%M:%S')
            if value_parsed.year in (2015, 2016):
                value[-1] = 'VALID'
            else:
                value[-1] = 'INVALID'
        except ValueError:
            if value[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def tpep_pickup_datetime(value):
        try:
            value_parsed = datetime.strptime(value[0], '%Y-%m-%d %H:%M:%S')
            if value_parsed.year in (2015, 2016):
                value[-1] = 'VALID'
            else:
                value[-1] = 'INVALID'
        except ValueError:
            if value[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def lpep_dropoff_datetime(value):
        try:
            value_parsed = datetime.strptime(value[0], '%Y-%m-%d %H:%M:%S')
            if value_parsed.year in (2015, 2016):
                value[-1] = 'VALID'
            else:
                if value_parsed.month == 1 and value_parsed.day == 1:
                    value[-1] = 'VALID'
                else:
                    value[-1] = 'INVALID'
        except ValueError:
            if date[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def tpep_dropoff_datetime(value):
        try:
            value_parsed = datetime.strptime(value[0], '%Y-%m-%d %H:%M:%S')
            if value_parsed.year in (2015, 2016):
                value[-1] = 'VALID'
            else:
                if value_parsed.month == 1 and value_parsed.day == 1:
                    value[-1] = 'VALID'
                else:
                    value[-1] = 'INVALID'
        except ValueError:
            if date[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def trip_distance(value):
        try:
            distance = float(value[0])
            if distance < 75 and distance > 0:
                value[-1] = 'VALID'
            elif distance == 0:
                value[-1] = 'NULL'
            elif distance > 75 or distance < 0:
                value[-1] = 'INVALID'
            else:
                value[-1] = 'INVALID'
        except ValueError:
            if value[0] in missing:
                value[-1] = 'NULL'
            else:
                value[-1] = 'INVALID'
        return value

    def pickup_longitude(output):
        """generates output for Pickup_longitude"""
        try:
            output[0] = float(output[0])
        except ValueError:
            output[-1] = 'INVALID'
        if output[0] == 0:
            output[-1] = 'NULL'
        elif output[0]  < -75 or output[0]  > -71:
            output[-1] = 'INVALID'
        else:
            output[-1] = 'VALID'
        return output

    def pickup_latitude(output):
        """generates output for Pickup_latitude"""
        try:
            output[0] = float(output[0])
        except ValueError:
            output[-1] = 'INVALID'
        if output[0] == 0:
            output[-1] = 'NULL'
        elif output[0] < 39.5 or output[0] > 43:
            output[-1] = 'INVALID'
        else:
            output[-1] = 'VALID'
        return output

    def dropoff_longitude(output):
        """generates output for Dropoff_longitude"""
        return pickup_longitude(output)

    def dropoff_latitude(output):
        """generates output for Dropoff_latitude"""
        return pickup_latitude(output)

    def vendorid(output):
        """generates output for VendorID"""
        try:
            output[0] = int(output[0])
        except:
            output[-1] = 'NULL'
        if output[0] in [1, 2]:
            output[-1] = 'VALID'
        else:
            output[-1] = 'INVALID'
        return output 

    def passenger_count(output, rang = range(1, 10)):
        '''generates output for Passenger_count'''
        try:
            output[0] = int(output[0])
        except:
            output[-1] = 'NULL'
        if output[0] in rang:
            output[-1] = 'VALID'
        else:
            output[-1] = 'INVALID'
        return output

    def payment_type(output):
        '''generates output for Payment_type'''
        return passenger_count(output, rang = range(1, 7))

    def fare_amount(output):
        '''generates output for Fare_amount'''
        try:
            output[0] = float(output[0])
            if output[0] < 0:
                output[-1] = 'INVALID'
            else:
                output[-1] = 'VALID'
        except:
            if output[0] in missing:
                output[-1] = 'NULL'
            else:
                output[-1] = 'INVALID'
        return output

    def extra(output, values = [.5, 1.0]):
        '''generates output for Extra'''
        try:
            output[0] = float(output[0])
        except:
            if output[0] in missing:
                output[-1] = 'NULL'
            else:
                output[-1] = 'INVALID'
        if output[0] not in values:
            output[-1] = 'INVALID'
        else:
            output[-1] = 'VALID'
        return output

    def mta_tax(output):
        return extra(output, values = [.5])

    def improvement_surcharge(output):
        return extra(output, values = [.3])

    def tip_amount(output):
        return fare_amount(output)

    def trip_type(output):
        return passenger_count(output, rang = range(1, 3))

    def ehail_fee(output):
        return fare_amount(output)

    def tolls_amount(output):
        return fare_amount(output)

    def total_amount(output):
        return fare_amount(output)

    def store_and_fwd_flag(output):
        if output[0] in missing:
            output[-1] = 'NULL'
        elif output[0] in ('Y', 'N'):
            output[-1] = 'VALID'
        else:
            output[-1] = 'INVALID'
        return output

    def ratecodeid(output):
        if (output[0] in missing) or (output[0] in ratecodeid_missing):
            output[-1] = 'NULL'
        try:
            output[0] = float(output[0])
        except:
            output[-1] = 'INVALID'
        if output[0] in range(1, 7):
            output[-1] = 'VALID'
        else:
            output[-1] = 'INVALID'
        return output

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

    print = print

    globals()['print']('yellow or green ' + str(sys.argv[1]))
    globals()['print']('column name ' + str(sys.argv[2]))
    globals()['print']('old_column_number ' + str(old_column_number))
    globals()['print']('new_column_number ' + str(new_column_number))

    if old_column_number >= 0:
        if sys.argv[1] == 'yellow':
            taxi_data_old = sc.textFile('/user/cer446/old_schema/yellow_*.csv')
        if sys.argv[1] == 'green':
            taxi_data_old = sc.textFile('/user/cer446/old_schema/green_*.csv')

        output_old = taxi_data_old.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= (old_column_number + 1)).map(lambda x: x[old_column_number]).\
        map(lambda x: [x, base_type(str(x)), semantic_type(str(x)), 0])
        output_old = output_old.filter(lambda x: x[0].lower() not in column_list).\
        map(getattr(current_module, sys.argv[2].lower())).\
        map(lambda x: str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2]) + '\t' + str(x[3]))

        globals()['print'](output_old.take(5))

    if new_column_number >= 0:
        if sys.argv[1] == 'yellow':
            taxi_data_new = sc.textFile('/user/cer446/new_schema/yellow_*.csv')
        if sys.argv[1] == 'green':
            taxi_data_new = sc.textFile('/user/cer446/new_schema/green_*.csv')

        output_new = taxi_data_new.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= (new_column_number + 1)).map(lambda x: x[new_column_number]).\
        map(lambda x: [x, base_type(str(x)), semantic_type(str(x)), 0])
        output_new = output_new.filter(lambda x: x[0].lower() not in column_list).\
        map(getattr(current_module, sys.argv[2].lower())).\
        map(lambda x: str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2]) + '\t' + str(x[3]))

        globals()['print'](output_new.take(5))

    if old_column_number >= 0 and new_column_number >=0:
        results = output_old.union(output_new) #if both results exist, union them
    if old_column_number == -999 and new_column_number >= 0:
        results = output_new #if only the new result exists, use that
    if old_column_number >= 0 and new_column_number == -999:
        results = output_old #if only the old results exist, use that

    results.saveAsTextFile("%s/%s_%s.out"%(sys.argv[3], sys.argv[1], sys.argv[2]))

    sc.stop()



