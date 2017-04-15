from __future__ import print_function
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re

def base_type(any_string):
    #Check to see if it's a datetime
    date = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    date_match = date.match(any_string)
    if date_match:
        return 'Datetime'
    else:
    
        #Check to see if it's a decimal
        decimal = re.compile('^-?\d*\.[0-9]{1,6}')
        decimal_match = decimal.match(any_string)
        if decimal_match:
            return 'Decimal'
        else:
    
            #Check to see if it's an integer
            integer = re.compile('^-?[0-9]+$')
            integer_match = integer.match(any_string)
            if integer_match and '.' not in any_string:
                return 'Integer'
            else:
    
                #Check to see if it's a character
                character = re.compile('^[a-zA-Z]+$')
                character_match = character.match(any_string)
                if character_match:
                    return 'Character'
                else:
                    return 'Unknown'

def semantic_type(any_string):
    types = \
    dict.fromkeys(['PO/DO Datetime', 'Lat/Long', 'Currency', 'Distance', 'LowID', 'LocationID', 'Count', 'Flag'])
    
    #Check to see if it's a datetime
    date = re.compile('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
    date_match = date.match(any_string)
    if date_match:
        types['PO/DO Datetime'] = 1
    
    #Check to see if it's a latitude or longitude (6 digits, positive or negative)
    decimal6 = re.compile('^-?\d*\.[0-9]{6,}')
    decimal6_match = decimal6.match(any_string)
    if decimal6_match:
        types['Lat/Long'] = 1
    
    #Check to see if it's a 2-pt. positive decimal
    decimal2 = re.compile('[0-9]+\.[0-9]{1,2}')
    decimal2_match = decimal2.match(any_string)
    if decimal2_match:
        types['Currency'] = 1
        types['Distance'] = 1
        
    #Check to see if it's an integer
    if base_type(any_string) == 'Integer':
        types['Count'] = 1
        if int(any_string) in range(1,7): #Many ID fields range from 1 to 6
            types['LowID'] = 1
        if int(any_string) in range(1,266): #Location ID ranges from 1 to 265
            types['LocationID'] = 1
            
    #Check to see if it's a Y or N
    if base_type(any_string) == 'Character':
        if any_string == 'Y' or any_string == 'N':
            types['Flag'] = 1
            
    keys = []

    for key, value in types.iteritems():
        if value == 1:
            keys.append(key)

    return ", ".join(keys)

def validate_date(date):
    missing = ['0', 'NULL', 'NaN', '']
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

sc = SparkContext()
lines = sc.textFile('new_schema/yellow_*.csv,old_schema/yellow_*.csv,new_schema/green_*.csv,new_schema/green_*.csv')
lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) >= 1).filter(lambda x: x[1] != 'tpep_pickup_datetime')
pickup_dates = lines.map(lambda x: x[1]).map(lambda x: (x, base_type(x), semantic_type(x), validate_date(x)))

pickup_dates.saveAsTextFile('pickup_dates.out')

sc.stop()