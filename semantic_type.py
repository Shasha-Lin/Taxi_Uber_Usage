def semantic_type(any_string):
    types = \
    pd.Series(index = ['PO/DO Datetime', 'Lat/Long', 'Currency', 'Distance', 'LowID', 'LocationID', 'Count', 'Flag'])
    
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
    try:
        #Check to see if it's a positive integer (no maximum value)
        if int(any_string) == float(any_string) and int(any_string) > 0:
            types['Currency'] = 1
            types['Distance'] = 1
            types['Count'] = 1
            if int(any_string) in range(1,7): #Many ID fields range from 1 to 6
                types['LowID'] = 1
            if int(any_string) in range(1,266): #Location ID ranges from 1 to 265
                types['LocationID'] = 1
    #error handling for strings that can't be converted to integer or float
    except TypeError:
        pass
    except ValueError:
        pass
    #if doesn't match any types in our data return none
    if types.loc[types.notnull()==True].index.tolist() == []:
        return 'None'
    #else return a list of the types that match
    else:
        return ", ".join(types.loc[types.notnull()==True].index.tolist())