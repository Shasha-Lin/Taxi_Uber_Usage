data file directories for this group's code is accessible at /user/cer446/new_schema/ and /user/cer446/old_schema/
example: to access all yellow taxi data: '/user/cer446/new_schema/y*.csv,/user/cer446/old_schema/y*.csv'

nbhr_matices.py is the python interpretable version of nhbr_heatmaps.ipynb, both require 'taxi+_zone_lookup.csv'
from 'http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml'

AND

'nhbr_data2.csv', the output from nhbr_data_PySpark.py

tip.py also requires 'taxi+_zone_lookup.csv' AND
'trip_cost.csv', the output from nhbr_data_PySpark.py (in the second RDD map function, replace '$' with 'fare' and 'passenger' with 'tip')
