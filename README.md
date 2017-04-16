# big_data_term_project

Directory structure

Our code analyzes all monthly files from 2015-2016 of the taxi dataset found here: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

## Data Folder Preparation
The number, names, and order of columns are different in yellow and green files from 2016-07 onwards. In order to run our unmodified code, please imitate our file structure as follows:

1. Download all monthly files for yellow, green, and fhv taxis from 2015-01 through 2016-12
2. Don't change the file names. If you do change the file names, keep "yellow_" "green_" or "fhv_" as the first part of each file name.
3. Put all green or yellow files from 2015-01 through 2016-06 in a folder called "old_schema"
4. Put all green or yellow files from 2016-07 through 2016-12 in a folder called "new_schema"
5. The old_schema and new_schema folders should be in the top-level (not within another directory) of the hdfs folder where pyspark will locate its input files by default. If running using NYU's Dumbo cluster, the top directory will follow this form: hdfs://babar.es.its.nyu.edu:8020/user/netid/, so the folders containing the taxi data should follow this form: hdfs://babar.es.its.nyu.edu:8020/user/netid/old_schema.
6. Put the fhv files directly in the top-level directory, not in the new_schema or old_shema folders.

## Data Quality Issues
Scripts for data quality investigation are in ./validation.
<ol>
<li>RequiredOutput.py outputs the base type, semantic type, valid/invalid/null for each value in a specified column in a specified monthly data file.<br/>
It is run by: "spark-submit RequiredOutput.py &#60;file&#62; &#60;column&#62; &#60;output directory&#62;<br/>
where file is any monthly green/yellow cab data files between 2015-01 and 2016-12<br/>
column is the exact name of the columnof interest: e.g. Pickup_longitude<br/>
output directory is where you would like to see your output in the hdfs: e.g. user/sl4964<br/>

<p>&#42;This script handles any column name other than:<br/>
tpep_pickup_datetime, lpep_pickup_datetime, tpep_dropoff_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, trip_distance/Trip_distance<br/>
These columns are handled separately by other scripts in ./validation</p>
</li>
</ol>

## Data Summary
Trips per Day and Trips per Month

The following pyspark code should be run using an HPC cluster such as NYU's Dumbo cluster:

+ by_date_pickup_fhv.py 
+ by_date_pickup_green.py 
+ by_date_pickup_yellow.py 

Run using the commands:  
+ spark-submit by_date_pickup_fhv.py 
+ spark-submit by_date_pickup_green.py 
+ spark-submit by_date_pickup_yellow.py 

This produces the following output files:  

+ by_date_pickup_fhv.out 
+ by_date_pickup_yellow.out 
+ by_date_pickup_green.out 

Place each output file with the name unchanged in the same directory as the following visualization code:  

trips_per_day_and_month_viz.py

And run the code using this command:

python trips_per_day_and_month_viz.py

You should see Pickups_per_Day.png and Pickups_per_Month.png appear in the same directory.

Trips by Borough

The following pyspark code should be run using an HPC cluster such as NYU's Dumbo cluster:

+ by_location.py

Run using the command: spark-submit by_location.py

This produces the output file by_borough.out.

Place the output file with the name unchanged in the same directory as the following visualization code:  

by_borough_viz.py

And run the code using this command:

python by_borough_viz.py

You should see heatmap_borough_all.png and heatmap_borough_exc_manhattan.png appear in the same directory.


