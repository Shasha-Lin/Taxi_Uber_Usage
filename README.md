# big_data_term_project

Directory structure

Our code analyzes all monthly files from 2015-2016 of the taxi dataset found here: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

## Data Folder Preparation

<b> Data file directories for this group's code is accessible on hadoop fs at /user/cer446/new_schema/ and /user/cer446/old_schema/ </b> <p>Old_schema dir includes Yellow/Green data from 2015 until 2016 June. <br/>New schema dir includes Yellow/Green data from 2016 July until 2017. FHV data is at the main level of the directory.<br/>
For example, use '/user/cer446/new_schema/yellow*.csv,/user/cer446/old_schema/yellow*.csv' to access all yellow taxi data from 2015-01 to 2016-12. These paths are the default file references for all our code in this repo. Alternatively you may recreate the file structure locally (details below). <br/>

The number, names, and order of columns are different in yellow and green files from 2016-07 onwards. In order to run our unmodified code, please imitate our file structure as follows:<br/>

1. Download all monthly files for yellow, green, and fhv taxis from 2015-01 through 2016-12
2. Don't change the file names. If you do change the file names, keep "yellow_" "green_" or "fhv_" as the first part of each file name.
3. Put all green or yellow files from 2015-01 through 2016-06 in a folder called "old_schema"
4. Put all green or yellow files from 2016-07 through 2016-12 in a folder called "new_schema"
5. The old_schema and new_schema folders should be in the top-level (not within another directory) of the hdfs folder where pyspark will locate its input files by default. If running using NYU's Dumbo cluster, the top directory will follow this form: hdfs://babar.es.its.nyu.edu:8020/user/netid/, so the folders containing the taxi data should follow this form: hdfs://babar.es.its.nyu.edu:8020/user/netid/old_schema.
6. Put the fhv files directly in the top-level directory, not in the new_schema or old_shema folders.

## Data Quality Issues
Scripts for data quality investigation are in ./validation.
<ol>
<li>validation_combined.py outputs the base type, semantic type, valid/invalid/null for each value in a specified column in a specified monthly data file.<br/>
It is run by: "spark-submit validation_combined.py &#60;color&#62; &#60;column&#62; &#60;output directory&#62;<br/>
where color is green or yellow<br/>
column is the lowercase name of the columnof interest: e.g. pickup_longitude<br/>
output directory is where you would like to see your output in the hdfs: e.g. user/sl4964<br/>


<li>
aggregation.py takes the output from validation_combined.py and counts the total number of each unique *base type + semantic type + validity type* combination. An example output file from aggregation.py looks like this:<br/>
<p>Decimal,Distance/Currency/Lat/Long,VALID,212185502<br/>
Integer,Count,NULL,3293115<br/>
Decimal,Distance/Currency,VALID,37220<br/>
Decimal,Distance/Currency,INVALID,1<br/>
Integer,LowID/Count/LocationID,INVALID,3<br/>
Decimal,Distance/Currency/Lat/Long,INVALID,3628<br/>
Decimal,None,INVALID,3<br/>
Decimal,Lat/Long,INVALID,37<br/>
<p>&#42; "/" indicates an ambiguous semantic type, in which case the correct semantic type for each column is then inferred from the majority values.

<li>
validation_green_all_cols.sh and validation_yellow_all_cols.sh run validation_combined.py and aggregation.py for each column in the data to generate all the relevant output.

<li>
format_validation_results.py formats the final output produced by running all the scripts as specified in validation_green_all_cols.sh and validation_yellow_all_cols.sh. The script combines the separate .out files into one and produces totals formatted for latek.
</li>
</ol>

## Data Summary
Trips per Day and Trips per Month

The following pyspark code should be run using an HPC cluster such as NYU's Dumbo cluster:

+ by_date_pickup_fhv.py 
+ by_date_pickup_green.py 
+ by_date_pickup_yellow.py 
+ by_minute.py
+ nhbr_data_PySpark.py

Run using the commands:  
+ spark-submit by_date_pickup_fhv.py 
+ spark-submit by_date_pickup_green.py 
+ spark-submit by_date_pickup_yellow.py 
+ spark-submit by_minute.py
+ spark-submit nhbr_data_PySpark.py

This produces the following output files:  

+ by_date_pickup_fhv.out 
+ by_date_pickup_yellow.out 
+ by_date_pickup_green.out 
+ avg_tra_day.out 
+ yellow_2016

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

## Data Exploration

Trips per Day and Month, Citi Bike included.

The citi bike data can be downloaded here: https://www.citibikenyc.com/system-data

It is also saved on Dumbo in /user/cer446/citibike/

In addition to previous spark scripts for the taxi data, run the following spark script for the citibike data:

by_date_citibike.py

Run using the command: spark-submit by_date_citibike.py

And run the following python script to aggregate and visualize results:

trips_per_day_and_month_viz_part2.py

Place the output file with the name unchanged in the same directory as the following visualization code:  

python trips_per_day_and_month_viz_part2.py

And run the code using this command:

python trips_per_day_and_month_viz_part2.py

You should see Trips_Per_Month_all.png and Trips_Over_Time_Transportation_Method.png appear in the same directory.
