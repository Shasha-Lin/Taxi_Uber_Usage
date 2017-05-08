array1=("pulocationid" "dolocationid" "lpep_pickup_datetime" "lpep_dropoff_datetime" "trip_distance" "vendorid" "passenger_count" "pickup_longitude" "pickup_latitude" "dropoff_longitude" "dropoff_latitude" "ratecodeid" "fare_amount" "extra" "mta_tax" "payment_type" "store_and_fwd_flag" "tip_amount" "tolls_amount" "improvement_surcharge" "total_amount" "ehail_fee" "trip_type")

for i in "${array1[@]}";
do

spark-submit --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) validation_combined.py green $i /user/cer446/validation/

hdfs dfs -getmerge /user/cer446/validation/green_$i.out /home/cer446/project/validation/green_$i.out

hdfs dfs -put /home/cer446/project/validation/green_$i.out /user/cer446/aggregation/

rm /home/cer446/project/validation/green_$i.out

spark-submit --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) aggregation.py /user/cer446/aggregation/green_$i.out;

hdfs dfs -getmerge "/user/cer446/aggregation/total_green_$i.out" "/home/cer446/project/aggregation/total_green_$i.out";
    
done