import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.functions import date_format
from pyspark.sql.functions import month
from pyspark.sql.functions import sum, format_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date, dayofmonth

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("NYC Rideshare Analysis") \
        .getOrCreate()

    # Shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

# TASK 1 
    
    #i) Reading from the bucket file paths
    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/taxi_zone_lookup.csv"

    # Read the rideshare and taxi zone lookup data
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)
    taxi_zone_lookup_df = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    #ii) Join the rideshare data with the taxi zone lookup data on pickup_location
    rideshare_with_pickup_df = rideshare_data_df.join(
        taxi_zone_lookup_df,
        rideshare_data_df.pickup_location == taxi_zone_lookup_df.LocationID,
        "left"
    ).withColumnRenamed("Borough", "Pickup_Borough") \
     .withColumnRenamed("Zone", "Pickup_Zone") \
     .withColumnRenamed("service_zone", "Pickup_service_zone")

    # After the first join
    rideshare_with_pickup_df = rideshare_with_pickup_df.drop('LocationID')

    # Join the result with the taxi zone lookup data on dropoff_location
    final_df = rideshare_with_pickup_df.join(
        taxi_zone_lookup_df,
        rideshare_with_pickup_df.dropoff_location == taxi_zone_lookup_df.LocationID,
        "left"
    ).withColumnRenamed("Borough", "Dropoff_Borough") \
     .withColumnRenamed("Zone", "Dropoff_Zone") \
     .withColumnRenamed("service_zone", "Dropoff_service_zone")

    # After the second join
    final_df = final_df.drop('LocationID')

    #iii) Convert the 'date' column to a more readable format, if it's a UNIX timestamp
    final_df = final_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

    # Extract month from the date
    final_df_with_month = final_df.withColumn("month", F.month("date"))

    # Task 4a: Calculate the average 'driver_total_pay' during different 'time_of_day' periods
    average_pay_by_time_of_day = final_df_with_month.groupBy("time_of_day").agg(
        F.avg("driver_total_pay").alias("average_driver_total_pay")
    ).orderBy("average_driver_total_pay", ascending=False)
    average_pay_by_time_of_day.show()
    
    # Task 4b: Calculate the average 'trip_length' during different 'time_of_day' periods
    average_trip_length_by_time_of_day = final_df_with_month.groupBy("time_of_day").agg(
        F.avg("trip_length").alias("average_trip_length")
    ).orderBy("average_trip_length", ascending=False)
    average_trip_length_by_time_of_day.show()
    
    # Task 4c: Calculate the average earned per mile for each 'time_of_day' period
    average_earning_per_mile = average_pay_by_time_of_day.join(
    average_trip_length_by_time_of_day,
        "time_of_day"
    ).withColumn(
        "average_earning_per_mile", 
        F.col("average_driver_total_pay") / F.col("average_trip_length")
    ).select(
        "time_of_day", "average_earning_per_mile"
    )
    average_earning_per_mile.show()

    spark.stop()