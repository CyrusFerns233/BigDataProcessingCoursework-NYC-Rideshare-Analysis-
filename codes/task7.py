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
from pyspark.sql.functions import concat_ws, lit, when

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

    #task 2 Ensure the data types are correct before aggregating
    final_df = final_df.withColumn("rideshare_profit", final_df["rideshare_profit"].cast("float"))
    final_df = final_df.withColumn("driver_total_pay", final_df["driver_total_pay"].cast("float"))

    # Extract month from the date
    #final_df_with_month = final_df.withColumn("month", F.month("date"))


    final_df_with_routes = final_df.withColumn("Route", concat_ws(" to ", col("Pickup_Zone"), col("Dropoff_Zone")))

    # Aggregate to find the total trip counts for each unique route for Uber and Lyft
    # Assuming there's an indicator column 'business' in your DataFrame which distinguishes between Uber and Lyft
    route_counts = final_df_with_routes.groupBy("Route").pivot("business").count()
    
    # Calculate the total count across Uber and Lyft for each route
    route_counts = route_counts.fillna(0)  # Replace nulls with 0s for accurate calculations
    route_counts = route_counts.withColumn("total_count", col("Uber") + col("Lyft"))
    
    # Rename the columns for clarity
    route_counts = route_counts.withColumnRenamed("Uber", "uber_count") \
                               .withColumnRenamed("Lyft", "lyft_count")
    
    # Order the entire dataset by 'total_count' in descending order and take the top 10
    top_routes = route_counts.orderBy(col("total_count").desc()).limit(10)
    
    # Show the top 10 earnest routes without truncating the 'Route' names
    top_routes.show(10, truncate=False)

    
    spark.stop()