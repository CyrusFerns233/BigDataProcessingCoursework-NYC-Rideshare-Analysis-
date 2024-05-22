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

    # Group data by 'Pickup_Borough' and 'month', then count the number of trips in each group
    pickup_borough_grouped = final_df_with_month.groupBy("Pickup_Borough", "month").count()
    
    # Rename the 'count' column to 'trip_count' for clarity
    pickup_borough_grouped = pickup_borough_grouped.withColumnRenamed("count", "trip_count")
    
    # Define window specification to partition data by 'month' and order within each partition by 'trip_count' in descending order
    windowSpec = Window.partitionBy("month").orderBy(F.desc("trip_count"))
    
    # Use dense_rank to find the top 5 within each partition
    top_pickup_boroughs = pickup_borough_grouped.withColumn("rank", F.dense_rank().over(windowSpec)).filter(F.col("rank") <= 5)
    
    # Drop the 'rank' column as it is not required in the output
    top_pickup_boroughs = top_pickup_boroughs.drop("rank")
    
    # Show the top 5 popular pickup boroughs each month
    top_pickup_boroughs.show(100)
    
        
    # Group data by 'Dropoff_Borough' and 'month', and count the number of trips in each group
    dropoff_borough_grouped = final_df_with_month.groupBy("Dropoff_Borough", "month").count()
    
    # Rename the 'count' column to 'trip_count' for clarity
    dropoff_borough_grouped = dropoff_borough_grouped.withColumnRenamed("count", "trip_count")
    
    # Use the same window specification as before for dense ranking
    top_dropoff_boroughs = dropoff_borough_grouped.withColumn("rank", F.dense_rank().over(windowSpec)).filter(F.col("rank") <= 5)
    
    # Drop the 'rank' column as it is not required in the output
    top_dropoff_boroughs = top_dropoff_boroughs.drop("rank")
    
    # Show the top 5 popular dropoff boroughs each month
    top_dropoff_boroughs.show(100)
    
        
        # Add a new column 'Route' that concatenates 'Pickup_Borough' and 'Dropoff_Borough'
    final_df_with_routes = final_df.withColumn("Route", F.concat_ws(" to ", "Pickup_Borough", "Dropoff_Borough"))
    
    # Group data by 'Route' and sum the 'driver_total_pay' to get the total profit for each route
    route_profit_grouped = final_df_with_routes.groupBy("Route").agg(F.sum("driver_total_pay").alias("total_profit"))
    
    # Order the entire dataset by 'total_profit' in descending order
    top_routes = route_profit_grouped.orderBy(F.desc("total_profit")).limit(30)
    
    # Show the top 30 earnest routes without truncating the 'Route' column
    top_routes.show(30, truncate=False)


    spark.stop()