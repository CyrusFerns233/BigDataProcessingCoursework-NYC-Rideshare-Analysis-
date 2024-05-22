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

    #Task 6a: Find trip counts greater than 0 and less than 1000
    trips_by_pickup_borough_time_of_day = final_df.groupBy("Pickup_Borough", "time_of_day") \
                                                  .count() \
                                                  .withColumnRenamed("count", "trip_count") \
                                                  .filter((col("trip_count") > 0) & (col("trip_count") < 1000)) \
                                                  .orderBy("Pickup_Borough", "time_of_day")
    
    # Show the results for Task 6a
    trips_by_pickup_borough_time_of_day.show()
    
    # Task 6b: Calculate the number of trips for each 'Pickup_Borough' in the evening
    evening_trips_by_pickup_borough = final_df.filter(col("time_of_day") == "evening") \
                                              .groupBy("Pickup_Borough") \
                                              .count() \
                                              .withColumnRenamed("count", "trip_count") \
                                              .withColumn("time_of_day", F.lit("evening")) \
                                              .select("Pickup_Borough", "time_of_day", "trip_count") \
                                              .orderBy("Pickup_Borough")
    
    # Show the results for Task 6b
    evening_trips_by_pickup_borough.show()
    
        
    # task 6c
    # A DataFrame is created by filtering the original 'final_df' DataFrame. 
    # This DataFrame contains only the records where the pickup was in Brooklyn and the dropoff was in Staten Island.
    brooklyn_to_staten_island_df = final_df.filter(
        (F.col("Pickup_Borough") == "Brooklyn") & 
        (F.col("Dropoff_Borough") == "Staten Island")
    )
    
    # The 'count()' action is used on the filtered DataFrame to find the total number of trips that meet the filter criteria.
    # This count is stored in the variable 'num_trips'.
    num_trips = brooklyn_to_staten_island_df.count()
    
    # A smaller DataFrame 'brooklyn_to_staten_island_samples' is created by selecting only the relevant columns
    # that identify the pickup borough, dropoff borough, and the specific pickup zone.
    brooklyn_to_staten_island_samples = brooklyn_to_staten_island_df.select(
        "Pickup_Borough", 
        "Dropoff_Borough", 
        "Pickup_Zone"
    )
    
    # The first 10 records of the 'brooklyn_to_staten_island_samples' DataFrame are displayed.
    # 'truncate=False' ensures that the data is shown completely without being cut off.
    brooklyn_to_staten_island_samples.show(10, truncate=False)

    # Output the total count of trips to the terminal or write it in a report
    print(f"Number of trips from Brooklyn to Staten Island: {num_trips}")


    spark.stop()