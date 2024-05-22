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

# Main execution definition for the script.
if __name__ == "__main__":

    # Create a Spark session for data processing.
    spark = SparkSession \
        .builder \
        .appName("NYC Rideshare Analysis") \
        .getOrCreate()

    # Retrieve environment variables for accessing the S3 data bucket.
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = f"{os.environ['S3_ENDPOINT_URL']}:{os.environ['BUCKET_PORT']}"
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    # Set the Hadoop configuration for connecting to S3 using the retrieved environment variables.
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Define the file paths for the source datasets in the S3 bucket.
    rideshare_data_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/rideshare_data.csv"
    taxi_zone_lookup_path = f"s3a://{s3_data_repository_bucket}/ECS765/rideshare_2023/taxi_zone_lookup.csv"

    # Load the rideshare data and taxi zone lookup data into DataFrames with headers.
    rideshare_data_df = spark.read.option("header", "true").csv(rideshare_data_path)
    taxi_zone_lookup_df = spark.read.option("header", "true").csv(taxi_zone_lookup_path)

    # Join the rideshare data with the taxi zone lookup on the pickup location.
    # Rename relevant columns for clarity post-join.
    rideshare_with_pickup_df = rideshare_data_df.join(
        taxi_zone_lookup_df,
        rideshare_data_df.pickup_location == taxi_zone_lookup_df.LocationID,
        "left"
    ).withColumnRenamed("Borough", "Pickup_Borough") \
     .withColumnRenamed("Zone", "Pickup_Zone") \
     .withColumnRenamed("service_zone", "Pickup_service_zone")

    # Drop the redundant 'LocationID' column after the join.
    rideshare_with_pickup_df = rideshare_with_pickup_df.drop('LocationID')

    # Repeat the join process for dropoff locations.
    final_df = rideshare_with_pickup_df.join(
        taxi_zone_lookup_df,
        rideshare_with_pickup_df.dropoff_location == taxi_zone_lookup_df.LocationID,
        "left"
    ).withColumnRenamed("Borough", "Dropoff_Borough") \
     .withColumnRenamed("Zone", "Dropoff_Zone") \
     .withColumnRenamed("service_zone", "Dropoff_service_zone")

    # Drop the 'LocationID' column after the dropoff join.
    final_df = final_df.drop('LocationID')

    # Convert the UNIX timestamp in the 'date' column to a human-readable date format.
    final_df = final_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

    # Display the first few rows to verify the DataFrame's contents after the transformations.
    final_df.show(5, truncate=False)
    # Print the schema to verify data types and column names post-joins.
    final_df.printSchema()

    # Count and print the total number of rows in the DataFrame to confirm data integrity.
    print("Counting the total number of rows in the DataFrame...")
    row_count = final_df.count()
    print(f"Total number of rows after join: {row_count}")

    # Terminate the Spark session to free up resources.
    spark.stop()