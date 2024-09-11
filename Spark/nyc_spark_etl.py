# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 15:15:28 2024

@author: Влад
"""

import os
#Set your env parameters if you need
#os.environ["SPARK_HOME"] = r"D:\Spark"
#os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
#os.environ["HADOOP_HOME"] = r"C:\Users\Влад\Downloads\hadoop-3.3.6-aarch64"
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit,min
from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DateType,FloatType
import logging


PATH = 'D:/capgemeni/capgemeni_data_engineering_course/Spark/'
processed_list_path = PATH + "processed/processed_list.txt"
log_file_path = PATH + "logs/NYC_Airbnb_ETL.log"

customSchema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('host_id', IntegerType(), True),
    StructField('host_name', StringType(), True),
    StructField('neighbourhood_group', StringType(), True),
    StructField('neighbourhood', StringType(), True),
    StructField('latitude', FloatType(), True),
    StructField('longitude', FloatType(), True),
    StructField('room_type', StringType(), True),
    StructField('price', FloatType(), True),
    StructField('minimum_nights', IntegerType(), True),
    StructField('number_of_reviews', IntegerType(), True),
    StructField('last_review', DateType(), True),
    StructField('reviews_per_month', StringType(), True),
    StructField('calculated_host_listings_count', FloatType(), True),
    StructField('availability_365', IntegerType(), True)])

logging.basicConfig(filename=log_file_path,
                    level=logging.INFO,
                    filemode = 'a',
                    format="%(asctime)s %(levelname)s %(message)s")

def load_processed_files():
    """Load list of already processed files from log."""
    if os.path.exists(processed_list_path):
        with open(processed_list_path, "r") as log_file:
            processed_files = log_file.read().splitlines()
        return set(processed_files)
    return set()

def update_processed_files(file_name):
    """Add new processed file to log."""
    with open(processed_list_path, "a") as log_file:
        log_file.write(file_name + "\n")

# Deduplicate by filtering out rows present in the processed data
def deduplicate(new_data, existing_data):
    return new_data.join(existing_data, on=["host_id", "latitude", "longitude", "room_type", "price"], how="left_anti")    
    

def transform_data(data):
    data = data.filter(col("price") > 0)

    # Convert 'last_review' to date type
    data = data.withColumn("last_review", to_date("last_review", "yyyy-MM-dd"))

    # Fill missing last_review values with the earliest date in the dataset or a default value
    earliest_date = data.select(min("last_review")).collect()[0][0]
    data = data.fillna({"last_review": earliest_date.strftime("%Y-%m-%d")})

    # Fill missing 'reviews_per_month' with 0
    data = data.fillna({"reviews_per_month": 0})

    # Drop rows with missing latitude or longitude
    data = data.dropna(subset=["latitude", "longitude"])
    budget = data.approxQuantile("price", [0.33], 0.25)[0]
    mid_range = data.approxQuantile("price", [0.66], 0.25)[0]
    # Add price category column
    data = data.withColumn("price_category", when(col("price") < budget, "budget")
                       .when((col("price") >= budget) & (col("price") < mid_range), "mid-range")
                       .otherwise("luxury"))

    # Add price_per_review column
    data = data.withColumn("price_per_review", col("price") / (col("number_of_reviews") + lit(1)))
    return data

def SQL_Queries(data):
    # SQL Query 1: Listings by Neighborhood Group
    listings_by_neighborhood  = spark.sql("""
            SELECT neighbourhood_group, COUNT(*) AS listing_count
            FROM airbnb_data GROUP BY neighbourhood_group
            ORDER BY listing_count DESC
        """)
    listings_by_neighborhood.show()

    # SQL Query 2: Top 10 most expensive listings
    top_10_expensive_listings  = spark.sql("""
            SELECT id, name, price FROM airbnb_data
            ORDER BY price DESC LIMIT 10
        """)
    top_10_expensive_listings.show()

    # SQL Query 3: Average Price by Room Type
    avg_price_by_room_type = spark.sql("""
            SELECT neighbourhood_group, room_type, AVG(price) AS avg_price
            FROM airbnb_data GROUP BY neighbourhood_group, room_type
        """)
    avg_price_by_room_type.show()
   
def save_data(data):
    partitioned_data = data.repartition("neighbourhood_group")
    
    partitioned_data.write.partitionBy("neighbourhood_group").\
        mode('append').parquet(PATH + "processed/")
    
def data_validation(data):
    null_price_count = data.filter(col("price").isNull()).count()
    null_minimum_nights_count = data.filter(col("minimum_nights").isNull()).count()
    null_availability_365_count = data.filter(col("availability_365").isNull()).count()

    # Assert that there are no NULL values
    if null_price_count == 0:
        logging.info("No NULL values found in 'price' column.")
    else:
        logging.debug(f"Found {null_price_count} NULL values in 'price' column.")

    if null_minimum_nights_count == 0:
        logging.info("No NULL values found in 'minimum_nights' column.")
    else:
        logging.debug(f"Found {null_minimum_nights_count} NULL values in 'minimum_nights' column.")

    if null_availability_365_count == 0:
        logging.info("No NULL values found in 'availability_365' column.")
    else:
        logging.debug(f"Found {null_availability_365_count} NULL values in 'availability_365' column.")
    count = data.count()
    return count
    
    
def data_process(batch_df, batch_id):
    count = 0
    file_name = "NYC_Airbnb_ETL" + str(batch_id)
    #file_name = batch_df.input_file_name().distinct().collect()[0]
    logging.info(f"Start data process with batch_id:{batch_id}")
    batch_df.show()

    try:
        # Transform the Data using PySpark:
        logging.info("Start data transformation")
        batch_df = transform_data(batch_df)
        
        # Register the DataFrame as a Temporary SQL Table
        batch_df.createOrReplaceTempView("airbnb_data")
        
        # Perform SQL Queries using PySpark SQL:
        logging.info("Provide SQL queries")
        batch_df = SQL_Queries(batch_df)
    
        # Save the Data using PySpark
        logging.info("Save data as parquet")
        save_data(batch_df)
    
        # Data Quality Checks using PySpark:
        logging.info("Data validation process")
        count = data_validation(batch_df)
    
        update_processed_files(file_name)
        logging.info(f"Data process with batch_id:{batch_id} is successfully ended, row count: {count}")
    except Exception as e:
        logging.error(f"Error while data process: {e}")
        print(f"Error while data process: {e}")
    
spark = SparkSession.builder.appName("NYC_Airbnb_ETL").getOrCreate()

# Monitor the 'raw' folder for new CSV files
raw_data_stream = spark.readStream.csv(PATH + "raw",header = True,schema = customSchema)

raw_data_stream.writeStream \
    .trigger(processingTime='10 seconds') \
    .format("csv") \
    .option("path", PATH + "processed/") \
    .option("checkpointLocation", PATH + "checkpoint/") \
    .start()
    
#Cleaning: Remove rows with missing values in essential columns
cleaned_data_stream = raw_data_stream.filter(col("host_id").isNotNull()) \
                                     .filter(col("price").isNotNull())
                                                                       
processed_data = spark.read.csv(PATH +"processed/",header = True,schema = customSchema)

deduplicated_data = deduplicate(cleaned_data_stream, processed_data)

deduplicated_data.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", PATH + "processed/") \
    .option("checkpointLocation", PATH + "checkpoint/") \
    .start()

stream = raw_data_stream.writeStream.foreachBatch(data_process).start()
stream.awaitTermination()







