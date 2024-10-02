# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 16:56:34 2024

@author: VladislavHnatiuk
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce,lit
from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DateType,FloatType
import logging

try:
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
        StructField('reviews_per_month', IntegerType(), True),
        StructField('calculated_host_listings_count', FloatType(), True),
        StructField('availability_365', IntegerType(), True)])


    log_file_path = "NYC_Airbnb_ETL.log"
    logging.basicConfig(filename=log_file_path,
                    level=logging.INFO,
                    filemode = 'a',
                    format="%(asctime)s %(levelname)s %(message)s")

    # Define the source path for CSV files
    source_path = "/FileStore/tables/source_data/"
    bronze_table_path = "s3://<backet>/delta/nyc_airbnb/bronze"
    silver_table_path = "s3://<backet>/delta/nyc_airbnb/silver"

    spark = SparkSession.builder.appName("NYC_Airbnb_ETL").getOrCreate()
except Exception as e:
    logging.error(f"Error while initialization: {e}")
    print(f"Error while initialization: {e}")
    

try: 
    #Create tables
    spark.sql("CREATE DATABASE IF NOT EXISTS nyc_airbnb;")
    
    spark.sql("""
              CREATE OR REPLACE TABLE nyc_airbnb.bronze
              (
                  id INT,
                  name STRING,
                  host_id INT,
                  host_name STRING,
                  neighbourhood_group STRING,
                  neighbourhood STRING,
                  latitude FLOAT,
                  longitude FLOAT,
                  room_type STRING,
                  price FLOAT,
                  minimum_nights INT,
                  number_of_reviews INT,
                  last_review DATE,
                  reviews_per_month INT,
                  calculated_host_listings_count FLOAT,
                  availability_365 INT
                  )
                  USING delta LOCATION 's3://<backet>/delta/nyc_airbnb/bronze/'
              """)
              
    spark.sql("""
              CREATE OR REPLACE TABLE nyc_airbnb.silver
              (
                  id INT,
                  name STRING,
                  host_id INT,
                  host_name STRING,
                  neighbourhood_group STRING,
                  neighbourhood STRING,
                  latitude FLOAT,
                  longitude FLOAT,
                  room_type STRING,
                  price FLOAT,
                  minimum_nights INT,
                  number_of_reviews INT,
                  last_review DATE,
                  reviews_per_month INT,
                  calculated_host_listings_count FLOAT,
                  availability_365 INT
                  )
              USING delta LOCATION 's3://<backet>/delta/nyc_airbnb/silver/'
              TBLPROPERTIES (
                  delta.constraints.price_check = 'price IS NOT NULL AND price > 0',
                  delta.constraints.minimum_nights_check = 'minimum_nights IS NOT NULL AND minimum_nights > 0',
                  delta.constraints.availability_check = 'availability_365 IS NOT NULL AND availability_365 >= 0'
                  )
              """)
except Exception as e:
    logging.error(f"Error while creating tables: {e}")
    print(f"Error while creating tables: {e}")
    
    
try:
    # Set up Auto Loader to read CSV files continuously from the source path
    df_bronze = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("cloudFiles.schemaLocation", "s3://<backet>/delta/nyc_airbnb/schema/")
             .schema(customSchema) 
             .load(source_path))

    (df_bronze.writeStream
           .format("delta")
           .option("checkpointLocation", "s3://<backet>/delta/nyc_airbnb/checkpoints/bronze") 
           .outputMode("append")
           .start(bronze_table_path))
    
except Exception as e:
    logging.error(f"Error while set up Auto Loader: {e}")
    print(f"Error while set up Auto Loader: {e}")
    
    
try:
    # Perform transformations on streaming data
    df_bronze_stream = spark.readStream.format("delta").load(bronze_table_path)

    df_silver_stream = df_bronze_stream.filter(df_bronze_stream['price'] > 0)

    # Step 2: Convert last_review to valid date and fill missing values with the earliest available date
    df_silver_stream = df_silver_stream.withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd"))
    earliest_review_date = spark.sql("select min(last_review) as export_time from  nyc_airbnb_etl.nyc_airbnb.bronze").first()[0]
    df_silver_stream = df_silver_stream.withColumn("last_review", coalesce(col("last_review"), lit(earliest_review_date)))

    # Step 3: Handle missing values in reviews_per_month by setting to 0
    df_silver_stream = df_silver_stream.na.fill(value=0, subset=["reviews_per_month"])

    # Step 4: Drop rows with missing latitude or longitude
    df_silver_stream = df_silver_stream.dropna(subset=["latitude", "longitude"])


    # Write the transformed data into Silver Delta table using Structured Streaming
    (df_silver_stream.writeStream
                 .format("delta")
                 .option("checkpointLocation", "s3://<backet>/delta/nyc_airbnb/checkpoints/silverStreame")
                 .outputMode("append")
                 .start(silver_table_path))
except Exception as e:
    logging.error(f"Error while transformations on streaming data: {e}")
    print(f"Error while transformations on streaming data: {e}")