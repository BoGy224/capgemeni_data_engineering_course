USE DATABASE nyc_airbnb;
USE WAREHOUSE nyc_warehouse;

-- Create a table to store the raw Airbnb data
CREATE OR REPLACE TABLE nyc_data (
    id INT,
    name STRING,
    host_id INT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude FLOAT,
    longitude FLOAT,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
);

--  Create a new table for the transformed data
CREATE OR REPLACE TABLE nyc_transformed_data (
    id INT,
    name STRING,
    host_id INT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude FLOAT,
    longitude FLOAT,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
);

--  Set up continuous ingestion using Streams to track changes in the data for incremental loads.
CREATE OR REPLACE STREAM nyc_data_stream
ON TABLE NYC_AIRBNB.PUBLIC.NYC_DATA;

CREATE OR REPLACE STREAM nyc_transformed_data_stream
ON TABLE NYC_AIRBNB.PUBLIC.NYC_TRANSFORMED_DATA;

--Transform the Data in Snowflake and save the transformed data to this new table:
INSERT INTO nyc_transformed_data
SELECT
    id,
    name,
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    latitude,
    longitude,
    room_type,
    price,
    minimum_nights,
    number_of_reviews,
    COALESCE(TRY_TO_DATE(last_review), (SELECT MIN(last_review) FROM nyc_data)) AS last_review,
    COALESCE(reviews_per_month, 0) AS reviews_per_month,
    calculated_host_listings_count,
    availability_365
FROM nyc_data
WHERE price > 0
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

--  Create a Task in Snowflake to automate the transformation process daily
CREATE OR REPLACE TASK transform_task
  WAREHOUSE = nyc_warehouse
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Runs daily at midnight
AS
  INSERT INTO NYC_AIRBNB.PUBLIC.NYC_TRANSFORMED_DATA
  SELECT
      id,
      name,
      host_id,
      host_name,
      neighbourhood_group,
      neighbourhood,
      latitude,
      longitude,
      room_type,
      price,
      minimum_nights,
      number_of_reviews,
      COALESCE(TRY_TO_DATE(last_review), (SELECT MIN(last_review) FROM NYC_AIRBNB.PUBLIC.NYC_DATA)) AS last_review,
      COALESCE(reviews_per_month, 0) AS reviews_per_month,
      calculated_host_listings_count,
      availability_365
  FROM NYC_AIRBNB.PUBLIC.NYC_DATA
  WHERE price > 0
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

ALTER TASK transform_task RESUME;

--  Ensure that there are no NULL values in critical columns (price, minimum_nights, and availability_365)
SELECT * FROM nyc_transformed_data
WHERE price IS NULL
   OR minimum_nights IS NULL
   OR availability_365 IS NULL;

-- Implement Time Travel to recover from errors by querying historical data
SELECT * FROM nyc_transformed_data AT (OFFSET => -1);