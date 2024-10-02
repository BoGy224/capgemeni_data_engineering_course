-- Create the database
CREATE DATABASE IF NOT EXISTS nyc_airbnb;

-- Create the Bronze table
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
                  USING delta LOCATION 's3://<backet>/delta/nyc_airbnb/bronze/';

-- Create the Silver table
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
                  );