CREATE DATABASE airbnb_db;
USE airbnb_db;

CREATE EXTERNAL DATA SOURCE AirbnbBlobStorage
WITH (
    LOCATION = '<your-raw-data-location-path>'

);

CREATE EXTERNAL DATA SOURCE AirbnbBlobStorageTranformData
WITH (
    LOCATION = '<your-transformed-data-location-path>'

);

CREATE EXTERNAL FILE FORMAT CSVFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);

CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
WITH ( FORMAT_TYPE = PARQUET)

CREATE EXTERNAL TABLE [dbo].[RawTable] (
    id INT,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood_group NVARCHAR(255),
    neighbourhood NVARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(50),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review NVARCHAR(50),
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
)
WITH (
    LOCATION = '/',  -- Folder path where the raw CSV files are stored
    DATA_SOURCE = [AirbnbBlobStorage],
    FILE_FORMAT = [CSVFormat]
);

CREATE EXTERNAL TABLE [dbo].[TranformedData] (
    id INT,
    name NVARCHAR(255),
    host_id INT,
    host_name NVARCHAR(255),
    neighbourhood_group NVARCHAR(255),
    neighbourhood NVARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    room_type NVARCHAR(50),
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
)
WITH (
    LOCATION = '/',  -- Folder path where the raw CSV files are stored
    DATA_SOURCE = [AirbnbBlobStorageTranformData],
    FILE_FORMAT = [SynapseParquetFormat]
);
