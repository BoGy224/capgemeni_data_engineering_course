--  Create a Snowflake database named nyc_airbnb.
CREATE DATABASE IF NOT EXISTS nyc_airbnb;

--  Create a Snowflake Warehouse for executing SQL queries.
CREATE IF NOT EXISTS WAREHOUSE nyc_warehouse
WITH 
  WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE DATABASE nyc_airbnb;
USE WAREHOUSE nyc_warehouse;

-- Use Snowpipe to automatically ingest the raw CSV data from the cloud storage bucket into a Snowflake Stage
CREATE OR REPLACE STORAGE INTEGRATION new_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<aws_role_arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<backet>/<path>/');
  

CREATE OR REPLACE STAGE airbnb_stage
    URL = 's3://<backet>/<path>/'
    STORAGE_INTEGRATION = new_int;

CREATE OR REPLACE PIPE airbnb_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO NYC_AIRBNB.PUBLIC.NYC_DATA
    FROM @NYC_AIRBNB.PUBLIC.AIRBNB_STAGE
    FILE_FORMAT = (TYPE = 'CSV', ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER = 1);