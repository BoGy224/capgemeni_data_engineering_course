# Snowflake NYC Airbnb ETL Pipeline

## Directory Structure

The directory structure for this project is as follows:
```
/Spark/
├── chunks/
│   ├── AB_NYC_2019_0.csv
│   ├── AB_NYC_2019_1.csv
│   ├── AB_NYC_2019_2.csv
│   ├── AB_NYC_2019_3.csv
│   ├── AB_NYC_2019_4.csv
│   ├── AB_NYC_2019_5.csv
│   ├── AB_NYC_2019_6.csv
│   ├── AB_NYC_2019_7.csv
│   ├── AB_NYC_2019_8.csv
│   ├── AB_NYC_2019_9.csv
├── pipeline.sql
├── configuration.sql
```
- **`chunks/`**: Contains the raw CSV files.

## Setup

- **Snowflake Account** Go to Snowflake's website (https://www.snowflake.com/login/) and sign up for a trial account.
- **SnowSQL CLI** Go to SnowSQL download page and download the SnowSQL CLI installer for your operating system.
- **Upload the CSV File to Cloud Storage** Create an AWS S3 bucket if you don't have one already and upload the CSV file to the S3 bucket.

## Running the ETL script

1. Prepare your AWS to automating load using this guide (https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3?gad_source=1&gclid=CjwKCAjwl6-3BhBWEiwApN6_kiZjlymxGgprr8C14CvKGLbwqOUozV5cZqHChl3fO9Xq4xuaCRNPXBoC2u4QAvD_BwE)
2. Change `configuration.sql` with your AWS settings and execute.
3. Execute the `pipepline.sql`
4. Use the Snowflake UI to monitor job execution.

