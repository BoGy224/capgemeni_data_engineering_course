# Databricks NYC Airbnb ETL Pipeline

## Directory Structure

The directory structure for this project is as follows:
```
/Snowflake/
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
├── create_tables.sql
├── nyc_airbnb_etl.py
├── README.md
├── restore_silver_table.py
```
- **`chunks/`**: Contains the raw CSV files.
- **`create_tables.sql`**: Script which create database, bronze and silver tables.
- **`nyc_airbnb_etl.py`**: ETL script which ingest, transform, and load data into bronze and silver tables.
- **`restore_silver_table.py`**: Script which restore silver table to previous state.

## Setup

- **Databricks Account** Go to Databricks's website (https://www.databricks.com/try-databricks/thank-you-aws) and sign up for a trial account.
- **Create Databricks cluster** Create an Databricks cluster.
- **Upload files to DBFS** Upload files to DBFS from `chunks/` folder and save your DBFS data path.


## Running the ETL script

1. Check `create_tables.sql`, `nyc_airbnb_etl.py`, `restore_silver_table.py` files and change all data path to yours. 
2. Execute queries from `create_tables.sql` file. 
3. Run the `nyc_airbnb_etl.py` script. You can execute each try catch block as a cell in Databricks notebook.
4. If you need to restore data from previous state of silver table use `restore_silver_table.py` script.



