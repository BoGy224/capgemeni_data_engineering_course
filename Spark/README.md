# Apache Spark NYC Airbnb ETL Pipeline

## Directory Structure

The directory structure for this project is as follows:
```
/Spark/
├── raw/
│   └── AB_NYC_2019.csv
├── processed/
├── logs/
├── nyc_spark_etl.py
```
- **`raw/`**: Contains the raw CSV file.
- **`processed/`**: Stores the already processed files and resuts.
- **`logs/`**: Stores the failure logs.

## Setup

- **Java** install JDK version 17 or 11
- **Hadoop** install version 3.x
- **Spark** install version 3.x
- Install pyspark library and edit environment variables JAVA_HOME, HADOOP_HOME, SPARK_HOME if you need

## Running the ETL script

1. Ensure the `AB_NYC_2019.csv` file is placed in the `raw/` directory.
2. Place the `nyc_spark_etl.py` script in `Spark/` directory.
3. Execute the `nyc_spark_etl.py`
4. Use Spark’s Web UI to monitor job execution
5. Look into `logs/` to check logs
