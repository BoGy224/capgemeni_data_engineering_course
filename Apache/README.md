# Apache Airflof NYC Airbnb ETL Pipeline

## Directory Structure

The directory structure for this project is as follows:
```
/Apache/
├── raw/
│   └── AB_NYC_2019.csv
├── transformed/
├── dags/
│   └── nyc_airbnb_etl.py
├── logs/
│   
```
- **`dags/`**: Contains the DAG script.
- **`raw/`**: Contains the raw CSV file.
- **`transformed/`**: Stores the transformed CSV file.
- **`logs/`**: Stores the failure logs.

## Setup

- Run ```docker-compose up airflow-init``` and ```docker-compose up -d``` in comand line from work directory 
- **Apache Airflow** installed and running as docker container.
- **PostgreSQL** installed and a database named `airflow_etl` created.

## Configuring Parameters

### Airflow Variables

Set the following Airflow variables in the UI or through the Airflow CLI:

- **`FILE_PATH`**: Path to the raw data file (e.g., `/opt/airflow/raw/AB_NYC_2019.csv`).
- **`TRANSFORMED_DATA_PATH`**: Path where the transformed data will be saved (e.g., `/opt/airflow/transformed/transformed_AB_NYC_2019.csv`).
- **`DB_CONN_ID`**: Connection ID for PostgreSQL (e.g., `postgres_localhost`).
- **`TABLE_NAME`**: Table name in PostgreSQL DB (e.g., `airbnb_listings`).

### Airflow Connections

Ensure that a PostgreSQL connection is configured in the Airflow UI with the connection ID provided above.

## Running the DAG

1. Ensure the `AB_NYC_2019.csv` file is placed in the `raw/` directory.
2. Place the `nyc_airbnb_etl.py` script in `dags/` directory.
3. Trigger the DAG from the Airflow UI
