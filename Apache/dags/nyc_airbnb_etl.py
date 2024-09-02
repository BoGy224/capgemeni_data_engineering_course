from datetime import datetime,timedelta
import pandas as pd
import os

from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable


default_args = {
	'owner': 'Vlad_Hnatiuk',
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

# Variable which need to initialize in Airflow UI
DB_CONN_ID = Variable.get("DB_CONN_ID", default_var="postgres_localhost")
TABLE_NAME = Variable.get("TABLE_NAME", default_var="airbnb_listings")
FILE_PATH = Variable.get("FILE_PATH", default_var="/opt/airflow/raw/AB_NYC_2019.csv")
TRANSFORMED_DATA_PATH = Variable.get("TRANSFORMED_DATA_PATH",
                                     default_var="/opt/airflow/transformed/transformed_AB_NYC_2019.csv")

def load_data_from_csv(**kwargs):
    try:
        # Check if the file exists
        if not os.path.exists(FILE_PATH):
            raise FileNotFoundError(f"File not found: {FILE_PATH}")
        
        # Read the CSV file into a Pandas DataFrame
        data = pd.read_csv(FILE_PATH)
        
        # Log the success and return the DataFrame
        print("File read successfully, number of rows:", len(data))
        
        # Store the DataFrame in XCom for further processing if needed
        kwargs['ti'].xcom_push(key='raw_data', value=data.to_dict())
        #return data
    except Exception as e:
        print(f"Error reading the file: {e}")
        raise

def data_transformation(**kwargs):
    ti = kwargs['ti']
    
    # Get Data from from ti
    data = pd.DataFrame(ti.xcom_pull(key = 'raw_data'))
    
    # Filter out rows where price is 0 or negative.
    transform_data = data[data.price >= 0].copy()
    
    # Convert last_review to a datetime object.
    transform_data['last_review'] = pd.to_datetime(transform_data['last_review'])
    
    # Handle missing (if any) last_review dates by filling them with the earliest 
    # date in the dataset or a default date
    transform_data['last_review'].fillna(method = 'ffill',inplace = True)
    
    # Handle missing values in reviews_per_month by filling them with 0
    transform_data['reviews_per_month'].fillna(0,inplace = True)
    
    # Drop any rows(if any) with missing latitude or longitude values
    transform_data = transform_data.dropna(subset=['latitude', 'longitude'])
    
    # Convert latitude,longitude,reviews_per_month columns to a format 
    # suitable for the table airbnb_listings
    transform_data['latitude'] = transform_data['latitude'].round(6) 
    transform_data['longitude'] = transform_data['longitude'].round(6)
    transform_data['reviews_per_month'][transform_data['reviews_per_month'] >= 10] = 9.99
    transform_data['reviews_per_month'] = transform_data['reviews_per_month'].round(2)
    
    # Write transform data to folder
    transform_data.to_csv(TRANSFORMED_DATA_PATH,index=False)

def insert_data_into_airbnb_listings(**kwargs):
    # Get data from file 
    transformed_data = pd.read_csv(TRANSFORMED_DATA_PATH)
    df = pd.DataFrame(transformed_data)
    # Create Hook
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    # Delete data from the database to prevent conflicts 
    pg_hook.run(f"TRUNCATE TABLE {TABLE_NAME}")
    tuples = [tuple(x) for x in df.to_numpy()]
    # Insert data to table
    pg_hook.insert_rows(TABLE_NAME, tuples, target_fields=None)


def data_quality_check(**kwargs):
    # Create Hook
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    # Select from db count of all row
    query = f"SELECT COUNT(*) FROM {TABLE_NAME};"
    table_count = pg_hook.get_first(query)[0]
    # Get tarnsformed data from file 
    transformed_data = pd.read_csv(TRANSFORMED_DATA_PATH)
    transformed_data_row_count = len(pd.DataFrame(transformed_data))
    # Check whether the number of rows from different sources is the same 
    if table_count != transformed_data_row_count:
        print(f"""Postgres insert error: Row count in db {table_count}, 
              but expected {transformed_data_row_count}""")
        return 'fail'
    # Get count of row where values is null 
    query = f""" SELECT COUNT(*) FROM {TABLE_NAME}
        WHERE price IS NULL OR minimum_nights IS NULL OR availability_365 IS NULL;
    """
    
    count_of_null = pg_hook.get_first(query)[0]
    
    # Validate that there are no NULL values in the price, minimum_nights, and 
    # availability_365 columns.
    if count_of_null > 0:
        print("NULL values found in critical columns of the PostgreSQL table.")
        return 'fail'

    return 'success'

def success(**kwargs):
    print('All data is valid!!!')
    
def fail(**kwargs):
    print('Error: data is not valid')

with DAG(
	dag_id = 'nyc_airbnb_etl',
	default_args = default_args,
	description = """ETL pipeline that 
                    automates the process of ingesting, transforming,
                    and loading data from a local CSV file 
                    (AB_NYC_2019.csv from New York City Airbnb Open
                    Data Kaggle dataset)""",
	start_date = datetime(2024,9,2),
	schedule_interval='0 0 * * *'
) as dag:  
    # Task 2. Create the PostgreSQL Database
    create_table = PostgresOperator(
        task_id='create_airbnb_listings_table',
        postgres_conn_id=DB_CONN_ID,
        sql="""CREATE TABLE IF NOT EXISTS airbnb_listings (
                id SERIAL PRIMARY KEY,
                name TEXT,
                host_id INTEGER,
                host_name TEXT,
                neighbourhood_group TEXT,
                neighbourhood TEXT,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                room_type TEXT,
                price INTEGER,
                minimum_nights INTEGER,
                number_of_reviews INTEGER,
                last_review DATE,
                reviews_per_month DECIMAL(3,2),
                calculated_host_listings_count INTEGER,
                availability_365 INTEGER
                );"""
    )
            
    # Task 4. Ingest Data from the CSV File:
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_from_csv
    )
    
    # Task 5. Transform the Data:
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=data_transformation
    )
    
    # Task 6. Load Data into PostgreSQL:
    insert_data = PythonOperator(
        task_id='insert_data_to_postgres',
        python_callable=insert_data_into_airbnb_listings,
        provide_context=True
    )    
    
    # Task 7. Implement Data Quality Checks:
    data_quality_checks = BranchPythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        provide_context=True
    )
    
    fail = PythonOperator(
        task_id='fail',
        python_callable=fail,
        provide_context=True
    )
    
    success = PythonOperator(
        task_id='success',
        python_callable=success,
        provide_context=True
    )
    
    # Tasks dependencies
    create_table >> load_data >> transform_data >> insert_data >> data_quality_checks
    data_quality_checks >> [success, fail]
	