# Azure Data Factory ETL Pipeline for NYC Airbnb Data

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
├── AirbnbTransformPipeline_support_live.zip
├── init.sql
├── README.md
├── data_quality_check.sql
```
- **`chunks/`**: Contains the raw CSV files.
- **`init.sql`**: Script which create database, raw and transformed data tables.
- **`data_quality_check.sql`**: SQL script which provide  data quality checks.
- **`AirbnbTransformPipeline_support_live.zip`**: data factory support files 


## Setup

- **Azure Account** Go to Azure website (https://azure.microsoft.com/ru-ru/get-started/azure-portal/) and sign up for a trial account.
- **Create a Storage Account and Blob Container:**
 1. In the Azure Portal, navigate to Storage Accounts and create a new Storage Account.
 2. Inside the storage account, create a Blob Container.
- **Set Up Azure Synapse Analytics (Serverless SQL Pool):**
 1. In the Azure Portal, create an Azure Synapse Workspace.
 2. Inside the Synapse workspace, configure a Serverless SQL Pool.
 3. Define external tables in the serverless SQL pool for raw and processed data using `init.sql` SQL scripts. External tables are linked to the Blob Storage for querying CSV data.
- **Set Up Azure Data Factory (ADF):** 
 1. In the Azure Portal, create an Azure Data Factory instance.
 2. Use `AirbnbTransformPipeline_support_live.zip` to build an ETL pipeline.
 - **Configure Azure Monitor and Log Analytics:** 
 1. Set up a Log Analytics Workspace for pipeline diagnostics.
 2. Enable monitoring for your ADF pipelines and configure log collection.
 3. Set up alerts using Azure Monitor to track failures, long-running tasks, or other anomalies in your pipeline.

- **Upload files to DBFS** Upload files to DBFS from `chunks/` folder and save your DBFS data path.


## Running the ETL script

1. Check `init.sql` file and change all data path to yours. 
2. Upload the NYC Airbnb CSV files from `chunks/` file into the designated Blob container.
3. Trigger the Pipeline: Navigate to the Author section, select the ETL pipeline, and click Trigger Now.
4. Configure an event-based trigger in ADF to automatically run the pipeline when new files are uploaded to Blob Storage. In ADF, under the Triggers tab, create an event trigger. This ensures the pipeline runs as soon as new data is available
5. Run `init.sql` to create raw and transformed data table.
6. Run `data_quality_check.sql` script to provide data quality checks.



