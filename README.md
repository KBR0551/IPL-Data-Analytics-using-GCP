# <img src="https://github.com/user-attachments/assets/b52f367f-d2e0-4233-82b6-2c4683cd6a15" width="30"/> GCP Data Engineering Project: Building and Orchestrating an ETL Pipeline for cricket data analysis to generate reports that are on https://www.iplt20.com/stats/<year> IPL website.
In this repository I have a developed ETL data pipeline using GCP services like GCS, Big Query, Cloud composer(Airflow) and on premise Apache spark to demonstrate the use of hybrid cloud. the repository is python based.
 - <img src="https://github.com/user-attachments/assets/929c57e4-0cfe-4dd2-a0b4-751a7a92dc9e" width="20"/> GCS is used to store and manage the transactional data
 - <img src="https://github.com/user-attachments/assets/c474f33c-c3b9-4631-9703-d44965a8277b" width="20"/> Composer, a managed Apache Airflow service, is utilized to orchestrate Dataflow jobs 
 - <img src="https://github.com/user-attachments/assets/729ae49d-7b4d-47ee-bd1c-7936fe26196c" width="20"/>  BigQuery serves as a serverless data warehouse
 - <img src="https://github.com/user-attachments/assets/d115e980-990b-4a04-9f3e-c55ae0b4123b" width="20"/>  Dataproc to run server less pyspark cluter to convert file format/trasnformations etc. 
- <img src="https://github.com/user-attachments/assets/95c82bb0-70a3-4cd0-a3ca-277a6814a356" width="20"/> IAM to manage resource policies and permissions 
