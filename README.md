# <img src="https://github.com/user-attachments/assets/b52f367f-d2e0-4233-82b6-2c4683cd6a15" width="30"/> GCP Data Engineering Project: Building and Orchestrating an ETL Pipeline for cricket data analysis to generate reports that are on https://www.iplt20.com/stats/<year> IPL website.
In this repository I have a developed ETL data pipeline using GCP services like GCS, Big Query, Cloud composer(Airflow) and on premise Apache spark to demonstrate the use of hybrid cloud. the repository is python based.
 - <img src="https://github.com/user-attachments/assets/929c57e4-0cfe-4dd2-a0b4-751a7a92dc9e" width="20"/> GCS is used to store and manage the transactional data
 - <img src="https://github.com/user-attachments/assets/c474f33c-c3b9-4631-9703-d44965a8277b" width="20"/> Composer, a managed Apache Airflow service, is utilized to orchestrate Dataflow jobs 
 - <img src="https://github.com/user-attachments/assets/729ae49d-7b4d-47ee-bd1c-7936fe26196c" width="20"/>  BigQuery serves as a serverless data warehouse
 - <img src="https://github.com/user-attachments/assets/d115e980-990b-4a04-9f3e-c55ae0b4123b" width="20"/>  Dataproc to run server less pyspark cluter to extract & convert file format/trasnformations etc. 
- <img src="https://github.com/user-attachments/assets/95c82bb0-70a3-4cd0-a3ca-277a6814a356" width="20"/> IAM to manage resource policies and permissions 

These technologies work together to efficiently process, store, and generate reports on google cloud platform.

<img width="797" alt="image" src="https://github.com/user-attachments/assets/96d888cc-d2c6-42d2-a8c0-e2b0166033f0" />

# <img src="https://github.com/user-attachments/assets/8195e8b0-b96f-4f39-bed6-d83db3df0827" width="40"/> Resource setup
 - google cloud account
 - GCS Bucket creation and folders to organize code,data,configurations,dependencies,logs etc.
   <img width="356" alt="image" src="https://github.com/user-attachments/assets/baea73e9-924f-443c-a509-6e3b8a16e098" />

 - Basic Data procs cluster <br>
   <pre> ```gcloud dataproc clusters create < cluster_name > --enable-component-gateway --region us-central1 --master-machine-type n1-standard-2 --master-boot-disk-size 100 --num-workers 2 - worker-machine-type n1-standard-2 --worker-boot-disk-size 100 --image-version 2.1-debian11 --project vocal-chiller-457916-r2 --initialization-actions=gs://< bucket_name >/config/set_params.ksh```</pre> <br>
Replace cluster_name & bucket_name<br>
initialization-actions parameter to set up variables to later use in the pyspark script<br>

 - Create data sets in Biq Query `ipl_data_dwh`, `ipl_bowling_stats`, `ipl_batting_stats`
 - spin up a composer instance (Air flow) instance.
 - Assign Necessary permissions to service account.

# <img src="https://github.com/user-attachments/assets/929c57e4-0cfe-4dd2-a0b4-751a7a92dc9e" width="40"/> GCS

Upload the CSV files to your designated Google Cloud Storage (GCS) bucket. This data has information on every ball bowled in each IPL match, season, player infromation, match venu information, teams information.etc. File will need to be uploaded to ``gs://<bucket>/data/raw_data``

![image](https://github.com/user-attachments/assets/4df03cfb-f11e-4a22-9fd8-5ad318784755)

#  <img src="https://github.com/user-attachments/assets/729ae49d-7b4d-47ee-bd1c-7936fe26196c" width="40"/>

Create data sets `ipl_data_dwh`, `ipl_batting_stats`, `ipl_bowling_stats` to create table and store the raw data, create final bowling & batting stats data marts.
Necessary SQL files will be stored in /sql directory as shown below.

![image](https://github.com/user-attachments/assets/fa3ebbdd-dd02-4af8-bd37-1d030a8573ff) Biq Query

`ipl_table_ddl.sql`: create table statemens to create table in `ipl_data_dwh` dataset.<br>
`bq_bowling_stats_temp_tables.sql`: create temp tables for bowling stats in bq which will later be used to create final bowling stats.<br>
`bq_batting_stats_temp_tables.sql`: create temp tables for batting stats in bq which will later be used to create final batting stats.<br>
`bowling_stats.sql`: sql's to create final bowling stats in `ipl_bowling_stats` dataset.<br>
`batting_stats.sql`: sql's to create final batting stats in `ipl_batting_stats` dataset.<br>
`cleanup_temp_tables.sql`: drop all temp tables created.<br>

# <img src="https://github.com/user-attachments/assets/d115e980-990b-4a04-9f3e-c55ae0b4123b" width="40"/> Dataproc
Pyspark code to read csv files from `raw_data`, enforce schema and convert them to parquet format and store them in `parquer_data` directory. <br>
``pyspark_file_extract.py`` is parameterized to run again & again with different input file name ``--file_name``, run of running the pyspark code <br>

``gcloud dataproc jobs submit pyspark gs://{GCS_BUCKET}/code/pyspark_file_extract.py \
                                 --cluster={DATA_PROC_ClUSTER} \
                                 --region={LOCATION} \
                                 --py-files=gs://{GCS_BUCKET}/dependencies/utility.zip \
                                 -- \
                                --file_name {file_name}`` <br>
   
  ``file_name`` paramater will be passed from command line as show above, the file_name paramater will pick up:
  - schema file: <pre> ```schema_path=os.getenv("SCHEMA_FILE_PATH")+f"{file_name}_schema.json" ``` </pre> <br>
  - data file: <pre> ```df=spark.read.csv(os.getenv("SOURCE_FILE_PATH")+f"{file_name}.csv",header=True, schema=schema,mode="FAILFAST")```</pre> <br>
  where `SCHEMA_FILE_PATH` & `SOURCE_FILE_PATH` are evironment variable exported when creating dataproc cluster using ``--initialization-actions`` paramater

![image](https://github.com/user-attachments/assets/8f1f480a-4ca1-4dca-ae94-60ee00110f29)
