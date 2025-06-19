from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from google.cloud import storage
import re
import json

# Config 
GCS_BUCKET = 'xxxxxxxxxxxxxxxxxxxxxx' #replace with you GcS bucket Name
PROJECT_ID = 'xxxxxxxxxxxxxxxxxxxxxx' #replace with you GCP project ID
LOCATION = 'GCP-ZONE' #replace with the GCP zone you are working in ex: us-central-1
GCP_CONN_ID = 'google_cloud_default'
BQ_DATASET_NM='xxxx' #replace with bq dataset name where your tables and parquet file data is going to be stored 
BOWLING_STATS_DATASET_NM='xxxxxx' #replace with bq dataset name where you will store bowling stats 
BATTING_STATS_DATASET_NM='xxxxxx' #replace with bq dataset name where you will store batting stats 
DATA_PROC_ClUSTER='xxxxxxxx' #dataproc cluster name


# Function to read from gcs
def read_from_gcs(file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob=bucket.blob(f'{file_name}')
    data = blob.download_as_string().decode('utf-8')
    return data


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='ETL_pipeline_for_IPL_Data_Processing',
    default_args=default_args,
    description='Create BQ tables using DDL from GCS file and BigQueryInsertJobOperator using TaskGroup',
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'ddl', 'gcs'],
) as dag:
    
    #convert csv files to parquet using spark on dataproc,
    read_file_list = read_from_gcs(file_name='config/input_files_list.txt')
    input_file_list=[line.strip() for line in read_file_list.splitlines() if line.strip()]
    with TaskGroup(group_id='extract_and_convert_files') as extract_and_convert_files:  
          prev_task=None
          for idx, file_name in enumerate(input_file_list):
            current_task=BashOperator(
                task_id=f'extract_{file_name}',
                bash_command=f"""gcloud dataproc jobs submit pyspark gs://{GCS_BUCKET}/code/pyspark_file_extract.py \
                                 --cluster={DATA_PROC_ClUSTER} \
                                 --region={LOCATION} \
                                 --py-files=gs://{GCS_BUCKET}/dependencies/utility.zip \
                                 -- \
                                --file_name {file_name}
                             """,
            )
            if prev_task:
                prev_task >> current_task
            prev_task=current_task
    
    #create big query tables to store data
    read_ddl_queries = read_from_gcs(file_name='sql/ipl_table_ddls.sql')
    ddl_queries = [q.strip() for q in read_ddl_queries.split(';') if q.strip()]

    with TaskGroup(group_id='create_bq_tables') as create_bq_tables: # 
        prev_task=None
        for idx, query in enumerate(ddl_queries):
            match = re.search(r'`[^`]*\.(\w+)`\s*\(', query) #gets the table name from the sql statement which is used for creating task 
            table_name = match.group(1).strip().replace('.', '_')  if match else f"table_{idx}"
            current_task=BigQueryInsertJobOperator(
                task_id=f"create_{table_name}_table",
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,

                    }
                },
                params={
                     "project_id":PROJECT_ID,
                     "src_dataset_name":BQ_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )

            if prev_task:
                prev_task >> current_task
            prev_task=current_task
   
    #load data to Biqquery tables
    with TaskGroup(group_id='load_from_gcs_to_bq_table') as load_from_gcs_to_bq_table:
        tables =read_from_gcs(file_name='config/file_to_bq_tbl_map.json')  #get file to table name map to load using  GCSToBigQueryOperator
        data=json.loads(tables)
        prev_task=None
        for table, file in data.items():
            current_task=GCSToBigQueryOperator(
                task_id=f'load_{table}_table',
                bucket=GCS_BUCKET,
                source_objects=[f'data/parquet_data/{file}/*parquet'],
                destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NM}.{table}',
                source_format='PARQUET',
                autodetect=True,
                write_disposition='WRITE_TRUNCATE',
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )
          
            if prev_task:
                prev_task >> current_task
            prev_task=current_task
    
    #create necessary temp table to generate actual stats reports created in create_bowling_stats task
    bowling_temp_sql=read_from_gcs('sql/bq_bowling_stats_temp_tables.sql')
    create_bowling_stats_temp_tables=BigQueryInsertJobOperator(
                task_id="create_bowling_stats_temp_tables",
                configuration={
                    "query": {
                        "query": bowling_temp_sql,
                        "useLegacySql": False
                    }
                },
                params={
                     "project_id":PROJECT_ID,
                     "src_dataset_name":BQ_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )
    
    #create necessary temp table to generate actual stats reports created in create_batting_stats task
    batting_temp_sql=read_from_gcs('sql/bq_batting_stats_temp_tables.sql')
    create_batting_stats_temp_tables=BigQueryInsertJobOperator(
                task_id="create_batting_stats_temp_tables",
                configuration={
                    "query": {
                        "query": batting_temp_sql,
                        "useLegacySql": False
                    }
                },
                params={
                     "project_id":PROJECT_ID,
                     "src_dataset_name":BQ_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )
    
    #target bowling_stats reports in bq
    read_bowling_stats_sql=read_from_gcs(file_name='sql/bowling_stats.sql')
    bowling_stats_sql = [q.strip() for q in read_bowling_stats_sql.split(';') if q.strip()]
    with TaskGroup(group_id='create_bowling_stats') as create_bowling_stats:
        prev_task=None
        for idx, query in enumerate(bowling_stats_sql):
                pattern = r'`(?:[^`]*\.)?([^.`]+)`'
                match = re.search(pattern,query)
                table_name = match.group(1)
                current_task=BigQueryInsertJobOperator(
                task_id=f"create_{table_name}_stats",
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,

                    }
                },
                params={
                     "project_id":PROJECT_ID,
                     "src_dataset_name":BQ_DATASET_NM,
                     "tgt_dataset_name": BOWLING_STATS_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )

                if prev_task:
                    prev_task >> current_task
                prev_task=current_task

    #target batting_stats reports in bq     
    read_batting_stats_sql=read_from_gcs(file_name='sql/batting_stats.sql')
    batting_stats_sql = [q.strip() for q in read_batting_stats_sql.split(';') if q.strip()]
    with TaskGroup(group_id='create_batting_stats') as create_batting_stats:
        prev_task=None
        for idx, query in enumerate(batting_stats_sql):
                pattern = r'`(?:[^`]*\.)?([^.`]+)`'
                match = re.search(pattern,query)
                table_name = match.group(1)
                current_task=BigQueryInsertJobOperator(
                task_id=f"create_{table_name}_stats",
                configuration={
                    "query": {
                        "query": query,
                        "useLegacySql": False,

                    }
                },
                params={
                     "project_id":PROJECT_ID,
                     "src_dataset_name":BQ_DATASET_NM,
                     "tgt_dataset_name": BATTING_STATS_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )

                if prev_task:
                    prev_task >> current_task
                prev_task=current_task

    #drop temp tables 
    drop_temp_table_sql=read_from_gcs('sql/cleanup_temp_tables.sql')
    cleanup_temp_tables=BigQueryInsertJobOperator(
                task_id="cleanup_temp_tables",
                configuration={
                    "query": {
                        "query": drop_temp_table_sql,
                        "useLegacySql": False
                    }
                },
                params={
                     "project_id": PROJECT_ID,
                     "dataset_name": BQ_DATASET_NM,
                },
                location=LOCATION,
                gcp_conn_id=GCP_CONN_ID,
                project_id=PROJECT_ID,
            )
         

extract_and_convert_files >> create_bq_tables >> load_from_gcs_to_bq_table >> create_bowling_stats_temp_tables >> create_batting_stats_temp_tables >> create_bowling_stats >> create_batting_stats >> cleanup_temp_tables
