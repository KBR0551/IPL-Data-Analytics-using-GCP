# a config file will have datafilename , schema_file_name 

#Enforce schema on when reading from bronze layer


#read csv files and store them as parquet files
import sys
import os
#from dotenv import load_dotenv
import json
import logging
import datetime as dt
import subprocess
import argparse
from utility import spark_session, schema_loader

# Add the parent directory of utility to the path
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

#logging.basicConfig(filename=os.getenv("LOG_PATH")+'file_convert_'+dt.date.today().strftime("%Y-%m-%d")+".log",
#                    level=logging.INFO,
#                    format='%(asctime)s - %(levelname)s - %(message)s'
#                    )


parser = argparse.ArgumentParser()
parser.add_argument('--file_name', required=True)
args = parser.parse_args()

print(f"File name is: {args.file_name}")

file_name=args.file_name

log_file='/tmp/'+file_name+'-file_convert_'+dt.date.today().strftime("%Y-%m-%d")+".log"
logging.basicConfig(filename=log_file,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )

spark=spark_session.create_get_spark_session()


schema_path=os.getenv("SCHEMA_FILE_PATH")+f"{file_name}_schema.json"

print(schema_path)

schema=schema_loader.convert_json_schema_to_spark(os.getenv("SCHEMA_FILE_PATH")+f"{file_name}_schema.json")

def convert_to_parquet_file(spark,schema,file_name: str):
    try:
        logging.info("+++++++++++++ Convert job started +++++++++++++")
        df=spark.read.csv(os.getenv("SOURCE_FILE_PATH")+f"{file_name}.csv",header=True, schema=schema,mode="FAILFAST")
        df.coalesce(1).write.mode("overwrite").parquet(os.getenv("TARGET_FILE_PATH")+f"{file_name}.parquet")
        record_count=df.count()
        file_info=  {"FILE_NAME" : f"{file_name}.csv", \
                     "INPUT_FILE_PATH"  :  os.getenv("SOURCE_FILE_PATH"), \
                     "TARGET_FILE_PATH" : os.getenv("TARGET_FILE_PATH"), \
                     "RECORD_COUNT": record_count
                    }
        logging.info("Done: Converted Input file to parquet:\n" + json.dumps(file_info, indent=4))
        print("+++++++++++++ Completed +++++++++++++")
        logging.info("+++++++++++++ Completed +++++++++++++")
         
    except Exception as e:
        error_message=f"Error converting {file_name}.csv to Parquet: {str(e)}"
        logging.error(error_message)
        raise
    logging.shutdown()

if __name__=="__main__":
    convert_to_parquet_file(spark,schema,file_name)
    subprocess.run(["gsutil","cp",log_file,os.getenv("LOG_PATH")], check=True)
    print("+++++++++++++++++++++++++++++ Job Completes +++++++++++++++++++++++++")