import json
from pyspark.sql.types import StructField, StructType, StringType,IntegerType,DateType,LongType
import gcsfs


spark_dtypes={"integer": IntegerType(),
              "string":StringType(),
              "date":DateType(),
              "long":LongType()}

def convert_json_schema_to_spark(schema_file_path) -> StructType:
    fs=gcsfs.GCSFileSystem()
    with fs.open(schema_file_path,'r') as file:
         json_schema=json.load(file)

    struct_fields = []

    for field in json_schema:
        dtype=spark_dtypes.get(field['type'])
        if not dtype:
            raise ValueError(f"Unsupported data type: {field['name']}")
        struct_fields.append(StructField(field["name"], dtype, field["nullable"]))
    
    return StructType(struct_fields)
