from pyspark.sql import SparkSession

def create_get_spark_session(app_name="ipl_data_analytic_etl"):
    spark = SparkSession.builder.appName("ipl_data_analytic_etl").getOrCreate()
    return spark
