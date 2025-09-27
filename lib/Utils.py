from pyspark.sql import SparkSession
from lib.configreader import get_pyspark_config
def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .config("spark.driver.memory", "2g")\
        .config("spark.executor.memory", "4g")\
        .enableHiveSupport() \
        .master("local[*]") \
        .getOrCreate()
    else:
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()
    

