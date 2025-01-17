from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

def get_pyspark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()

#calling function 1