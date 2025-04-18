from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class Spark():
    spark = SparkSession\
         .builder\
         .appName("amazonPipeline")\
         .config("spark.jars", "mysql-connector-java-8.0.28.jar")\
         .getOrCreate()
