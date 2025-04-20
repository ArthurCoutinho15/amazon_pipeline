from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, when
import json

class Spark():
    
    
    @staticmethod
    def sparkSession():
        spark = SparkSession\
            .builder\
            .appName("amazonPipeline")\
            .config("spark.jars", "mysql-connector-java-8.0.28.jar")\
            .getOrCreate()
        return spark
    
    def transform_data_into_json_lines(self, path):
        with open(path, "r") as f:
            raw_data = json.load(f)
        
        data = raw_data["data"]["products"]

    def save_json_lines(self, data, path):
        with open(path, "w") as f:
            for d in data:
                f.write(json.dumsps(d) + "\n")

    def transform_data(self,spark, country, currency_symbol, path):
        df = spark.read_json(path)
        df = df.withColumn("product_price", regexp_replace(col("product_price"), f"[{currency_symbol} ,]", "").cast("float"))\
        .drop("is_best_seller", "is_amazon_choice", "is_prime", "product_badge", "climate_pledge_friendly")
        