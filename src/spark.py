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

        return data

    def save_json_lines(self, data, path):
        with open(path, "w") as f:
            for d in data:
                f.write(json.dumps(d) + "\n")
        
        return path

    def transform_data(self, spark, country, currency_symbol, path):
        """Realiza transformações nos dados usando PySpark."""
        df = spark.read.json(path)

        drop_cols = ["is_best_seller", "is_amazon_choice", "is_prime",
                     "product_badge", "climate_pledge_friendly", "product_availability", "delivery", "has_variations",
                     "coupon_text", "book_format"]

        df = df.withColumn("product_price", when(col("product_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_original_price", when(col("product_original_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_original_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_minimum_offer_price", when(col("product_minimum_offer_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_minimum_offer_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_star_rating", col("product_star_rating").cast("float")) \
               .withColumn("product_num_offers", col("product_num_offers").cast("float")) \
               .withColumn("product_num_ratings", col("product_num_ratings").cast("float")) \
               .withColumn("country", lit(country))

        if country == "BR":
            df = df.filter(col("product_title").startswith("Apple iPhone"))

        df = df.drop(*[c for c in drop_cols if c in df.columns])
        return df
        
    def join_df(self, df1, df2):
        """Une dois DataFrames com as mesmas colunas."""
        return df1.unionByName(df2)

    def write_to_mysql(self, df, table_name, user, password, host="localhost", port=3306, database="meu_banco"):
        """Salva o DataFrame no banco de dados MySQL."""
        url = f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC"

        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append")\
            .save()