from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, when
import json
import os
from dotenv import load_dotenv
load_dotenv()


class Spark():

    @staticmethod
    def sparkSession():
        spark = SparkSession.builder \
            .appName("amazonPipeline") \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
            .getOrCreate()
        return spark

    def transform_data_into_json_lines(self, path):
        with open(path, "r") as f:
            raw_data = json.load(f)

        data = raw_data["data"]["products"]

        return data

    def save_json_lines(self, data, path):
        base_path = os.path.splitext(path)[0]
        new_path = f"{base_path}_lines.json"
        with open(new_path, "w") as f:
            for d in data:
                f.write(json.dumps(d) + "\n")

        return new_path

    def transform_data(self, spark, country, currency_symbol, path):
        """Realiza transformações nos dados usando PySpark."""
        df = spark.read.json(path)

        drop_cols = ["is_best_seller", "is_amazon_choice", "is_prime",
                     "product_badge", "climate_pledge_friendly", "product_availability", "delivery", "has_variations",
                     "coupon_text", "book_format", "product_byline"]

        df = df.withColumn("product_price", when(col("product_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_original_price", when(col("product_original_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_original_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_minimum_offer_price", when(col("product_minimum_offer_price").isNull(), 0)
                           .otherwise(regexp_replace(col("product_minimum_offer_price"), f"[{currency_symbol} ,]", "").cast("float"))) \
               .withColumn("product_star_rating", col("product_star_rating").cast("int")) \
               .withColumn("product_num_offers", col("product_num_offers").cast("float")) \
               .withColumn("product_num_ratings", col("product_num_ratings").cast("float")) \
               .withColumn("country", lit(country))

        if country == "BR":
            df = df.filter(col("product_title").startswith("Apple iPhone"))

        df = df.drop(*[c for c in drop_cols if c in df.columns])
        return df

    def join_df(self, dataframe_list):
        """Une DataFrames com as mesmas colunas."""
        if not dataframe_list:
            raise ValueError("Lista de dataframes vazia")
        
        final_df = dataframe_list[0]
        for df in dataframe_list[1:]:
            final_df = final_df.unionByName(df)
            
        return final_df

    def write_to_mysql(self, df):
        """Salva o DataFrame no banco de dados MySQL."""
        user = os.getenv("USER")
        password = str(os.getenv("PASS"))

        DATABASE_CONFIG = {
            "url": str(os.getenv("url")),
            "user": str(os.getenv("USER")),
            "password": str(os.getenv("PASS")),
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        print(df)

        try:
            df.write \
                .format("jdbc") \
                .option("url", DATABASE_CONFIG["url"]) \
                .option("dbtable", "produtos_amazon") \
                .option("user", DATABASE_CONFIG["user"]) \
                .option("password", DATABASE_CONFIG["password"]) \
                .option("driver", DATABASE_CONFIG["driver"]) \
                .mode("overwrite") \
                .save()
            print("✅ Dados carregados com sucesso!")
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {str(e)}")
