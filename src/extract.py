import requests
import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

load_dotenv()

def extract_products_by_category(url, headers, querystring):
    request = requests.get(url, headers=headers, params=querystring)
    response = request.json()
    
    return response	

def save_json(response):
    with open('/home/arthur/Projetos/amazon_pipeline/data/dados.json', 'w') as f:
        json.dump(response, f, indent=4)



# spark = SparkSession\
#         .builder\
#         .appName("amazonPipeline")\
#         .config("spark.jars", "mysql-connector-java-8.0.28.jar")\
#         .getOrCreate()


if __name__ == "__main__":
    
    headers = {
	"x-rapidapi-key": str(os.getenv('x-rapidapi-key')),
	"x-rapidapi-host": str(os.getenv('x-rapidapi-host'))
    }
    
    url_products_by_category = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"
    querystring_products_by_category = {"category_id":"2478868012","page":"2","country":"US","sort_by":"RELEVANCE",\
        "product_condition":"ALL","is_prime":"false","deals_and_discounts":"NONE"}
    
    response = extract_products_by_category(url_products_by_category,headers,querystring_products_by_category)
    save_json(response)

