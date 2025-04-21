from extract import Extract
from mongo import Mongo
from spark import Spark
import json
import os
from dotenv import load_dotenv
load_dotenv()

def extract_data_for_country(endpoint, query, country, url, pages=4, file_name=""):
    extractor = Extract(url=url, country=country)
    
    for i in range(1, pages + 1):
        name = f"{file_name}_{i}"
        query_string = extractor.querystring(endpoint=endpoint,query=query, page=i)
        response = extractor.extract_data(querystring=query_string)
        path = extractor.save_json(response=response, name=name)

    return path


def load_data_into_mongo(uri, db_name, coll_name,path):
    mongo = Mongo(uri=uri, db_name=db_name, coll_name=coll_name)
    mongo.connect_mongo()
    database = mongo.connect_db()
    collection = mongo.connect_collection()
    
    with open(path, 'r') as f:
        data = json.load(f)
    
    data = data['data']['products']
    
    result = mongo.load(data)
    print(result)

def transform_data(path, country, currency_symbol):
    spark = Spark()
    spark_session = spark.sparkSession()
    data = spark.transform_data_into_json_lines(path)
    new_path = spark.save_json_lines(data, path)
    
    df = spark.transform_data(spark_session, country=country, currency_symbol=currency_symbol, path=new_path)
    print("Dados transformados com sucesso!")
    print(df.show(5))
    return df




if __name__ == "__main__":
    url_products_by_category = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"
    url_products = "https://real-time-amazon-data.p.rapidapi.com/search"
    
    for country in ["US", "BR"]:
        data = extract_data_for_country(endpoint="category",query="468642", country=country, url=url_products_by_category, file_name="dados_pagina")
        load_data_into_mongo(uri=str(os.getenv('uri')), db_name="amazon_pipeline", coll_name="Amazon_Products_by_category", data=["data"]["products"])
    
    for country in ["US", "BR"]:
        path = extract_data_for_country(endpoint="products", query="Iphone", country=country, url=url_products, pages=1, file_name="produtos")
        load_data_into_mongo(uri=str(os.getenv('uri')), db_name="amazon_pipeline", coll_name="amazon_products", path=path)
    
    path = "/home/arthur/Projetos/amazon_pipeline/data/produtos_1_BR_copy.json"
    df = transform_data(path, "BR", "R$")