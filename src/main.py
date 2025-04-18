from extract import Extract
from mongo import Mongo
import json
import os
from dotenv import load_dotenv
load_dotenv()

def extract_data_for_country(country, url, pages=4):
    extractor = Extract(url=url, country=country)
    
    for i in range(1, pages + 1):
        name = f"dados_pagina_{i}"
        query_string = extractor.querystring(i)
        response = extractor.extract_products_by_category(querystring=query_string)
        data = extractor.save_json(response=response, name=name)

    return data

def load_data_into_mongo(uri, db_name, coll_name,data):
    mongo = Mongo(uri=uri, db_name=db_name, coll_name=coll_name)
    mongo.connect_mongo()
    database = mongo.connect_db()
    collection = mongo.connect_collection()
    
    mongo.load(data)
    
    


if __name__ == "__main__":
    url_products_by_category = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"
    
    
    for country in ["US", "BR"]:
        data = extract_data_for_country(country=country, url=url_products_by_category)
        load_data_into_mongo(uri=str(os.getenv('uri')), db_name="amazon_pipeline", coll_name="Amazon_Products", data=["data"]["products"])
    