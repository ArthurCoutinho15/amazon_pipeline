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
    
def sparkSession():
    spark = Spark()
    spark_session = spark.sparkSession()
    return spark

def transform_data(path, country, currency_symbol):
    spark = Spark()
    spark_session = spark.sparkSession()
    data = spark.transform_data_into_json_lines(path)
    new_path = spark.save_json_lines(data, path)
    
    df = spark.transform_data(spark_session, country, currency_symbol, path=new_path)
    print("Dados transformados com sucesso!")
    print(df.show(5))
    return df

def load_data_into_mysql(df):
    spark = Spark()
    spark.write_to_mysql(df)
    

if __name__ == "__main__":
    
    url_products = "https://real-time-amazon-data.p.rapidapi.com/search"
    
    dataframes = []
    currency = {
        "US": "$",
        "BR": "R$",
        "CA": "$"
    }
    
    for country in ["US", "BR", "CA"]:
        path = extract_data_for_country(endpoint="products", query="Iphone", country=country, url=url_products, pages=1, file_name="produtos")
        load_data_into_mongo(uri=str(os.getenv('uri')), db_name="amazon_pipeline", coll_name="amazon_products", path=path)
        print(path)
        currency_symbol = currency[country][0:]
        spark = sparkSession()
        df = transform_data(path, country, currency_symbol)
        dataframes.append(df)
        df_final = spark.join_df(dataframes)
        
    
    load_data_into_mysql(df_final)  
      
        # path_br = "/home/arthur/Projetos/amazon_pipeline/data/produtos_1_BR.json"
        # path_us = "/home/arthur/Projetos/amazon_pipeline/data/produtos_1_US.json"
        
        # spark = sparkSession()
        
        # df_br = transform_data(path_br, "BR", "R$")
        # df_us = transform_data(path_us, "US", "$")
            
        # df_total = spark.join_df(df_br, df_us)
        
        # load_data_into_mysql(df=df_total)
    