import os
from dotenv import load_dotenv
import requests
import json
load_dotenv()

class Extract():
    def __init__(self, url, country):
        self.headers = {
	            "x-rapidapi-key": str(os.getenv('x-rapidapi-key')),
	            "x-rapidapi-host": str(os.getenv('x-rapidapi-host'))
            }
        self.url = url
        self.country = country
    
    def querystring(self,endpoint,query, page):
        querystring_category = {"category_id":f"{query}", #468642 id videogame
                       "page":f"{page}",
                       "country":f"{self.country}",
                       "sort_by":"RELEVANCE",
                       "product_condition":"ALL",
                       "is_prime":"false",
                       "deals_and_discounts":"NONE"}
        
        querystring_products = {"query":f"{query}",
                       "page":f"{page}",
                       "country": f"{self.country}",
                       "sort_by":"RELEVANCE",
                       "product_condition":"ALL",
                       "is_prime":"false",
                       "deals_and_discounts":"NONE"}

        if endpoint == "products":
            return querystring_products
        elif endpoint == "category":
            return querystring_category
    
    
    def extract_data(self, querystring):
        try:
            response = requests.get(self.url, headers=self.headers, params=querystring)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Erro ao fazer requisição: {e}")
            return {}
    
    
    def save_json(self, response, name):
        path = f'/home/arthur/Projetos/amazon_pipeline/data/{name}_{self.country}.json'
        with open(path, 'w') as f:
            json.dump(response, f, indent=4)
        
        print("Dados salvos com sucesso!")
        return path
    
    
    
            
    