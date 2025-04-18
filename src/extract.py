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
    
    def querystring(self, page):
        querystring = {"category_id":"468642",
                       "page":f"{page}",
                       "country":f"{self.country}",
                       "sort_by":"RELEVANCE",
                       "product_condition":"ALL",
                       "is_prime":"false",
                       "deals_and_discounts":"NONE"}
        
        return querystring

    def extract_products_by_category(self, querystring):
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
        return path
    
    
    
            
    