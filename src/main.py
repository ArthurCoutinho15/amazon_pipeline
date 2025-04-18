from extract import Extract

def extract_data_for_country(country, url, pages=4):
    extractor = Extract(url=url, country=country)
    
    for i in range(1, pages + 1):
        name = f"dados_pagina_{i}"
        query_string = extractor.querystring(i)
        response = extractor.extract_products_by_category(querystring=query_string)
        extractor.save_json(response=response, name=name)


if __name__ == "__main__":
    url_products_by_category = "https://real-time-amazon-data.p.rapidapi.com/products-by-category"


    for country in ["US", "BR"]:
        extract_data_for_country(country=country, url=url_products_by_category)
       