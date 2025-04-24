import pytest
from src.extract import Extract
import json

@pytest.fixture
def extractor():
    return Extract(url="http://teste.com", country="BR")

class TesteClass():
    def test_querystring(self, extractor):
        querystring = extractor.querystring(endpoint="products", query="Iphone", page="2")
        
        assert querystring["query"] == "Iphone"
        assert querystring["country"] == "BR"
        assert querystring["page"] == "2"
    
    def test_save_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(Extract, "save_json", Extract.save_json)
        extract = Extract("url", "BR")
        data = {"Test":"test"}
        
        path = tmp_path / "saida_test_br.json"
        returned = extract.save_json(data, "saida_Test")
        
        with open(returned) as f:
            assert json.load(f) == data