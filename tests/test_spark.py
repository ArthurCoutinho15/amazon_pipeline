from src.spark import Spark
import json
import tempfile

class TestSpark():

    def test_transform_data_into_json_lines(self, tmp_path):
        data = {"data": {"products": [{"x":1}]}}
        file = tmp_path / "in.json"
        file.write_text(json.dumps(data))
        spark = Spark()
        products = spark.transform_data_into_json_lines(str(file))
        assert products == [{"x":1}]

    def test_save_json_lines(self, tmp_path):
        spark = Spark()
        data = [{"x":1}, {"y":2}]
        in_file = tmp_path / "in.json"
        in_file.write_text("")
        out = spark.save_json_lines(data, str(in_file))
        lines = (tmp_path / "in_lines.json").read_text().splitlines()
        assert len(lines) == 2

