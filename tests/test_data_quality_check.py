import logging
import pytest
import json
import tempfile
import os
from scripts.data_quality_ckeck import DataQualityChecker


class TestDataQualityChecker:
    @pytest.fixture
    def temp_json_file(self):
        # Create a temporary JSON file for testing
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as temp_file:
            json_objects = [
                {"_id": {"$oid": "1"}, "value": 10},
                {"_id": {"$oid": "2"}, "value": -5},
                {"_id": {"$oid": "1"}, "value": 20},  # Duplicate ID
                {
                    "_id": {"$oid": "3"},
                    "value": 0,
                    "createDate": {"$date": 1611955498184},
                },  # 2021-01-29 15:24:58.184000
            ]
            for obj in json_objects:
                json.dump(obj, temp_file)
                temp_file.write("\n")
        yield temp_file.name
        os.unlink(temp_file.name)

    @pytest.fixture
    def data_quality_checker(self, temp_json_file):
        checker = DataQualityChecker(temp_json_file)
        checker.load_data()
        return checker

    def test_load_data(self, data_quality_checker):
        assert len(data_quality_checker.data) == 4

    def test_find_duplicates(self, data_quality_checker):
        duplicates = data_quality_checker.find_duplicates(
            [item["_id"] for item in data_quality_checker.data]
        )
        assert duplicates == ["1"]

    def test_check_negative_values(self, data_quality_checker):
        negative_values = data_quality_checker.check_negative_values(
            data_quality_checker.data[1]
        )
        assert negative_values == {"value": -5}

    def test_check_json_quality(self, data_quality_checker, caplog):
        caplog.set_level(logging.INFO)
        data_quality_checker.check_json_quality()
        assert "Data quality check completed" in caplog.text

    def test_check_schema_consistency(
        self, data_quality_checker, caplog, temp_json_file
    ):
        expected_keys = set(data_quality_checker.data[0].keys())
        tmp_file_name = os.path.splitext(os.path.basename(temp_json_file))[0]
        data_quality_checker.check_schema_consistency(
            {"_id": {"$oid": "4"}, "extra_key": "value"}, 0, expected_keys
        )
        assert f"Inconsistent schema in {tmp_file_name} at line number 0" in caplog.text

    def test_check_date_format(self, data_quality_checker, caplog):
        caplog.set_level(logging.INFO)
        item_with_date = data_quality_checker.data[3]
        data_quality_checker.check_date_format(item_with_date, 3)
        assert "createDate: Valid date - 2021-01-29 15:24:58.184000" in caplog.text

    def test_check_negative_numeric_fields(
        self, data_quality_checker, caplog, temp_json_file
    ):
        data_quality_checker.check_negative_numeric_fields("price", -10, 1)
        tmp_file_name = os.path.splitext(os.path.basename(temp_json_file))[0]
        assert (
            f"Negative value for price found in {tmp_file_name} at line number  1"
            in caplog.text
        )

    def test_check_id_uniqueness(self, data_quality_checker, caplog):
        expected_keys = set(data_quality_checker.data[0].keys())
        data_quality_checker.check_id_uniqueness(expected_keys)
        assert "Duplicate OIDs found" in caplog.text

    def test_invalid_json_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as temp_file:
            temp_file.write("Invalid JSON")

        checker = DataQualityChecker(temp_file.name)
        with pytest.raises(json.JSONDecodeError):
            checker.load_data()

        os.unlink(temp_file.name)

    def test_file_not_found(self):
        checker = DataQualityChecker("non_existent_file.json")
        with pytest.raises(FileNotFoundError):
            checker.load_data()
