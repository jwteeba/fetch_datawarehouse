import json
from datetime import datetime
import logging
import os
from typing import Any, Counter, Dict, List

logging.basicConfig(
    filename="DataQualityChecker.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class JsonFileReader:
    @staticmethod
    def read_json_file(file_path):
        with open(file_path, "r") as file:
            for line in file:
                yield json.loads(line)


class DataQualityChecker:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.entity = os.path.splitext(os.path.basename(self.file_path))[0]

    def load_data(self) -> None:
        """Load json data from file"""
        try:
            self.data = list(JsonFileReader.read_json_file(self.file_path))
        except json.JSONDecodeError:
            logger.warning("Invalid JSON structure")
            raise
        except FileNotFoundError:
            logger.error(f"File not found: {self.entity}")
            raise

        if not self.data:
            logger.warning("The file is empty")

    def find_duplicates(self, data: Dict) -> List:
        """Find duplicates in JSON

        Args:
            data (Dict): Data to check for duplicates

        Returns:
            List: List of duplicates
        """
        oid_values = [item["$oid"] for item in data]
        count = Counter(oid_values)
        duplicates = [oid for oid, freq in count.items() if freq > 1]
        return duplicates

    def check_negative_values(self, data: Dict, path: str = "") -> Dict:
        """Check for negative values in JSON - Use primarily for Receipts

        Args:
            data (Dict): Data to check for negative values
            path (str, optional): Specific path from the JSON. Defaults to "".

        Returns:
            Dict: Dictionary of negative values
        """
        negative_values = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_path = f"{path}.{key}" if path else key
                if isinstance(value, (dict, list)):
                    negative_values.update(self.check_negative_values(value, new_path))
                else:
                    try:
                        num_value = float(value) if isinstance(value, str) else value
                        if isinstance(num_value, (int, float)) and num_value < 0:
                            negative_values[new_path] = num_value
                    except (ValueError, TypeError):
                        pass
        elif isinstance(data, list):
            for index, item in enumerate(data):
                new_path = f"{path}[{index}]"
                negative_values.update(self.check_negative_values(item, new_path))

        return negative_values

    def check_json_quality(self) -> None:
        """Check the quality of the JSON data"""
        if not self.data:
            return

        expected_keys = set(self.data[0].keys())

        for index, item in enumerate(self.data):
            self.check_item(item, index, expected_keys)

        self.check_id_uniqueness(expected_keys)

        logger.info("Data quality check completed")

    def check_item(self, item: Dict, index: int, expected_keys: set) -> None:
        """Check item in the JSON

        Args:
            item (Dict): Json item to check
            index (int): The index of the item in the JSON
            expected_keys (set): The set expected keys in the JSON
        """
        self.check_negative_values_in_item(item)
        self.check_schema_consistency(item, index, expected_keys)
        self.check_field_types(item, index)

    def check_negative_values_in_item(self, item: Dict) -> None:
        """Check for negative values in item

        Args:
            item (Dict): The item to check for negative values
        """
        result = self.check_negative_values(item)
        result.update(self.check_negative_values(item, path="rewardsReceiptItemList"))

        if result:
            logger.warning("Negative values found:")
            for key, value in result.items():
                logger.info(f"{key}: {value}")
        else:
            logger.info(f"No negative values found in {self.entity}.")

    def check_schema_consistency(self, item: Dict, index: int, expected_keys: set):
        """Check schema consistency

        Args:
            item (Dict): The item to check for schema consistency
            index (int): The index of the item in the JSON
            expected_keys (set): The set expected keys in the JSON
        """
        if set(item.keys()) != expected_keys:
            logger.warning(
                f"Inconsistent schema in {self.entity} at line number {index}"
            )

    def check_field_types(self, item: Dict, index: int) -> None:
        """Check field types

        Args:
            item (Dict): The item to check for field types
            index (int): The index of the item in the JSON
        """
        for key, value in item.items():
            if isinstance(value, str):
                self.check_date_format(item, index)
            elif isinstance(value, (int, float)):
                self.check_negative_numeric_fields(key, value, index)
            elif isinstance(value, list):
                self.check_empty_list(key, value, index)
            elif value is None:
                logger.warning(
                    f"Null value for {key} found in {self.entity} at line number {index}"
                )

        self.check_id_type(item, index)

    def check_date_format(self, item: Dict, index: int) -> None:
        """Check date format

        Args:
            item (Dict): The json item to check for date format in
            index (int): The index of the item in the JSON
        """

        # The fields order matter - DO NOT CHANGE!
        date_fields = [
            "lastLogin",
            "createDate",
            "dateScanned",
            "finishedDate",
            "modifyDate",
            "pointsAwardedDate",
            "purchaseDate",
        ]

        for field in date_fields:
            if field in item:
                if isinstance(item[field], dict) and "$date" in item[field]:
                    timestamp = item[field]["$date"]
                    try:
                        # Convert milliseconds to seconds
                        date = datetime.fromtimestamp(timestamp / 1000)
                        logger.info(f"{field}: Valid date - {date} in {self.entity}")
                    except (ValueError, TypeError, OverflowError):
                        logger.warning(f"{field}: Invalid date format in {self.entity}")
                else:
                    logger.warning(
                        f"{field}: Incorrect date format found in {self.entity} at line number {index} (expected {{'$date': timestamp}})"
                    )
            else:
                # Only Receipts and Users has date fields
                if self.entity == "receipts" and field in date_fields[1:]:
                    logger.warning(
                        f"Missing date field '{field}' in receipts data at line number {index}"
                    )
                elif self.entity == "users" and field in date_fields[:1]:
                    logger.warning(
                        f"Missing date field '{field}' in users data at line number {index}"
                    )

    def check_negative_numeric_fields(self, key: str, value: Any, index: int) -> None:
        """Check negative numeric fields

        Args:
            key (str): The key of the item to check for negative numeric fields
            value (Any): The value of the item to check for negative numeric fields
            index (int): The index of the item in the JSON
        """
        if (
            any(
                indicator in key.lower()
                for indicator in ["count", "amount", "price", "quantity"]
            )
            and value < 0
        ):
            logger.warning(
                f"Negative value for {key} found in {self.entity} at line number  {index}"
            )

    def check_empty_list(self, key: str, value: Any, index: int) -> None:
        """Check empty list

        Args:
            key (str): The key of the item to check for empty list
            value (Any): The value of the item to check for empty list
            index (int): The index of the item in the JSON
        """
        if not value:
            logger.warning(
                f"Empty list for {key} in {self.entity} at line number {index}"
            )

    def check_id_type(self, item: Dict, index: int) -> None:
        """Check id type

        Args:
            item (Dict): The item to check for id type
            index (int): The index of the item in the JSON
        """
        if "_id" in item:
            if not isinstance(item["_id"], dict) or "$oid" not in item["_id"]:
                logger.warning(
                    f"Invalid _id type found in {self.entity} at line number {index}"
                )
            elif not isinstance(item["_id"]["$oid"], str):
                logger.warning(
                    f"Invalid $oid type in _id in {self.entity} at line number {index}"
                )

    def check_id_uniqueness(self, expected_keys: set) -> None:
        """Check id uniqueness

        Args:
            expected_keys (set): The set expected keys in the JSON
        """
        if "_id" in expected_keys:
            ids = [item["_id"] for item in self.data if "_id" in item]
            duplicate_oids = self.find_duplicates(ids)
            if duplicate_oids:
                logger.warning(f"Duplicate OIDs found in {self.entity}")
                for oid in duplicate_oids:
                    logger.info(oid)
            else:
                logger.info(f"No duplicates found in {self.entity}.")
