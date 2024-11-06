import os
from scripts.data_quality_ckeck import DataQualityChecker, logger


def main(file_path: str) -> None:
    """The main function to execute the DataQualityChecker

    Args:
        file_path (str): The path to the JSON file
    """
    checker = DataQualityChecker(file_path)
    checker.load_data()
    checker.check_json_quality()


if __name__ == "__main__":
    logger.info("Starting data quality check...")
    # logs created in DataQualityChecker.log
    paths = ["data/brands.json", "data/users.json", "data/receipts.json"]
    for file_path in paths:
        base_name = os.path.basename(file_path)
        file_name_without_extension = os.path.splitext(base_name)[0]
        logger.info(f"Checking {file_name_without_extension} data quality...")
        main(file_path)
