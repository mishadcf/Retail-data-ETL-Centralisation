import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import json
import boto3
import os
import config


class DataExtractor:
    def __init__(self, db_connector=None):
        if db_connector is None:
            db_connector = DatabaseConnector()
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """_summary_

        Args:
            table_name (_type_): _description_

        Raises:
            ValueError: _description_

        Returns:
            DataFrame: _description_
        """
        # if table_name not in self.list_tables():
        #     raise ValueError(f"{table_name} is not found in the database")

        engine = self.db_connector.init_db_engine()
        return pd.read_sql_table(table_name, engine)

    @staticmethod
    def retrieve_pdf_data(URL):
        """Uses tabula-py to read a PDF document's table into a DataFrame

        Args:
            URL (str): location of PDF

        Returns:
            pd.DataFrame
        """
        all_pages = tabula.read_pdf(URL, pages="all")
        df_pdf = pd.concat(all_pages, ignore_index=True, join="inner")
        return df_pdf

    @staticmethod
    def list_number_of_stores(n_stores_API_endpoint, headers):
        r = requests.get(n_stores_API_endpoint, headers=headers)
        if r.status_code == 200:
            return json.loads(r.text)["number_stores"]
        else:
            return None  # or raise an exception

    @staticmethod
    def retrieve_stores_data(
        headers=None,
        base_URL="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}",
        n_stores_API_endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
    ):
        if headers == None:
            api_key = os.environ.get("API_KEY")
            if api_key == None:
                raise Exception(
                    "Please set the API_KEY environment variable before running this script."
                )
            headers = {"x-api-key": api_key}
            print(headers)
        n = DataExtractor.list_number_of_stores(n_stores_API_endpoint, headers)
        if n is None:
            return None  # or raise an exception

        # base_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        endpoints_list = [base_URL.format(store_number=i) for i in range(n)]

        response_list = []
        for URL_string in endpoints_list:
            r = requests.get(URL_string, headers=headers)
            if r.status_code == 200:
                response_list.append(r.json())

        df_stores_info = pd.DataFrame(response_list)
        return df_stores_info

    @staticmethod
    def extract_from_s3(
        bucket_name="data-handling-public",
        file_key="products.csv",
        local_file_path="./products.csv",
    ):
        s3 = boto3.client("s3")
        df_products = pd.read_csv(local_file_path)
        return df_products

    import boto3

    @staticmethod
    def extract_from_s32(
        bucket_name="data-handling-public",
        file_key="products.csv",
        local_file_path="./products.csv",
    ):
        s3 = boto3.client("s3")

        # Download the file from S3 to a local directory
        try:
            s3.download_file(bucket_name, file_key, local_file_path)
        except Exception as e:
            print(f"An error occurred while downloading the file from S3: {e}")
            return None

        # Read the file into a DataFrame
        try:
            df_products = pd.read_csv(local_file_path)
            return df_products
        except FileNotFoundError as e:
            print(f"File not found: {e}")
            return None


# TODO: fill docstrings
