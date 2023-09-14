import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import json
import boto3


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
    def list_number_of_stores(self, n_stores_API_endpoint, headers):
        r = requests.get(n_stores_API_endpoint, headers=headers)
        return json.loads(r.text)["number_stores"]  # number of stores

    @staticmethod
    def retrieve_stores_data(stores_info_endpoint, headers):
        n = e.list_number_of_stores(n_stores_API_endpoint, headers)
        base_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        endpoints_list = [
            f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
            for store_number in range(n)
        ]
        response_list = [
            requests.get(URL_string, headers=headers).text
            for URL_string in endpoints_list
        ]
        response_list = [json.loads(response) for response in response_list]
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


# TODO: fill docstrings
