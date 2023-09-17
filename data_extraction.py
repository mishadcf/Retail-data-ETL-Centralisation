import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import json
import boto3
import os
import config


class DataExtractor:
    """Class to handle various types of data extraction methods."""

    def __init__(self, db_connector=None):
        """Initialize DataExtractor with a DatabaseConnector instance.

        Args:
            db_connector (DatabaseConnector, optional): Instance of DatabaseConnector. Defaults to None.
        """
        if db_connector is None:
            db_connector = DatabaseConnector()
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """
        Reads a table from RDS into a Pandas DataFrame.

        Args:
            table_name (str): Name of the table to read from RDS.

        Raises:
            ValueError: If the table cannot be read.

        Returns:
            pd.DataFrame: DataFrame containing table data.
        """

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
        """
        Gets the number of stores from an API endpoint.

        Args:
            n_stores_API_endpoint (str): API endpoint to get number of stores.
            headers (dict): HTTP headers for the request.

        Returns:
            int or None: Number of stores or None if the request is unsuccessful.
        """
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
        """
        Retrieve details of all stores from an API.

        Args:
            headers (dict, optional): HTTP headers for the request. Defaults to None.
            base_URL (str, optional): Base URL for store details API. Defaults to a preset URL.
            n_stores_API_endpoint (str, optional): API endpoint to get number of stores. Defaults to a preset URL.

        Returns:
            pd.DataFrame or None: DataFrame containing store details or None if unsuccessful.
        """
        if headers == None:
            api_key = os.environ.get("API_KEY")
            if api_key == None:
                raise Exception(
                    "Please set the API_KEY environment variable before running this script."
                )
            headers = {"x-api-key": api_key}

        n = DataExtractor.list_number_of_stores(n_stores_API_endpoint, headers)
        if n is None:
            return None  # or raise an exception

        endpoints_list = [base_URL.format(store_number=i) for i in range(n)]

        response_list = []
        for URL_string in endpoints_list:
            r = requests.get(URL_string, headers=headers)
            if r.status_code == 200:
                response_list.append(r.json())

        df_stores_info = pd.DataFrame(response_list)
        return df_stores_info

    @staticmethod
    def extract_from_s3():
        """
        Extracts product data from an S3 bucket.

        Returns:
            pd.DataFrame: DataFrame containing product details.
        """
        s3 = boto3.client("s3")
        bucket_name = "data-handling-public"
        file_key = "products.csv"
        local_file_path = "./products.csv"
        s3.download_file(bucket_name, file_key, local_file_path)
        df_products = pd.read_csv(local_file_path)
        return df_products

    @staticmethod
    def extract_json_from_URL(
        endpoint_URL="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
    ):
        """
        Extracts JSON data from a URL into a Pandas DataFrame.

        Args:
            endpoint_URL (str, optional): The URL where the JSON data is located. Defaults to a preset URL.

        Returns:
            pd.DataFrame: DataFrame containing the JSON data.
        """
        response = requests.get(endpoint_URL).text
        j = json.loads(response)
        df_date_events = pd.DataFrame(j)
        return df_date_events
