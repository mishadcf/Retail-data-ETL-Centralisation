import data_cleaning
import data_extraction
import database_utils
import argparse
import os
import config
import requests
import pandas as pd
import boto3

"""
Main script to execute ETL (Extract, Transform, Load) tasks for different types of data.
"""

api_key = os.environ.get("API_KEY")
if api_key is None:
    input("enter your S3")

headers = {"x-api-key": api_key}


def main():
    """
    Main function for executing ETL tasks based on user input.

    Uses argparse to allow the user to specify the type of data they want to clean.
    Fetches data using various extraction methods, cleans it, and then uploads it to a database.
    """
    parser = argparse.ArgumentParser(description="ETL Data Centralisation Tool")

    parser.add_argument(
        "type",
        type=str,
        choices=["user", "card", "store", "product", "order", "date_event"],
        help="""The type of data to clean:
                                - user: Cleans user data by removing duplicates, formatting dates, etc.
                                - card: Cleans card data, validates card numbers, etc.
                                - store: Cleans store data, removes redundant columns, etc.
                                - product: Cleans product data, standardizes weight units, etc.
                                - order: Cleans order data, removes incomplete rows, etc.
                                - date_event: Cleans date event data, combines date and time fields, etc.""",
    )

    args = parser.parse_args()

    if args.type == "user":
        de = data_extraction.DataExtractor()
        data = de.read_rds_table("legacy_users")
        cleaned_data = data_cleaning.DataCleaning.clean_user_data(data)

        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("dim_users_table", cleaned_data)

    if args.type == "card":
        de = data_extraction.DataExtractor()
        data = de.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("dim_cards", cleaned_data)

    if args.type == "store":
        data = data_extraction.DataExtractor.retrieve_stores_data()
        cleaned_data = data_cleaning.DataCleaning.clean_store_data(data)
        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("dim_stores", cleaned_data)

    if args.type == "product":
        data = data_extraction.DataExtractor.extract_from_s33()
        cleaned_data = data_cleaning.DataCleaning.clean_product_data(data)
        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("dim_productss", cleaned_data)

    if args.type == "order":
        de = data_extraction.DataExtractor()
        data = de.read_rds_table("orders_table")
        cleaned_data = data_cleaning.DataCleaning.clean_orders_data(data)
        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("orders_table", cleaned_data)

    if args.type == "date_event":
        data = data_extraction.DataExtractor.extract_json_from_URL()
        cleaned_data = data_cleaning.DataCleaning.clean_date_events(data)
        dc = database_utils.DatabaseConnector()
        dc.upload_to_db("dim_date_times", cleaned_data)


if __name__ == "__main__":
    """
    Executes the main function if the script is run as the main module.
    """
    main()
