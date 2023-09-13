import data_cleaning
import data_extraction
import database_utils
import argparse
import transformations


# initialise 3 classes : 6 functions - targeting a specific table: for each, connect to DB(create DBconnector)
# for each DF: call in sequence, which functions to call  - different sequences
# DatabaseConnector, DataExtractor, DataCleaning for each table (initialised)
# TODO : main.py sequence to call relevant functions for table name inputs, call upload_to_db

######EXTRACTION#######
######TRANSFORMATION#######
######LOADING#######


def main():
    # Argument parser to handle CLI arguments

    parser = argparse.ArgumentParser(description="ETL Data Centralisation Tool")

    parser.add_argument(
        "table_name",
        type=str,
        help="The name of the table to extract, clean and centralise into a local postgres database",
    )

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
        data = data_extraction.DataExtractor.read_rds_table("legacy_user_data")
        cleaned_data = data_cleaning.DataCleaning.clean_user_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_users_table", cleaned_data)

    if args.type == "card":
        data = data_extraction.DataExtractor.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_cards", cleaned_data)

    if args.type == "store":
        data = data_extraction.DataExtractor.retrieve_stores_data()
        cleaned_data = data_cleaning.DataCleaning.clean_user_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_users_table", cleaned_data)

    if args.type == "product":
        data = data_extraction.DataExtractor.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_cards", cleaned_data)

    if args.type == "order":
        data = data_extraction.DataExtractor.read_rds_table("legacy_user_data")
        cleaned_data = data_cleaning.DataCleaning.clean_user_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_users_table", cleaned_data)

    if args.type == "date_event":
        data = data_extraction.DataExtractor.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_cards", cleaned_data)

    # first write logic for user_data ETL (structure well - for exteneding the same format to other ETL for different tables)


if __name__ == "__main__":
    main()
