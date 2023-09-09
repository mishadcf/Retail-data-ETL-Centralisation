import pandas as pd
import transformations


class DataCleaning:
    """Base class for tables to clean"""

    def clean_user_data(df_user):
        """Cleans the user DataFrame"""

        if "index" in df_user.columns:
            df_user = df_user.drop("index", axis=1)
        df_user = transformations.clean_upper_or_numeric_rows(df_user)
        df_user["date_of_birth"] = pd.to_datetime(df_user["date_of_birth"])
        df_user["join_date"] = pd.to_datetime(df_user["join_date"])
        df_user["address"] = transformations.remove_newline_character(
            df_user["address"]
        )
        df_user = transformations.email_address_cleaner(df_user)
        df_user = transformations.clean_country_code_ggb(df_user)
        df_user["country_code"] = df_user["country_code"].astype("category")
        df_user["country"] = df_user["country"].astype("category")

        return df_user

    def clean_card_data(df):
        """Cleans the card data DataFrame"""

        def drop_rows_with_invalid_card_numbers(df):
            return df[~df["card_number"].astype(str).str.contains("\?", regex=True)]

        df = drop_rows_with_invalid_card_numbers(df)
        df = transformations.clean_upper_or_numeric_rows(df)
        df.date_payment_confirmed = df.date_payment_confirmed.astype("datetime64[as]")
        df.card_provider = df.card_provider.astype("str")
        df["expiry_date"] = pd.to_datetime(df["expiry_date"], format="%m/%y")

        return df

    def clean_store_data(df):
        """Cleans the store data DataFrame"""
        df = df.drop(0)  # 'N/A' entries - had to add in (PG detected)
        df = df.drop("lat", axis=1)
        df = df.drop("index", axis=1)
        df.continent = df.continent.str.replace("ee", "")
        df = transformations.clean_upper_or_numeric_rows(df)
        df.address = transformations.remove_newline_character(df.address)
        df.staff_numbers = df.staff_numbers.str.replace(r"\D", "", regex=True)
        df = transformations.clean_user_data_rows_all_NULL(df)

        df.locality = df.locality.astype("category")
        df.store_type = df.store_type.astype("category")
        df.country_code = df.country_code.astype("category")
        df.continent = df.continent.astype("category")
        df.staff_numbers = pd.to_numeric(df.staff_numbers)

        return df

    def clean_product_data(df_products):
        """Cleans the product data DataFrame"""

        df_products = transformations.clean_upper_or_numeric_rows(df_products)
        df_products.drop("Unnamed: 0", inplace=True, axis=1)
        df_products.dropna(inplace=True)
        df_products.rename(columns={"weight": "weight(KG)"}, inplace=True)
        return df_products

    def convert_product_weights(df_products):
        def clean_g(gram_string):
            return (
                float(gram_string.replace("g", "")) / 1000
            )  # logic to select strings with g but not x!

        def clean_x_g(x_g_string):
            """takes strings of the form : '5x60g' and returns 0.3 for 0.3kg"""

            x_g_string = x_g_string.strip("g")
            x_g_string = x_g_string.split("x")
            result = float(x_g_string[0]) * float(x_g_string[1])
            return result / 1000

        def clean_ml(ml_string):
            return float(ml_string.replace("ml", "")) / 1000

        def clean_kg(kg_string):
            return float(
                kg_string.replace("kg", "")
            )  # comes before clean_g: so as not to leave just k, for example.

        def clean_oz(oz_string):
            return float(oz_string.replace("oz", "")) * 0.0283495

        def clean_weight_entry(entry):
            entry = str(entry).lower()
            if "x" in entry and "g" in entry:
                return clean_x_g(entry)
            elif "kg" in entry:
                return clean_kg(entry)
            elif "g" in entry:
                return clean_g(entry)
            elif "ml" in entry:
                return clean_ml(entry)
            elif "oz" in entry:
                return clean_oz(entry)
            else:
                return None  # Return None if the pattern doesn't match any of the known patterns

        df_products["weight(KG)"] = transformations.remove_newline_character(
            df_products["weight(KG)"]
        )
        df_products["weight(KG)"] = df_products["weight(KG)"].str.strip(".")
        df_products["weight(KG)"] = df_products["weight(KG)"].apply(clean_weight_entry)

        # TODO : Join convert_product_weights with clean_product_data in 1 function?

    def clean_orders_data(df):
        """Cleans orders DataFrame"""
        df.drop(columns={"level_0", "index", "1"}, inplace=True)
        df.dropna(axis=0, subset=["first_name", "last_name"], inplace=True)
        df.dropna(axis=0, subset=["card_number"], inplace=True)
        df.reset_index(inplace=True)
        df.drop("index", axis=1, inplace=True)

        return df

    def clean_date_events(df):
        """Cleans date evetns DataFrame"""
        df = transformations.clean_upper_or_numeric_rows(df)
        df["month"] = df["month"].astype("int")
        df["year"] = df["year"].astype("int")
        df["day"] = df["day"].astype("int")

        #  date-time components string
        df["datetime_str"] = (
            df["year"].astype(str)
            + "-"
            + df["month"].astype(str)
            + "-"
            + df["day"].astype(str)
            + " "
            + df["timestamp"]
        )

        df["datetime"] = pd.to_datetime(df["datetime_str"])

        df.drop("datetime_str", axis=1, inplace=True)
        return df


# TODO : need consistency of naming  + docstrings
