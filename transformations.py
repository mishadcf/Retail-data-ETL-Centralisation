import pandas as pd

"""Generalised transformation functions (independent of the table to clean). I found similar patterns across the tables. 
"""
# unit test script for this/ error handling


def rows_with_alphabetic_dob_locator(df):
    """Locates rows where 'date_of_birth' has alphabetic characters."""
    return df[df["date_of_birth"].str.contains("[a-zA-Z]", regex=True)]


def rows_without_atsymbol_email_address_locator(df):
    """Locates rows where 'email_address' does not contain '@'."""
    return df[~df["email_address"].str.contains("@")]


def remove_rows_without_atsymbol_email_address(df):
    """transformation returns filtered df, with rows with no @ in email address removed."""
    return df[df["email_address"].str.contains("@")]


def clean_user_data_rows_all_NULL(df):
    """Returns the transformed DataFrame. The transformation was dropping rows consisting entirely of the string 'NULL' as entries"""
    cleaned_df = df.drop(df[df.eq("NULL").all(axis=1)].index)
    return cleaned_df


def is_upper_or_numeric(val):
    """
    The function `is_upper_or_numeric` checks if a given value is either uppercase or numeric.

    :param val: The parameter `val` is a variable that represents a value that we want to check if it is
    either uppercase or numeric
    :return: a boolean value indicating whether the input value is either uppercase or numeric.
    """
    return val.isupper() or val.isnumeric()


def clean_upper_or_numeric_rows(df):
    """Returns transformed DataFrame. The transformation was dropping rows consisting entirely of upper or numeric strings from the input"""
    return df.drop(df[df.applymap(is_upper_or_numeric).all(axis=1)].index)


def is_upper_or_numeric(val):
    if isinstance(val, str):
        return val.isupper() or val.isnumeric()
    elif isinstance(val, (int, float)):
        return True
    else:
        return False


def remove_newline_character(series):
    """Removes newline character from a pd.Series (IE a column) and replaces it with a comma and a space. Primarily useful for cleaning address columns./

    Args:
        series (pd.Series): typically a column to be cleaned
    """
    series = series.str.replace("\n", ", ")
    return series


def email_address_cleaner(df):
    """Replaces double '@' symbol typo with a single '@', to correct the email typos

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    """
    df["email_address"] = df["email_address"].str.replace("@@", "@")
    return df


def clean_country_code_ggb(df):
    """
    Replaces 'GGB' with 'GB' in the 'country_code' column

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    """
    df["country_code"] = df["country_code"].str.replace("GGB", "GB")
    return df


def find_na_rows(df):
    """
    Returns all rows where any entry in any column is 'N/A'.

    Parameters:
    - df (pd.DataFrame): DataFrame to search

    Returns:
    - pd.DataFrame: DataFrame containing only the rows with 'N/A' in any column.
    """
    return df[df.apply(lambda row: row.astype(str).str.contains("N/A").any(), axis=1)]


def drop_rows_with_invalid_card_numbers(df):
    return df[~df["card_number"].astype(str).str.contains("\?", regex=True)]


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
        entry = str(
            entry
        ).lower()  # Convert entry to string and make it lowercase for consistent matching
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

    df_products["weight(KG)"] = remove_newline_character(df_products["weight(KG)"])
    df_products["weight(KG)"] = df_products["weight(KG)"].str.strip(".")
    df_products["weight(KG)"] = df_products["weight(KG)"].apply(clean_weight_entry)
