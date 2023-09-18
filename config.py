"""
Configurations for the project.

This module reads key-value pairs from a `.env` file and sets them as
environment variables. It then retrieves the API_KEY from the environment.

Attributes:
    api_key (str): The API key retrieved from the environment.
"""


import os

with open(".env", "r") as f:
    for line in f:
        if line.strip() == "" or line.startswith("#"):
            continue
        key, value = line.strip().split("=", 1)
        os.environ[key] = value

api_key = os.environ.get("API_KEY")
print(api_key)
