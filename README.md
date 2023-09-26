# ETL Centralisation AIcore

### 📌 Table of Contents
- [Introduction](#introduction)
- [Data Sources Overview]((#overview))
- [Technologies](#technologies)
- [Setup](#setup)
- [Usage](#usage)
- [Screenshots](#screenshots)
- [License](#license)

### Introduction

![ETL Screenshot](images/q1.png)


> ℹ️ **Task**: We manage large data from six distinct sources, each with its unique characteristics, required cleaning/extraction steps, and key fields. Here's an overview:

------
Each data source posed unique challenges for extraction, cleaning, and utilization, warranting customized treatment.


> 🤔 **My approach**: for each data source, I wrote and checked the functions in Notebooks before organising the code in scripts, adhering to OOP principles.

-----


### Data Sources Overview

#### 1.RDS Database in AWS (Order Table)

- **Table**: `order_table`
- **Relevance**: High; contains crucial sales information.
- **Fields to Use**: `date_uuid`, `user_uuid`, `card_number`, `store_code`, `product_code`, `product_quantity`.
- **Cleaning**: 
#### 2. RDS Database in AWS (User Data)

- **Table**: `dim_users`
- **Primary Key**: `user_uuid`
- **Cleaning**: 

#### 3. AWS S3 Public Link (Card Details)

- **Source**: PDF available through an S3 public link.
- **Table**: `dim_card_details`
- **Reading Method**: Utilizing the `tabula` package for PDF extraction.
- **Primary Key**: `card_number`
- **Cleaning**: 

#### 4. AWS S3 Bucket (Product Data)

- **Table**: `dim_product`
- **Reading Method**: Using `boto3` for data retrieval.
- **Primary Key**: `product_code`
- **Cleaning**: 
#### 5. Restful API (Store Details)

- **Table**: `dim_store_details`
- **Reading Method**: GET method from the API.
- **Data Format**: JSON; convert to a Pandas DataFrame.
- **Primary Key**: `store_code`
- **Cleaning**: 
#### 6. Public Link (Date Times)

- **Table**: `dim_date_times`
- **Reading Method**: JSON available through a public link; convert to a Pandas DataFrame.
- **Primary Key**: `date_uuid`
- **Cleaning**: 

### Technology
- Python
- Packages: Tabula, Boto3 , Pandas, Argparse, Yaml, Json, Requests, OS 
- AWS : RDS, S3
- PostgreSQL (PGadmin)

### Setup

### Usage

### Screenshots

### Licence 



