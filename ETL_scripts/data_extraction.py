import sqlalchemy
from sqlalchemy import inspect, create_engine
import pandas as pd
import tabula
import requests
import boto3
import io

class DataExtractor:
    """"
    A utility class for extracting and preprocessing data from various sources,including databases,API,PDF,S3 and JSON.
    
    Parameters:
        engine : SQLAlchemy engine for connecting to databases.

    Methods:
        - list_db_tables():
            Returns a list of table names in the connected database.

        - extract_data_from_table(table_name, columns=None):
            Extracts data from a specified database table and returns the result as a list of tuples.

        - read_rds_table(table_name):
            Reads data from a specified database table into a Pandas DataFrame.

        - retrieve_pdf_data(pdf_path):
            Extracts tabular data from a PDF file using the Tabula library and returns it as a Pandas DataFrame.

        - list_number_of_stores(store_num_endpoint, header_details):
            Retrieves the number of stores from an API endpoint.

        - retrieve_stores_data(retrieve_store_endpoint, header_details, number_of_stores):
            Retrieves detailed information about stores from an API endpoint and returns the data as a Pandas DataFrame.

        - extract_from_s3(s3_address):
            Extracts data from an S3 bucket specified by the address and returns it as a Pandas DataFrame.

        - extract_date_data(json_url):
            Retrieves data from a JSON endpoint and returns it as a Pandas DataFrame.

    """
    def __init__(self, engine):
        self.engine = engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names
    

    def extract_data_from_table(self, table_name, columns=None):
        with self.engine.connect() as connection:
            if columns:
                column_num = ','.join(columns)
                query = f"SELECT {column_num} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
            result = connection.execute(query)
            data = result.fetchall()
            return data
    
    def read_rds_table(self, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            data = pd.read_sql_query(query, self.engine)
            return data
        except Exception as e:
            print(f"Error reading from {table_name}: {e}")
            return None
    
    def retrieve_pdf_data(self, pdf_path):
        try:
                # Download PDF data from the link
            pdf_data = requests.get(pdf_path).content

            # Use tabula to extract data from all pages of the PDF
            dfs = tabula.read_pdf(io.BytesIO(pdf_data), pages='all', stream=True)

            # Combine all DataFrames into a single DataFrame
            result_df = pd.concat(dfs, ignore_index=True)

            return result_df
        except Exception as e:
            print(f"Error retrieving data from PDF: {e}")
            return None
    '''
    def list_number_of_stores(self, store_num_endpoint, header_details):
        try:
            response = requests.get(store_num_endpoint, headers=header_details)
            if response.status_code == 200:
                number_of_stores = response.json()
                return number_of_stores['number_stores']
            else:
                print(f"Failed to retrieve store number: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error retrieving number of stores: {e}")
            return None
        

    def retrieve_stores_data(self, retrieve_store_endpoint, header_details, number_of_stores):
        stores_data = []

        for store_number in range(0, number_of_stores ):
            store_url = retrieve_store_endpoint.format(store_number=store_number)
            response = requests.get(store_url, headers=header_details)

            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Error for store {store_number}: {response.status_code}")

        return pd.DataFrame(stores_data)
    '''
    
    def extract_from_s3(self, s3_address):
        try:
            # Parse S3 address
            s3_info = s3_address.replace("s3://", "").split("/")
            bucket_name = s3_info[0]
            key = "/".join(s3_info[1:])

            # Download data from S3
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            data = obj['Body'].read()

            # Convert bytes to DataFrame
            df = pd.read_csv(io.BytesIO(data), encoding='utf-8')

            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None 
    
        
    def extract_date_data(self,json_url):
        try:
            response = requests.get(json_url)
            if response.status_code == 200:
                json_data = response.json()
                return pd.DataFrame(json_data)
            else:
                print(f"Failed to retrieve JSON data: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error retrieving JSON data: {e}")
            return None

if __name__ == "__main__":
       

            # Constants
    # API endpoint for retrieving number of stores
 number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # API endpoint for retrieving store details
retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    # Headers for API requests
header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    # URL or local path to a PDF file for data extraction.
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # S3 address for date details data.
date_details_s3_address = "s3://data-handling-public/date_details.json"
    # S3 address for product data in CSV format.
products_s3_address = "s3://data-handling-public/products.csv"
    # URL for retrieving date details data from S3
date_details_s3_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    # Define your database connection URL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

    # Create an SQLAlchemy engine
engine = create_engine(db_url)

    # Instantiate DataExtractor
data_extractor = DataExtractor(engine)
'''
    # List database tables
tables = data_extractor.list_db_tables()
print("Database tables:", tables)

    # Extract user data from specified table.
user_data_table = None

for table_name in tables:
        if 'user' in table_name.lower():  # Check if the table name contains 'user'
            user_data_table = table_name
            break

if user_data_table:
        print(f"The table containing user data is: {user_data_table}")
else:
        print("No user data table found.")

    # Extract the user data from the identified table
if user_data_table:
        user_data = data_extractor.read_rds_table(user_data_table)
        if user_data is not None:
            print(f"User data extracted from table '{user_data_table}':")
            print(user_data.head())
        else:
            print("Data extraction from the user data table failed.")
else:
        print("User data table not found in the database.")
        
     # Retrieve and display the PDF data as a DataFrame
    
pdf_data = data_extractor.retrieve_pdf_data(pdf_path)
if pdf_data is not None:
    print(pdf_data.head())
else:
    print("PDF data extraction failed.")
    pdf_data.to_csv('pdf_data.csv')
     # Extract the number of stores using the API

number_of_stores = data_extractor.list_number_of_stores(number_stores_endpoint, header_details)
print(f"Number of stores: {number_of_stores}")

    # API endpoint and header details
retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"


    # Retrieve all stores from the API and save in a DataFrame
stores_data_table = data_extractor.retrieve_stores_data(retrieve_store_endpoint, header_details, number_of_stores)
if stores_data_table is None:
    print(stores_data_table.head())
'''
     

# Extract data from S3
product_df = data_extractor.extract_from_s3(products_s3_address)
# Display the first few rows of the extracted data
if product_df is not None:
    print(product_df.head())
product_df.to_csv('product_df.csv',index = False)

# Step 1: List all tables in the database
tables = data_extractor.list_db_tables()
print("Database tables:", tables)

# Find the table that likely contains information about product orders
product_orders_table = None

for table_name in tables:
    if 'order' in table_name.lower():  # Check if the table name contains 'order'
        product_orders_table = table_name
        break

if product_orders_table:
    print(f"The table containing product orders is: {product_orders_table}")
else:
    print("No product orders table found.")

# Step 2: Extract orders data using the read_rds_table method
if product_orders_table:
    orders_data = data_extractor.read_rds_table(product_orders_table)
    if orders_data is not None:
        print(f"Orders data extracted from table '{product_orders_table}':")
        print(orders_data.head())
    else:
        print("Data extraction from the product orders table failed.")
else:
    print("Product orders table not found in the database.")
    

# Extract data from the given S3 URL
date_data = data_extractor.extract_date_data(date_details_s3_url)
print(date_data.head())

date_data.to_csv('date_data.csv')
