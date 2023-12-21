import sqlalchemy
from sqlalchemy import inspect, create_engine
import pandas as pd
import tabula
import requests
import boto3
import io
class DataExtractor:
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
            df = tabula.read_pdf(pdf_path, stream=True)
            dfs = pd.DataFrame(df)
            return dfs  # Return the extracted DataFrames
        except Exception as e:
            print(f"Error retrieving data from PDF: {e}")
            return None

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
        '''

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
        '''    
        
    def extract_json_data(self,json_url):
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

        

    # Constants
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
date_details_s3_address = "s3://data-handling-public/date_details.json"
products_s3_address = "s3://data-handling-public/products.csv"
date_details_s3_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    # Define your database connection URL, e.g., for PostgreSQL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

    # Create an SQLAlchemy engine
engine = create_engine(db_url)

    # Instantiate DataExtractor
data_extractor = DataExtractor(engine)

    # List database tables
tables = data_extractor.list_db_tables()
print("Database tables:", tables)

    # Now, let's find the table that likely contains user data based on its name or description.
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
        user_data_df = data_extractor.read_rds_table(user_data_table)
        if user_data_df is not None:
            print(f"User data extracted from table '{user_data_table}':")
            print(user_data_df.head(10))
        else:
            print("Data extraction from the user data table failed.")
else:
        print("User data table not found in the database.")
        '''
    # Extract the number of stores using the API
number_of_stores = data_extractor.list_number_of_stores(number_stores_endpoint, header_details)
print(f"Number of stores: {number_of_stores}")

    # API endpoint and header details
retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"


    # Retrieve all stores from the API and save in a DataFrame
stores_data_table = data_extractor.retrieve_stores_data(retrieve_store_endpoint, header_details, number_of_stores)
if stores_data_table is None:
    print(stores_data_table.head())
    
stores_data_table.to_csv('stores_data_table.csv',index = False)
     

# Extract data from S3
product_df = data_extractor.extract_from_s3(products_s3_address)
# Display the first few rows of the extracted data
if product_df is not None:
    print(product_df.head())
    
product_df.to_csv('product_df.csv',index=False)

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
        print(orders_data.head(10))
    else:
        print("Data extraction from the product orders table failed.")
else:
    print("Product orders table not found in the database.")

orders_data.to_csv('orders_data.csv',index = False)
 '''
# Extract data from the given S3 URL
date_details = data_extractor.extract_json_data(date_details_s3_url)
date_details.to_csv('date_details.csv',index = False)
