import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import create_engine
import pandas as pd
import tabula
import requests

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
    def retrieve_pdf_data(self, link):
        try:
            dfs = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf", stream=True)
            print(len(dfs))
            print(dfs[0])  # Access the first DataFrame extracted from the PDF
            return dfs  # Return the extracted DataFrames
        except Exception as e:
            print(f"Error retrieving data from PDF: {e}")
        return None
    def list_number_of_stores(self, store_num_endpoint, header):
        try:
            response = requests.get("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve store number:{response.status_code}")
                return None
        except requests.RequestException as e:
                print(f"error during API request:{e}")
                return None
            
    def retrieve_stores_data(self, retrieve_store_endpoint):
        try:
            response = requests.get("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
            if response.status_code == 200:
                stores_data = response.json()  # Extract all stores data
                stores_df = pd.DataFrame(stores_data)  # Convert to DataFrame
                return stores_df
            else:
                print(f"Failed to retrieve stores data. Status code: {response.status_code}")
                print(f"Response content: {response.content.decode('utf-8')}")
                return None
        except requests.RequestException as e:
            print(f"Error during API request: {e}")
            return None

            
        
        

# Define your database connection URL, e.g., for PostgreSQL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Create a DataExtractor instance with the engine
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


pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'  
tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)

# `tables` will contain a list of DataFrames (one for each table found in the PDF)
# Access and manipulate the required DataFrame

# For example, if the table you want is in the first position (index 0) in the list:
card_data = tables[0]

# Display the extracted data
print(card_data)

#data_extractor = DataExtractor(engine)
#number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
#header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Extract the number of stores using the API
#number_of_stores = data_extractor.list_number_of_stores(number_stores_endpoint, header_details)
#print(f"Number of stores: {number_of_stores}")

# Use the DataExtractor class to retrieve store data from the API
data_extractor = DataExtractor(engine)

# API endpoint and header details
retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Retrieve all stores from the API and save in a DataFrame
stores_data = data_extractor.retrieve_stores_data(retrieve_store_endpoint)

# Print or use the obtained stores DataFrame as needed
print(stores_data)
