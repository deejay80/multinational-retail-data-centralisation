import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import create_engine
import pandas as pd
import tabula

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

# Create a DataExtractor instance with the engine
data_extractor = DataExtractor(engine)

# Define the PDF link
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# Retrieve data from the PDF
extracted_data = data_extractor.retrieve_pdf_data(pdf_link)
# After extracting the data from the PDF:
if extracted_data is not None:
    print("Extracted Data:")
    for df in extracted_data:
        print("DataFrame:")
        print(df.head())  # Display the first few rows of each DataFrame extracted from the PDF
else:
    print("Extraction from the PDF failed.")
