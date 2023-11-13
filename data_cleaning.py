import pandas as pd
from sqlalchemy import create_engine
import tabula
from data_extraction import DataExtractor
import requests

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_data):
        # Handle NULL values (e.g., replace them with appropriate values or drop rows)
        user_data.dropna(axis ='index', inplace=True)  # Replace 'country_code' with the actual column name

        # Handle date errors (e.g., convert date strings to datetime objects)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], errors='coerce')
        
        # Standardize phone numbers to a specific format
        user_data['phone_number'] = user_data['phone_number'].str.replace(r'\D+', '', regex=True)
        user_data['phone_number'] = user_data['phone_number'].str[-10:]  # Assuming 10 digits are required


        return user_data
    
    def clean_card_data(self, tables):
        # Remove rows with NULL values
        cleaned_data = tables.dropna()

        # Check for and handle erroneous values or formatting errors
        # For instance, correcting data types, formatting issues, etc.
        
        return cleaned_data
    
    def clean_store_data(self, stores_data):

        # Perform data cleaning steps such as removing NaN values, renaming columns, etc.
        # For example, assuming column 'name' has NaN values and you want to drop those rows:
        cleaned_stores_data = stores_data.dropna(subset=['name'])
        df_cleaned_data = pd.DataFrame(cleaned_stores_data)  # Convert the API response data to a DataFrame
        
        return df_cleaned_data


# Create an instance of the DataCleaning class
data_cleaner = DataCleaning()

# Define your database connection URL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Load user data from the database
table_name = 'legacy_users'  
user_data = pd.read_sql_table(table_name, engine)
user_data.to_csv('user_data', index=False)

# Clean the user data
cleaned_user_data = data_cleaner.clean_user_data(user_data)
cleaned_user_data.to_csv('cleaned_user_data',index=False)
print(cleaned_user_data)





# Load card data from the PDF file
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)

# Clean and concatenate card data from all tables into a single DataFrame
cleaned_card_data = None

for table in tables:
    cleaned_table = data_cleaner.clean_card_data(table)
    if cleaned_card_data is None:
        cleaned_card_data = cleaned_table
    else:
        cleaned_card_data = pd.concat([cleaned_card_data, cleaned_table])

# Save cleaned card data to a CSV file
cleaned_card_data.to_csv('cleaned_card_data.csv', index=False)
print(cleaned_card_data)

# Assuming 'data_extractor' is an instance of the DataExtractor class
# Retrieving the data from the API

retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
store_number = 400
headers = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
cleaned_store_data = None
# Retrieve data from the API
response = requests.get(retrieve_store_endpoint, headers )
if response.status_code == 200:
    stores_data = response.json()
    stores_data_df = pd.DataFrame(stores_data,index= [0])  # Convert to DataFrame
    cleaned_store_data = data_cleaner.clean_store_data(stores_data_df)

    # Display the cleaned data
    print(cleaned_store_data)
else:
    print(f"Failed to retrieve store data. Status code: {response.status_code}")
    print(f"Response content: {response.content.decode('utf-8')}")



