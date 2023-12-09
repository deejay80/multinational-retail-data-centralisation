import pandas as pd
from data_extraction import DataExtractor 
import tabula
import requests
from sqlalchemy import inspect, create_engine





class DataCleaning:
    def __init__(self,stores_data)->None:
        de = DataExtractor()
        self.stores_data = stores_data

        
        
    
'''
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
        cleaned_card_data = tables.dropna()

        # Check for and handle erroneous values or formatting errors
        # For instance, correcting data types, formatting issues, etc.
        
        return cleaned_card_data
    '''
def clean_store_data(self, stores_data):
        cleaned_data =[]
        for store_data in stores_data:
         cleaned_store_data = store_data.drop(['lat'], axis = 1)

         null_count = cleaned_store_data.isnull().sum()
         cleaned_data.append({'cleaned store data':cleaned_store_data,'Null Counts':null_count})    
        return cleaned_store_data
    
    
    # Define your database connection URL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Load user data from the database
table_name = 'legacy_users'  
user_data = pd.read_sql_table(table_name, engine)
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
header_details = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

number_of_stores = DataExtractor.list_number_of_stores(number_stores_endpoint,header_details)

retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
stores_data = DataExtractor.retrieve_stores_data(retrieve_store_endpoint, header_details, number_of_stores)


data_cleaner = DataCleaning()

stores_data = DataExtractor.retrieve_store_data.stores_data
fine_data = data_cleaner.clean_store_data(stores_data)


'''# Define your database connection URL
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




cleaned_store_data = data_cleaner.clean_store_data(stores_data)

    #cleaned_store_data.to_csv('cleaned_store_data.csv',index = False)
    # Display the cleaned data
print(cleaned_store_data)
    

#cleaned_store_data = data_cleaner.clean_store_data(stores_data)
#print(cleaned_store_data)
'''

