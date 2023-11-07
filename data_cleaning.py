import pandas as pd
from sqlalchemy import create_engine
import tabula
from data_extraction import DataExtractor

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




