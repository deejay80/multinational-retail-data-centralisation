import pandas as pd
import re
class DataCleaning:
    def __init__(self):
        
        '''
    def clean_user_data(self, user_data):
        # Handle NULL values (e.g., replace them with appropriate values or drop rows)
        user_data.dropna(axis='index', inplace=True)  # Replace 'country_code' with the actual column name

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

    def clean_store_data(self, stores_data_table):
        cleaned_data = []
        cleaned_store_data = stores_data_table.drop(['lat'], axis=1)

        null_count = cleaned_store_data.isnull().sum()
        cleaned_data.append({'cleaned store data': cleaned_store_data, 'Null Counts': null_count})
        return cleaned_store_data
        '''
        
    
    def convert_product_weights(self,product_df):
        
        if 'weight' not in product_df.column:
            print("Warning:Weight column not found in DataFrame")
        
        # Define Function to  convert weights to kg
        def convert_to_kg(weight_str):
            try:
                # Handle ml to g conversion (assuming 1 ml = 1 g)
                if 'ml' in weight_str:
                    return float(re.sub(r'\D+', '', weight_str)) / 1000
                # Convert other units to kg as needed
                elif 'g' in weight_str:
                    return float(re.sub(r'\D+', '', weight_str)) / 1000
                elif 'kg' in weight_str:
                    return float(re.sub(r'\D+', '', weight_str))
                else:
                    return None  # Handle other cases if needed
            except (ValueError, TypeError):
                return None
        #Apply the conversion function to the 'weight' column
        product_df['weight'] = product_df['weight'].apply(convert_to_kg)
        # Drop rows with invalid or None values in the 'weight' column
        product_df = product_df.dropna(subset=['weight'])
        
        return product_df
    
                
        
    def clean_products_data(self,product_df):
       cleaned_product_df = product_df.drop_duplicates(subset=['category'])
       cleaned_product_df = product_df.dropna(subset=['weight'])
       
       return cleaned_product_df
    
    def clean_orders_data(self,orders_data):
        # Remove specified columns (first_name, last_name, 1)
        columns_to_remove = ['first_name', 'last_name', 1]
        cleaned_orders_data = orders_data.drop( columns=columns_to_remove, errors='ignore')

        # Additional cleaning steps go here
        # You can add specific cleaning logic based on your requirements

        return cleaned_orders_data
    def clean_date_details(self,date_details):
        cleaned_date_details = date_details.dropna()
        return cleaned_date_details
    


  


'''
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


stores_data = DataExtractor.retrieve_stores_data.stores_data
data_cleaner = DataCleaning(stores_data)

fine_data = data_cleaner.clean_store_data(stores_data)


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


data_cleaner = DataCleaning()

cleaned_store_data = data_cleaner.clean_store_data(stores_data)

    #cleaned_store_data.to_csv('cleaned_store_data.csv',index = False)
    # Display the cleaned data
print(cleaned_store_data)
'''
data_cleaner = DataCleaning()

cleaned_product_df = data_cleaner.clean_products_data(product_df)

    #cleaned_store_data.to_csv('cleaned_store_data.csv',index = False)
    # Display the cleaned data
print(cleaned_product_df)

