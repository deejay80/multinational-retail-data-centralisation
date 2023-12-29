import pandas as pd
import re
from data_extraction import DataExtractor

class DataCleaning:
    
    def __init__(self,date_data=None) ->None:
        # self.product_df = product_df
        # self.orders_data = orders_data
          self.date_data = date_data
        
    def clean_user_data(self, user_data):
        # Handle NULL values (e.g., replace them with appropriate values or drop rows)
        user_data.dropna(axis='index', inplace=True)  

        # Handle date errors (e.g., convert date strings to datetime objects)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], errors='coerce')

        # Standardize phone numbers to a specific format
        user_data['phone_number'] = user_data['phone_number'].str.replace(r'\D+', '', regex=True)
        user_data['phone_number'] = user_data['phone_number'].str[-10:]  # Assuming 10 digits are required

        return user_data

    def clean_card_data(self, pdf_data):
        # Remove rows with NULL values
        cleaned_card_data = pdf_data.dropna()

        return cleaned_card_data

    def clean_store_data(self, stores_data_table):
        cleaned_data = []
        cleaned_store_data = stores_data_table.drop(['lat'], axis=1)

        null_count = cleaned_store_data.isnull().sum()
        cleaned_data.append({'cleaned store data': cleaned_store_data, 'Null Counts': null_count})
        return cleaned_store_data
        
        
    
    def convert_product_weights(self,product_df):
        
        if 'weight' not in product_df.columns:
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
       cleaned_product_df = product_df.drop(columns=['removed','uuid'])
       
       return cleaned_product_df
    
    def clean_orders_data(self,orders_data):
        # Remove specified columns (first_name, last_name, 1)
        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_data = orders_data.drop( columns=columns_to_remove, errors='ignore')

        # Additional cleaning steps go here
        # You can add specific cleaning logic based on your requirements

        return cleaned_orders_data
    
    def clean_date_data(self,date_data):
        cleaned_date_data = date_data.dropna()
        cleaned_date_data = date_data.drop(columns='date_uuid')
        return cleaned_date_data
    


  


user_data = pd.read_csv('user_data.csv')
pdf_data_df = pd.read_csv('pdf_data_df')
stores_data = pd.read_csv('stores_data.csv')
product_df = pd.read_csv('product_df.csv')
orders_data = pd.read_csv('orders_data.csv')
date_data = pd.read_csv('date_data.csv')

data_cleaner = DataCleaning()

cleaned_user_data = data_cleaner.clean_user_data(user_data_df)
print(cleaned_user_data.head())
cleaned_user_data.to_csv('cleaned_user_data.csv')

cleaned_card_data = data_cleaner.clean_card_data(pdf_data_df)
print(cleaned_card_data.head())
cleaned_card_data.to_csv('cleaned_card_data.csv')

cleaned_store_data = data_cleaner.clean_store_data(stores_data)
print(cleaned_store_data.head())
cleaned_store_data.to_csv('cleaned_store_data.csv')



cleaned_product_df = data_cleaner.clean_products_data(product_df)
print(cleaned_product_df.head())



cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)
print(cleaned_orders_data.head())
cleaned_orders_data.to_csv('cleaned_orders_data.csv')

cleaned_date_data = data_cleaner.clean_date_data(date_data)
print(cleaned_date_data.head())
cleaned_date_data.to_csv('cleaned_date_data.csv')
