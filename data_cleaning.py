import pandas as pd
import re
from data_extraction import DataExtractor

class DataCleaning:
    """
  A class for cleaning and preprocessing data.
  
  Methods:
        clean_user_data(user_data):
            Cleans user data by handling NULL values, converting date strings to datetime objects,
            and standardizing phone numbers.

        clean_card_data(pdf_data):
            Cleans card data by removing rows with NULL values.

        clean_store_data(stores_data_table):
            Cleans store data by dropping the 'lat' column.

        convert_product_weights(product_df):
            Converts product weights to kilograms.

        clean_products_data(product_df):
            Cleans product data by dropping duplicates based on the 'category' column, removing NULL values
            in the 'weight' column, and dropping unnecessary columns.

        clean_orders_data(orders_data):
            Cleans orders data by removing specified columns ('first_name', 'last_name', '1').

        clean_date_data(date_data):
            Cleans date data by dropping NULL values and the 'date_uuid' column.
    
    """
    def __init__(self,date_data=None) ->None:
        # self.product_df = product_df
        # self.orders_data = orders_data
          self.date_data = date_data
    """   
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
    
    def clean_store_data(self, stores_data):
        cleaned_data = []
        cleaned_store_data = stores_data.drop(['lat'], axis=1)
        cleaned_store_data['opening_date'] = pd.to_datetime(cleaned_store_data['opening_date'],errors = 'coerce')
        
        null_count = cleaned_store_data.isnull().sum()
        cleaned_data.append({'cleaned store data': cleaned_store_data, 'Null Counts': null_count})
        return cleaned_store_data
     """  
        
    


    def convert_product_weights(self, product_df):
        if 'weight' not in product_df.columns:
            print("Warning: Weight column not found in DataFrame")

        def convert_to_kg(weight_str):
            try:
                if 'kg' in weight_str:
                    return  weight_str
                elif 'g' in weight_str:
                    return float(re.sub(r'\D+', '', weight_str)) / 1000
                elif 'ml' in weight_str:
                    return float(re.sub(r'\D+', '', weight_str)) / 1000
                
                else:
                    return None
            except (ValueError, TypeError):
                return None

        product_df['weight'] = product_df['weight'].apply(convert_to_kg)
    
    def clean_products_data(self, product_df):
        product_df = product_df.dropna(subset=['weight','product_price','category'])
        product_df['product_price'] = product_df['product_price'].str.replace('£','')

        #cleaned_product_df = product_df.drop_duplicates(subset=['category'])
        cleaned_product_df = product_df.drop(columns=['removed', 'uuid'])

        return cleaned_product_df
    
    
product_df = pd.read_csv('product_df.csv')
# Example Usage
data_cleaner = DataCleaning()

# Assuming product_df is your original DataFrame
cleaned_product_df = product_df.copy()

data_cleaner.convert_product_weights(cleaned_product_df)
cleaned_product_df = data_cleaner.clean_products_data(cleaned_product_df)
print(cleaned_product_df)
cleaned_product_df = pd.read_csv('cleaned_product_df.csv')