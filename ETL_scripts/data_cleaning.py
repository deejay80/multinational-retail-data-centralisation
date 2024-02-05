import pandas as pd
import re
import logging
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
      if 'weight' not in product_df.columns or 'product_price' not in product_df.columns:
        print("Warning: Weight or product_price column not found in DataFrame")
        

      def convert_to_kg(row):
        try:
            weight_str = str(row['weight']).strip()  # Remove leading and trailing spaces

            if 'x' in weight_str:
                # Handle 'x' separately for multiplication
                unit, quantity = weight_str.split('x')
                unit = float(unit)
                quantity = float(re.sub(r'[^0-9.]', '', quantity))
                converted_weight = unit * quantity / 1000
                
            else:
                # Remove any non-numeric characters from the weight string
                weight_numeric = re.sub(r'[^0-9.]', '', weight_str)

                if 'kg' in weight_str:
                    converted_weight = float(weight_numeric)
                elif 'g' in weight_str:
                    converted_weight = float(weight_numeric) / 1000
                elif 'ml' in weight_str:
                    converted_weight = float(weight_numeric) / 1000
                elif 'oz' in weight_str:
                    converted_weight = float(weight_numeric) * 0.0283495  # 1 oz = 0.0283495 kg
                else:
                    converted_weight = None  # No valid unit found

            if not pd.notna(converted_weight):
                logging.warning(f"Conversion issue in row:\n{row}\nConverted weight: {converted_weight}")

            return converted_weight

        except (ValueError, TypeError) as e:
            logging.error(f"Error in row:\n{row}\nError details: {e}")
            return None

      product_df['weight_in_kg'] = product_df.apply(convert_to_kg, axis=1)

    

    def clean_products_data(self, product_df):
        # Drop rows with NULL values in specific columns
        cleaned_product_df = product_df.dropna(subset=['weight', 'product_price', 'category'])

        # Clean product prices
        cleaned_product_df['product_price'] = cleaned_product_df['product_price'].str.replace('£', '')

        return cleaned_product_df
    
    def filter_by_cat(self, product_df, category):
        filtered_df = product_df[product_df['category'] == category].copy()
        return filtered_df


# Read the CSV file
product_df = pd.read_csv('product_df.csv')

# Instantiate the DataCleaning class
data_cleaner = DataCleaning()

# Call the convert_product_weights method
data_cleaner.convert_product_weights(product_df)
print(product_df)
# Call the clean_products_data method to clean the DataFrame
cleaned_product_df = data_cleaner.clean_products_data(product_df)
cleaned_product_df = cleaned_product_df.drop(columns=['Unnamed: 0'], errors='ignore')

cleaned_product_df.to_csv('cleaned_product_df.csv', index=False)

# Print the cleaned DataFrame
print(cleaned_product_df)

cleaned_product_df.to_csv('cleaned_product_df.csv',index = False)

filtered_product = data_cleaner.filter_by_cat(cleaned_product_df, 'pets')
print(filtered_product)

