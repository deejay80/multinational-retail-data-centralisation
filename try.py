import yaml
import sqlalchemy
import pandas as pd

class DatabaseConnector:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.connector = self.read_db_creds()  # Load the credentials immediately
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        try:
            with open(self.creds_file, mode='r') as creds_file:
                creds = yaml.safe_load(creds_file)
                return creds
        except FileNotFoundError:
            print(f"Credential file '{self.creds_file}' not found")
            return None

    def init_db_engine(self):
        if self.connector:
            try:
                db_url = f"postgresql://{self.connector['RDS_USER']}:{self.connector['RDS_PASSWORD']}@{self.connector['RDS_HOST']}:{self.connector['RDS_PORT']}/{self.connector['RDS_DATABASE']}"
                engine = sqlalchemy.create_engine(db_url)
                return engine
            except Exception as e:
                print(f"Error loading engine: {e}")
                return None

    def upload_to_db(self, df, table_name):
        if self.engine:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table '{table_name}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table '{table_name}': {e}")

# Usage:
file1 = 'db_creds.yml'  # Update with the correct file name
file2 = 'db_upload_creds.yml'  # Update with the correct file name

connector = DatabaseConnector(file1)  # Use the first set of credentials
connector2 = DatabaseConnector(file2)  # Use the second set of credentials

# Assuming 'user_data.csv' exists and has the appropriate data structure
cleaned_store_data = pd.read_csv('cleaned_store_data.csv')
cleaned_product_df = pd.read_csv('products_data_df.csv')
cleaned_orders_data = pd.read_csv('cleaned_orders_data.csv')
table_name = 'dim_users'
table_name_card = 'dim_card_details'
stores_details = 'dim_store_details'
product_details = 'dim_products'
orders_details = 'orders_table'
connector2.upload_to_db(cleaned_store_data, stores_details)
connector2.upload_to_db(cleaned_product_df, product_details)
connector2.upload_to_db(cleaned_orders_data, orders_details)
