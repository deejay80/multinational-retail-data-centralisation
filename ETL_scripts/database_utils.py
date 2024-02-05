import yaml
import sqlalchemy
import pandas as pd

class DatabaseConnector:
    """
    A class for connecting to a PostgreSQL database and uploading data.

    Parameters:
        creds_file (str): Path to the YAML file containing database credentials.

    Attributes:
        creds_file (str): Path to the YAML file containing database credentials.
        connector (dict): Database credentials loaded from the YAML file.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database interaction.

    Methods:
        read_db_creds():
            Reads and returns the database credentials from the specified YAML file.

        init_db_engine():
            Initializes and returns a SQLAlchemy engine using the loaded database credentials.

        upload_user_data(df, table_name):
            Uploads user data to the specified table in the database.

        upload_card_data(df, table_name_card):
            Uploads card data to the specified table in the database.

        upload_store_data(df, store_details):
            Uploads store data to the specified table in the database.

        upload_product_data(df, product_details):
            Uploads product data to the specified table in the database.

        upload_orders_data(df, orders_details):
            Uploads orders data to the specified table in the database.

        upload_date_data(df, date_details):
            Uploads date data to the specified table in the database.
         """

    def __init__(self, creds_file):
        """
        Initializes a new instance of the DatabaseConnector class.

        Parameters:
            creds_file (str): Path to the YAML file containing database credentials.
        """
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
        """
        Initializes and returns a SQLAlchemy engine using the loaded database credentials.

        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine.
        """

        if self.connector:
            try:
                db_url = f"postgresql://{self.connector['RDS_USER']}:{self.connector['RDS_PASSWORD']}@{self.connector['RDS_HOST']}:{self.connector['RDS_PORT']}/{self.connector['RDS_DATABASE']}"
                engine = sqlalchemy.create_engine(db_url)
                return engine
            except Exception as e:
                print(f"Error loading engine: {e}")
                return None
    def upload_user_data(self, df, table_name):
        if self.engine:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table '{table_name}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table '{table_name}': {e}")
    
    

    def upload_card_data(self, df, table_name_card):
        if self.engine:
            try:
                df.to_sql(table_name_card, self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table '{table_name_card}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table '{table_name_card}': {e}")
           
    def upload_store_data(self, df, store_details):
        if self.engine:
            try:
                df.to_sql(store_details, self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table '{store_details}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table '{store_details}': {e}")
            
                
    def upload_product_data(self,df,product_details):
        if self.engine:
            try:
                df.to_sql(product_details,self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table'{product_details}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table'{product_details}':{e}")
               
    def upload_orders_data(self,df,orders_details):
        if self.engine:
            try:
                df.to_sql(orders_details,self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table'{orders_details}' succesfully.")
            except Exception as e:
                print(f"Error uploading data to table'{orders_details}':{e}")
    
    def upload_date_data(self,df,date_details):
        if self.engine:
            try:
                df.to_sql(date_details,self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to table'{date_details}' succesfully.")
            except Exception as e:
                print(f"Error uploading data to table'{date_details}':{e}")

# Usage:
file1 = 'db_creds.yml'  # Update with the correct file name
file2 = 'db_upload_creds.yml'  # Update with the correct file name

connector = DatabaseConnector(file1)  # Use the first set of credentials
connector2 = DatabaseConnector(file2)  # Use the second set of credentials


# Assuming 'user_data.csv' exists and has the appropriate data structure
cleaned_user_data = pd.read_csv('cleaned_user_data.csv')
cleaned_card_data = pd.read_csv('cleaned_card_data.csv')
cleaned_store_data = pd.read_csv('cleaned_store_data.csv')
cleaned_product_df = pd.read_csv('cleaned_product_df.csv')
cleaned_orders_data = pd.read_csv('cleaned_orders_data.csv')
cleaned_date_data = pd.read_csv('cleaned_date_data.csv')
table_name = 'dim_users'
table_name_card = 'dim_card_details'
stores_details = 'dim_store_details'
product_details = 'dim_products'
orders_details = 'orders_table'
date_details = 'dim_date_times'
#connector2.upload_user_data(cleaned_user_data, table_name)  
#connector2.upload_card_data(cleaned_card_data,table_name_card)
#connector2.upload_store_data(cleaned_store_data,stores_details)
connector2.upload_product_data(cleaned_product_df,product_details)
#connector2.upload_orders_data(cleaned_orders_data,orders_details)
#connector2.upload_date_data(cleaned_date_data,date_details)



