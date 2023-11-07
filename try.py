import yaml
import sqlalchemy
import pandas as pd
from data_cleaning import cleaned_user_data

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

file_path_user = 'db_user_creds.yml'
file_path_card = 'db_card_creds.yml'
cleaned_user_data_file = 'cleaned_user_data.csv'
cleaned_card_data_file = 'cleaned_card_data.csv'


