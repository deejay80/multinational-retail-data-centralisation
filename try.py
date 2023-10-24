import yaml
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def read_db_creds(file):
        try:
            with open('db_creds.yml', mode='r') as creds_file:
                creds = yaml.safe_load(creds_file)
                return creds
        except FileNotFoundError:
            print(f"File '{'db_creds.yml'}' not found")
            return None 

    @staticmethod
    def init_db_engine(connector):
        try:
            db_url = f"postgresql://{connector['RDS_USER']}:{connector['RDS_PASSWORD']}@{connector['RDS_HOST']}:{connector['RDS_PORT']}/{connector['RDS_DATABASE']}"
            engine = sqlalchemy.create_engine(db_url)
            return engine
        except Exception as e:
            print(f"Error loading engine: {e}")
            return None

    def upload_to_db(self, df, table_name):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")

