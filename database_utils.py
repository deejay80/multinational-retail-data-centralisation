import yaml
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import data_cleaning


class DatabaseConnector:
    def __init__(self, engine):
        self.engine = engine

    
    def read_db_creds(file):
        try:
            with open('db_creds.yml', mode='r') as creds_file:
                creds = yaml.safe_load(creds_file)
                return creds
        except FileNotFoundError:
            print(f"File '{file}' not found")
            return None

    
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

# Load database credentials from the 'db_creds.yaml' file
connector = DatabaseConnector.read_db_creds('db_creds.yaml')

if connector:
    # Initialize the database engine
    engine = DatabaseConnector.init_db_engine(connector)

    if engine:
        # Create an instance of the DatabaseConnector class with the engine
        db_connector = DatabaseConnector(engine)

        # Load your cleaned user data from a CSV file into a Pandas DataFrame (replace with the actual file path)
        cleaned_user_data = pd.read_sql_table('cleaned_user_data')

        # Specify the table name where you want to upload the data (e.g., 'dim_users')
        table_name = 'dim_users'

        # Upload the cleaned user data to the 'dim_users' table in the 'sales_data' database
        db_connector.upload_to_db(cleaned_user_data, table_name)





