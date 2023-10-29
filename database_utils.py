import yaml
import sqlalchemy
import pandas as pd

class DatabaseConnector:
    def __init__(self, engine):
        self.engine = engine

    @staticmethod
    def read_db_creds(file):
        try:
            with open(file, mode='r') as creds_file:
                creds = yaml.safe_load(creds_file)
                return creds
        except FileNotFoundError:
            print(f"File '{file}' not found")
            return None

    @staticmethod
    def read_db_upload_creds(file):
        try:
            with open(file, mode='r') as upcreds_file:
                upcreds = yaml.safe_load(upcreds_file)
                return upcreds
        except FileNotFoundError:
            print(f"File '{file}' not found")
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

# Load database credentials from the 'db_creds.yml' file
connector = DatabaseConnector.read_db_creds('db_creds.yml')

if connector:
    # Initialize the database engine
    engine = DatabaseConnector.init_db_engine(connector)

    if engine:
        # Create an instance of the DatabaseConnector class with the engine
        db_connector = DatabaseConnector(engine)

        # Load your data into a Pandas DataFrame (replace this with your actual data)
        user_data = pd.read_csv('user_data.csv')  # Replace with your data source

        # Load database credentials for the upload destination from 'db_upload_creds.yml' file
        connector2 = DatabaseConnector.read_db_upload_creds('db_upload_creds.yml')

        if connector2:
            # Initialize the second database engine for the upload destination
            engine2 = DatabaseConnector.init_db_engine(connector2)

            if engine2:
                # Create a second instance of the DatabaseConnector class with the second engine
                db_connector2 = DatabaseConnector(engine2)

                # Specify the table name where you want to upload the data
                table_name = 'dim_users'

                # Upload the data to the specified table in the second database
                db_connector2.upload_to_db(user_data, table_name)
