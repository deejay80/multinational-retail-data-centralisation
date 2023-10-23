import pandas as pd
from sqlalchemy import create_engine

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_data):
        # Handle NULL values (e.g., replace them with appropriate values or drop rows)
        user_data.dropna(axis ='index', inplace=True)  # Replace 'country_code' with the actual column name

        # Handle date errors (e.g., convert date strings to datetime objects)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], errors='coerce')

        return user_data

# Create an instance of the DataCleaning class
data_cleaner = DataCleaning()

# Define your database connection URL
db_url = "postgresql://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Load user data from the database
table_name = 'legacy_users'  # Replace with your actual table name
user_data = pd.read_sql_table(table_name, engine)

# Clean the user data
cleaned_user_data = data_cleaner.clean_user_data(user_data)

# You can now use the 'cleaned_user_data' for further analysis or storage.
print(cleaned_user_data)