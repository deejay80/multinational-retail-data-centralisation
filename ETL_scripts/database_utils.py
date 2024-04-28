import yaml
from sqlalchemy import create_engine


class DatabaseConnector:
    """
    A class for connecting to a PostgreSQL database and uploading data.

    
    Methods:
        read_db_creds():Reads and returns the database credentials from the specified YAML file.
        init_db_engine():Initializes and returns a SQLAlchemy engine using the loaded database credentials.
        upload_to_db():This method is used to upload cleaned data to the database.


    """
    def __init__(self, file_path):
        ''' This initialises the instance of the class based on the credentials supplied.

        Args:
            file_path (`str`): This is the filepath for the credentials.
        '''
        self.file = file_path

    class NotYAMLFileError(Exception):
        "definining a custom exception for when the file isnt a YAML file."
        pass

    def read_db_creds(self):
        
        try:
            if not self.file.endswith('.yaml') and not self.file.endswith('.yml'):
                raise self.NotYAMLFileError("File is not a YAML file.")

            with open(self.file, 'r') as file:
                cred_dict = yaml.load(file, Loader=yaml.FullLoader)
            return cred_dict

        except (FileNotFoundError, TypeError, self.NotYAMLFileError, ValueError, yaml.YAMLError, AttributeError) as e:
            print(f'Error: {e}, please check your file path, format, and content.')

    def init_db_engine(self):
        
        credentials = self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = credentials["RDS_HOST"]
        USER = credentials["RDS_USER"]
        PASSWORD = credentials["RDS_PASSWORD"]
        DATABASE = credentials["RDS_DATABASE"]
        PORT = credentials["RDS_PORT"] 

        engine_for_extraction = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", future=True)

        return engine_for_extraction

    def upload_to_db(self, cleaned_dataframe, table_name): 
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = ''
        DATABASE = 'sales_data'
        PORT = 5432
        engine_for_uploading = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", future=True)

        with engine_for_uploading.begin() as connection_to_sales_data:
            cleaned_dataframe.to_sql(table_name, con=connection_to_sales_data, if_exists='replace')