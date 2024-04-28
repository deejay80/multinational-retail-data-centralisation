import requests
import pandas as pd
from sqlalchemy import inspect
import tabula
import boto3
import validators
from database_utils import DatabaseConnector


class DataExtractor:
    """
    A utility class for extracting and preprocessing data from various sources, including databases, APIs, PDFs, S3, and JSON.

    Parameters:
        engine : SQLAlchemy engine for connecting to databases.

    Methods:
        - list_db_tables():
            Returns a list of table names in the connected database.

        - extract_data_from_table(table_name, columns=None):
            Extracts data from a specified database table and returns the result as a list of tuples.

        - read_rds_table(table_name):
            Reads data from a specified database table into a Pandas DataFrame.

        - retrieve_pdf_data(pdf_path):
            Extracts tabular data from a PDF file using the Tabula library and returns it as a Pandas DataFrame.

        - list_number_of_stores(store_num_endpoint, header_details):
            Retrieves the number of stores from an API endpoint.

        - retrieve_stores_data(retrieve_store_endpoint, header_details, number_of_stores):
            Retrieves detailed information about stores from an API endpoint and returns the data as a Pandas DataFrame.

        - extract_from_s3(s3_address):
            Extracts data from an S3 bucket specified by the address and returns it as a Pandas DataFrame.

        - extract_date_data(json_url):
            Retrieves data from a JSON endpoint and returns it as a Pandas DataFrame.

    """

    def __init__(self):
        """Initialize DataExtractor class."""
        self.file = 'db_creds.yaml'
        self.rds_db_con = DatabaseConnector(self.file)
        self.engine = self.rds_db_con.init_db_engine()
        self.headers_stores = {
            "Content-Type": "application/json",
            "x-api-key": "<insert_key_here>"
        }

    def list_db_tables(self):
        """List all tables in the connected database.

        Returns:
            list: A list of table names in the connected database.
        """
        with self.engine.begin() as connection:
            inspector = inspect(connection)
            table_names = inspector.get_table_names()
        return table_names

    def extract_data_from_table(self, table_name, columns=None):
        """Extract data from a specified database table.

        Args:
            table_name (str): Name of the database table.
            columns (list, optional): List of column names to extract. Default is None.

        Returns:
            list: Data extracted from the specified database table as a list of tuples.
        """
        with self.engine.connect() as connection:
            if columns:
                column_num = ','.join(columns)
                query = f"SELECT {column_num} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
            result = connection.execute(query)
            data = result.fetchall()
            return data

    def read_rds_table(self, table):
        """Read data from a specified database table into a Pandas DataFrame.

        Args:
            table (str): Name of the database table.

        Returns:
            DataFrame: Pandas DataFrame containing data from the specified database table.
        """
        with self.engine.begin() as connection:
            df = pd.read_sql_table(table, connection)
        return df

    def retrieve_pdf_data(self, link):
        """Retrieve tabular data from a PDF document available at the specified URL.

        Args:
            link (str): The URL of the PDF document to extract data from.

        Returns:
            DataFrame or None: A DataFrame containing extracted tabular data from the PDF,
            or None in case of an invalid URL.
        """
        class ValidationError(Exception):
            pass

        try:
            if not validators.url(link):
                raise ValidationError
        except ValidationError:
            print(f'The URL ({link}) you have provided is invalid, please try again.')
            return

        pdf_data = tabula.read_pdf(link, pages='all', multiple_tables=True)
        pdf_data = pd.concat(pdf_data, ignore_index=True)
        return pdf_data

    def list_number_of_stores(self):
        """Retrieve the number of stores from an API endpoint.

        Returns:
            int: Number of stores retrieved from the API endpoint.
        """
        url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        response = requests.get(url, headers=self.headers_stores).json()
        number_of_stores = response['number_stores']
        return number_of_stores

    def retrieve_stores_data(self):
        """Retrieve detailed information about stores from an API endpoint.

        Returns:
            DataFrame: Pandas DataFrame containing detailed information about stores.
        """
        number_of_stores = self.list_number_of_stores()
        store_data = pd.DataFrame()

        for store_number in range(number_of_stores):
            url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            response = requests.get(url, headers=self.headers_stores).json()
            df_for_store = pd.DataFrame([response])
            store_data = pd.concat([store_data, df_for_store], ignore_index=True)
        store_data = store_data.set_index('index')
        return store_data

    def extract_from_s3(self, link):
        """Extract data from an S3 bucket specified by the address.

        Args:
            link (str): Address of the data in the S3 bucket.

        Returns:
            DataFrame: Pandas DataFrame containing data from the specified S3 bucket.
        """
        link_parts = link.split('/')
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', link_parts[-1], link_parts[-1])

        if '.csv' in link_parts[-1]:
            df = pd.read_csv(link_parts[-1])
        elif '.json' in link_parts[-1]:
            df = pd.read_json(link_parts[-1])

        return df
