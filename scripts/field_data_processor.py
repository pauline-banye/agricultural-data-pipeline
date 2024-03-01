"""
Module: field_data_processor.py
Description: This module contains the FieldDataProcessor class, which processes field data based on provided configuration parameters.
Author: Pauline Banye
Version: 1.0
Date: 25th February 2024

Classes:
- FieldDataProcessor: A class for processing field data.

Functions:
- initialize_logging: Initializes logging for the FieldDataProcessor instance.
- ingest_sql_data: Retrieves data from the SQL database and stores it in a DataFrame.
- rename_columns: Renames columns in the DataFrame based on the provided configuration.
- apply_corrections: Applies corrections to specific columns in the DataFrame.
- weather_station_mapping: Maps weather station data to the DataFrame based on field ID.
- process: Orchestrates the data processing workflow.
"""

import logging
import pandas as pd
from scripts.data_ingestion import create_db_engine, query_data, read_from_web_CSV

class FieldDataProcessor:
    """
    The FieldDataProcessor class processes field data based on provided configuration parameters.

    Parameters:
    - config_params (dict): A dictionary containing configuration parameters including:
        - 'db_path': str, the path to the database
        - 'sql_query': str, the SQL query to retrieve data from the database
        - 'columns_to_rename': dict, a dictionary specifying columns to rename
        - 'values_to_rename': dict, a dictionary specifying values to rename
        - 'weather_mapping_csv': str, the URL of the weather data mapping CSV
    - logging_level (str): The logging level for the class, default is "INFO".

    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes a new instance of the FieldDataProcessor class.

        Args:
        - self: The instance of the FieldDataProcessor class.
        - config_params (dict): A dictionary containing configuration parameters including:
            - 'db_path': str, the path to the database
            - 'sql_query': str, the SQL query to retrieve data from the database
            - 'columns_to_rename': dict, a dictionary specifying columns to rename
            - 'values_to_rename': dict, a dictionary specifying values to rename
            - 'weather_mapping_csv': str, the URL of the weather data mapping CSV
        - logging_level (str): The logging level for the class. Defaults to "INFO".

        Returns:
        - None
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']

        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Initializes logging for the FieldDataProcessor instance.

        Args:
        - logging_level (str): The desired logging level. It can be one of {"DEBUG", "INFO", "NONE"}.
          Defaults to "INFO".

        Returns:
        - None
        """
         
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  

        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  

        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()  
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Retrieves data from the SQL database and stores it in a DataFrame.

        Args:
        - self: The instance of the FieldDataProcessor class.

        Returns:
        - pandas.DataFrame: The DataFrame containing the retrieved data.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info("Successfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Renames columns in the DataFrame based on the provided configuration.

        Args:
        - self: The instance of the FieldDataProcessor class.

        Returns:
        - None
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
        
        self.logger.info(f"Swapped columns: {column1} with {column2}")

        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specific columns in the DataFrame.

        Args:
        - self: The instance of the FieldDataProcessor class.
        - column_name (str): The name of the column to apply corrections to. Defaults to 'Crop_type'.
        - abs_column (str): The name of the column for which absolute values will be taken. Defaults to 'Elevation'.

        Returns:
        - None
        """
        self.df['Elevation'] = self.df['Elevation'].abs()
        self.df['Crop_type'] = self.df['Crop_type'].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        """
        Maps weather station data to the DataFrame based on field ID.

        Args:
        - self: The instance of the FieldDataProcessor class.

        Returns:
        - pandas.DataFrame: The DataFrame with mapped weather station data.

        """
        weather_mapping_df = self.df.merge(pd.read_csv(self.weather_map_data), on='Field_ID', how='left').drop(columns=['Unnamed: 0'])
        self.df['Weather_station'] = weather_mapping_df['Weather_station']
        return read_from_web_CSV(self.weather_map_data)

    def process(self):
        """
        Processes the data by ingesting SQL data, renaming columns, applying corrections, and mapping weather stations.

        Args:
        - self: The instance of the FieldDataProcessor class.

        Returns:
        - None
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
