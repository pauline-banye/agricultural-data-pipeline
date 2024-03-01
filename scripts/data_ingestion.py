"""
Module: data_ingestion.py
Description: This module contains functions for data ingestion including creating database engines, querying data from databases, and reading data from web-based CSV files.
Author: Pauline Banye
Version: 1.0
Date: 25th February 2024

Functions:
- create_db_engine: Creates a SQLAlchemy database engine.
- query_data: Queries data from a database using the provided engine and SQL query.
- read_from_web_CSV: Reads data from a web-based CSV file.

"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_db_engine(db_path):
    """
    Create a SQLAlchemy database engine.

    Args:
    - db_path (str): The path to the SQLite database.

    Returns:
    - engine: A SQLAlchemy engine object.

    Example:
    - db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except ImportError as e:
        # If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:
        # If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e

def query_data(engine, sql_query):
    """
    Query data from a database using the provided engine and SQL query.

    Args:
    - engine: A SQLAlchemy engine object.
    - sql_query (str): The SQL query to execute.

    Returns:
    - df: A pandas DataFrame containing the queried data.

    Example:
    - sql_query = '''
        SELECT *
        FROM geographic_features
        LEFT JOIN weather_features USING (Field_ID)
        LEFT JOIN soil_and_crop_features USING (Field_ID)
        LEFT JOIN farm_management_features USING (Field_ID)
        '''
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e

def read_from_web_CSV(URL):
    """
    Read data from a web-based CSV file.

    Args:
    - URL (str): The URL of the CSV file.

    Returns:
    - df: A pandas DataFrame containing the data read from the CSV.

    Example:
    - weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
    """
    try:
        df = pd.read_csv(URL)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e