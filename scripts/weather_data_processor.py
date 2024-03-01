"""
Module: weather_data_processor.py
Description: This module contains classes and functions for processing weather data.
Author: Pauline Banye
Version: 1.0
Date: 25th February 2024

Classes:
- WeatherDataProcessor: A class for processing weather data including mapping weather station data, extracting measurements from messages, processing messages, calculating means, and orchestrating the overall process.

Functions:
- initialize_logging: Initializes logging for the WeatherDataProcessor class.
- weather_station_mapping: Maps weather station data.
- extract_measurement: Extracts measurements from messages.
- process_messages: Processes messages to extract measurements.
- calculate_means: Calculates means of measurements.
- process: Orchestrates the overall data processing.
"""

import re
import numpy as np
import pandas as pd
import logging
from scripts.data_ingestion import read_from_web_CSV


class WeatherDataProcessor:
    """
    The WeatherDataProcessor class processes weather data based on provided configuration parameters.

    Parameters:
    - config_params (dict): A dictionary containing configuration parameters including:
        - 'weather_csv_path': str, the URL or path of the weather station data CSV
        - 'regex_patterns': dict, a dictionary containing regular expressions for message extraction

    - logging_level (str): The logging level for the class, default is "INFO".

    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes a new instance of the WeatherDataProcessor class.

        Args:
        - self: The instance of the WeatherDataProcessor class.
        - config_params (dict): A dictionary containing configuration parameters including:
            - 'weather_csv_path': str, the URL or path of the weather station data CSV
            - 'regex_patterns': dict, a dictionary containing regular expressions for message extraction
        - logging_level (str): The logging level for the class. Defaults to "INFO".

        Returns:
        - None
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  
        self.initialize_logging(logging_level)


    def initialize_logging(self, logging_level):
        """
        Initializes logging for this instance of WeatherDataProcessor.

        Args:
        - self: The instance of the WeatherDataProcessor class.
        - logging_level (str): The desired logging level. It can be one of {"DEBUG", "INFO", "NONE"}.
          Defaults to "INFO".

        Returns:
        - None
        """
        logger_name = __name__ + ".WeatherDataProcessor"
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


    def weather_station_mapping(self):
        """
        Loads weather station data from the web and assigns it to the weather_df attribute.

        Args:
        - self: The instance of the WeatherDataProcessor class.

        Returns:
        - None
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.")


    def extract_measurement(self, message):
        """
        Extracts measurement from a given message using regex patterns.

        Args:
        - self: The instance of the WeatherDataProcessor class.
        - message (str): The message from which to extract the measurement.

        Returns:
        - tuple: A tuple containing the measurement key and its value.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None


    def process_messages(self):
        """
        Processes messages in the weather_df to extract measurements.

        Args:
        - self: The instance of the WeatherDataProcessor class.

        Returns:
        - pandas.DataFrame: The DataFrame with extracted measurements.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df


    def calculate_means(self):
        """
        Calculates mean values of measurements grouped by weather station ID.

        Args:
        - self: The instance of the WeatherDataProcessor class.

        Returns:
        - pandas.DataFrame or None: The DataFrame with mean values if weather_df is initialized, otherwise None.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None


    def process(self):
        """
        Processes the data by loading weather station data, processing messages, and calculating means.

        Args:
        - self: The instance of the WeatherDataProcessor class.

        Returns:
        - None
        """
        self.weather_station_mapping()  
        self.process_messages()  
        self.logger.info("Data processing completed.")
