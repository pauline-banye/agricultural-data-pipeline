import unittest
from scripts.field_data_processor import FieldDataProcessor
from scripts.weather_data_processor import WeatherDataProcessor

# define the parameters
config_params = {
    "sql_query": """
        SELECT *
        FROM geographic_features
        LEFT JOIN weather_features USING (Field_ID)
        LEFT JOIN soil_and_crop_features USING (Field_ID)
        LEFT JOIN farm_management_features USING (Field_ID)
    """,
    "db_path": 'sqlite:///database/Maji_Ndogo_farm_survey_small.db',
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'},
    "values_to_rename": {'cassaval': 'cassava', 'wheatn': 'wheat', 'teaa': 'tea'},
    "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv",
    "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv",
    "regex_patterns": {
        'Rainfall': r'(\d+(\.\d+)?)\s?mm',
        'Temperature': r'(\d+(\.\d+)?)\s?C',
        'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    }
}

# 1. test that both dataframes have the correct shape
def test_read_weather_DataFrame_shape():
    """
    Test that the processed dataframe has the correct shape.
    """
    weather_processor = WeatherDataProcessor(config_params)
    weather_processor.process()
    weather_df = weather_processor.weather_df
    num_rows, num_cols = weather_df.shape

    # Assert that the number of rows and columns are as expected
    assert num_rows == weather_df.shape[0], f"Number of rows should be {weather_df.shape[0]}"
    assert num_cols == weather_df.shape[1], f"Number of rows should be {weather_df.shape[1]}"


def test_read_field_DataFrame_shape():
    """
    Test that the processed dataframe has the correct shape.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df
    num_rows, num_cols = field_df.shape

    # Assert that the number of rows and columns are as expected
    assert num_rows == field_df.shape[0], f"Number of rows should be {field_df.shape[0]}"
    assert num_cols == field_df.shape[1], f"Number of rows should be {field_df.shape[1]}"


# 2. test that both dataframes have the expected columns
def test_weather_dataframe_columns():
    """
    Test that the weather DataFrame has the expected columns.
    """
    weather_processor = WeatherDataProcessor(config_params)
    weather_processor.process()
    weather_df = weather_processor.weather_df
    expected_columns = [
        'Weather_station_ID', 'Message', 'Measurement', 'Value'
        ]
    
    # Assert that the names of the columns are as expected
    assert list(weather_df.columns) == expected_columns, "Weather DataFrame columns don't match."

def test_field_dataframe_columns():
    """
    Test that the field DataFrame has the expected columns after processing.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df
    expected_columns = [
        'Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location', 'Slope',
        'Rainfall', 'Min_temperature_C', 'Max_temperature_C', 'Ave_temps',
        'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level', 'Plot_size',
        'Annual_yield', 'Crop_type', 'Standard_yield', 'Weather_station'
        ]
    
    # Assert that the names of the columns are as expected
    assert list(field_df.columns) == expected_columns, "Field DataFrame columns don't match."


# 3. test that all crops names are valid
def test_crop_types_are_valid():
    """
    Test that all crop types in the field DataFrame are valid.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df
    valid_crop_types = [
        'cassava', 'tea', 'wheat', 'potato', 'banana', 'coffee', 'rice','maize'
        ]
    
    field_crop_types_lower = field_df['Crop_type'].str.lower().str.strip()
    valid_crop_types_lower = [crop.lower().strip() for crop in valid_crop_types]

    # Assert that the crop names are valid
    assert field_crop_types_lower.isin(valid_crop_types_lower).all(), "Invalid crop types found."


# 4. test for negative values
def test_field_dataframe_non_negative_elevation():
    """
    Test that all elevation values in the field DataFrame are not negative.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df

    # Assert that all values in the elevation column are not negative
    assert (field_df['Elevation'] >= 0).all(), "Elevation values should not be negative."

def test_positive_rainfall_values():
    """
    Test that all rainfall values in the weather DataFrame are not negative.
    """
    weather_processor = WeatherDataProcessor(config_params)
    weather_processor.process()
    weather_df = weather_processor.weather_df
    rainfall_values = weather_df.loc[weather_df['Measurement'] == 'Rainfall', 'Value']

    # assert that all values in the rainfall column are not negative
    assert (rainfall_values >= 0).all(), "Rainfall values should not be negative."


# EXTRA TESTS
# 5. test that the dataframes are not empty
def test_weather_dataframe_not_empty():
    """
    Test that the weather DataFrame is not empty.
    """
    weather_processor = WeatherDataProcessor(config_params)
    weather_processor.process()
    weather_df = weather_processor.weather_df

    # assert that the weather DataFrame is not empty
    assert len(weather_df) > 0, "Weather DataFrame should contain data."

def test_field_dataframe_not_empty():
    """
    Test that the field DataFrame is not empty.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df

    # assert that the field DataFrame is not empty
    assert len(field_df) > 0, "Field DataFrame should contain data."



if __name__ == '__main__':
    unittest.main()
