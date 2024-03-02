# Agricultural Data Pipeline

> ## Table of Contents
- [Overview](#overview)
- [Data Dictionary](#data-dictionary)
- [Requirements](#requirements)
- [Setup the Project](#setup-the-project)
    - [Clone the repository](#1-clone-the-repository)
    - [Set Up Anaconda on Windows 10](#2-set-up-anaconda-on-windows-10)
    - [Setup your IDE](#3-setup-your-ide)
- [Building our Data Pipeline](#building-our-data-pipeline)
  - [Data Ingestion](#data-ingestion)
  - [Field data processor](#field-data-processor)
  - [Weather data processor](#weather-data-processor)
- [Testing](#testing)
- [Contributing to the project](#contributing-to-the-project)

#
> ## Overview
<p align="justify">
This project focuses on validating agricultural data by building a data pipeline for ingesting, cleaning, and validating agricultural data. The goal is to enhance the efficiency and accuracy of data validation processes in agricultural datasets. By building a modular pipeline, we can ensure the code is reusable, maintainable, and easier to debug.

There are two notebooks in this project. The `Integrated_project_P3_Validating_our_data_student.ipynb` consisting of the step-by-step method and thought process involving building the pipeline, tests and analysis, and the `Validating_our_data.ipynb` which contains only the completed pipeline, tests, and analysis.
</p>

#

> ## Data Dictionary
The project involves the following data features:

### Geographic Features
- **`Field_ID:`** Unique identifier for each field (BigInt).
- **`Elevation:`** Elevation of the field above sea level in meters (Float).
- **`Latitude:`** Geographical latitude of the field in degrees (Float).
- **`Longitude:`** Geographical longitude of the field in degrees (Float).
- **`Location:`** Province where the field is located (Text).
- **`Slope:`** Slope of the land in the field (Float).

### Weather Features
- **`Field_ID:`** Corresponding field identifier (BigInt).
- **`Rainfall:`** Amount of rainfall in the area in millimeters (Float).
- **`Min_temperature_C:`** Average minimum temperature recorded in Celsius (Float).
- **`Max_temperature_C:`** Average maximum temperature recorded in Celsius (Float).
- **`Ave_temps:`** Average temperature in Celsius (Float).

### Soil and Crop Features
- **`Field_ID:`** Corresponding field identifier (BigInt).
- **`Soil_fertility:`** Measure of soil fertility where 0 is infertile soil, and 1 is very fertile soil (Float).
- **`Soil_type:`** Type of soil present in the field (Text).
- **`pH:`** pH level of the soil, indicating soil acidity/basicity (Float).

### Farm Management Features
- **`Field_ID:`** Corresponding field identifier (BigInt).
- **`Pollution_level:`** Level of pollution in the area where 0 is unpolluted and 1 is very polluted (Float).
- **`Plot_size:`** Size of the plot in the field (hectares) (Float).
- **`Chosen_crop:`** Type of crop chosen for cultivation (Text).
- **`Annual_yield:`** Annual yield from the field (Float).
- **`Standard_yield:`** Standardized yield expected from the field, normalized per crop (Float).

### Weather Station Data (CSV)
- **`Weather_station_ID:`** ID of the weather station where the data originated (Int).
- **`Message:`** Weather data captured by sensors at the stations, in text message format (Str).

### Weather Data Field Mapping (CSV)
- **`Field_ID:`** ID of the field connected to a weather station (Int).
- **`Weather_station_ID:`** ID of the weather station connected to a field. If a field has `weather_station_ID = 0`, it's closest to weather station 0 (Int).

#

> ## Requirements
To run the code in this project, you need the following dependencies:
| <b><u>Dependency</u></b> | <b><u>Usage</u></b> |
| :------------------ | :------------------ |
| **`pandas`**      | Manipulates and analyzes data in tables      |
| **`numpy`**      | Performs numerical computations and works with arrays.            |
| **`sqlalchemy`**      | Connects to and interacts with relational databases.            |
| **`matplotlib`**      | Creates various static, animated, and interactive visualizations.            |
| **`seaborn`**      | Creates statistical data visualizations built on top of matplotlib.            |
| **`Pyarrow`**      | Handles large datasets efficiently with columnar data structures.            |

#
> ## Setup the Project
**`*Note:`**

<p align="justify">
This project was setup on a windows 10 system using the anaconda prompt terminal. Some of the commands used may not work with the VScode terminal, command prompt or powershell.

You would also require an IDE such as jupyter notebook or VScode. The steps involved are outlined below:-
</p>

### **1. Clone the repository**
- **Open a terminal or command prompt:**
    * Search for "Command Prompt" or "Terminal" in the Start menu and launch it.

- **Navigate to your desired local directory:**
    * Use the `cd` command to navigate to the directory where you want to clone the repository locally.

- **Clone the repository using the URL:**
    * Type the following command

      ```bash
      git clone https://github.com/pauline-banye/agricultural-data-pipeline.git
      ```
- Once completed, you should see a new folder with the repository name in your chosen local directory. 


### **2. Set Up Anaconda on Windows 10**

- Go to the Anaconda website: [https://www.anaconda.com/download](https://www.anaconda.com/download)
    * Choose Python 3.x graphical installer.
    * Download the appropriate installer for your system (32-bit or 64-bit).
- Run the Installer
    * Double-click the downloaded installer.
    * Click "Next" and follow the on-screen instructions.
- Open Anaconda prompt
    * `Note`: Anaconda Prompt is already installed alongside Anaconda on Windows 10.
- Create and activate a new conda environment
    
    ```bash
    conda create -n <environment_name>

    conda activate <environment_name>
    ```
    Alternatively, you can create a new conda environment using the environment.yml file

    ```bash
    conda env create -f <environment_file.yml>
    ```
- Install the required dependencies
    * To add a new package to your already existing conda environment, execute the `conda install` command i.e.

    ```bash
    conda install numpy
    ```
### **3. Setup your IDE**
**`*Note:`**
You can use either jupyter note book or Visual Studio code (or both).

- `Jupyter notebook`
    * To install jupyter notebook in your conda environment, open your anaconda prompt terminal and execute the following command.

    ```bash
    conda install jupyter
    ```
    * Launch the jupyter notebook by running the `jupyter notebook` command

 - `Visual Studio Code`

    * Download the installer from [https://code.visualstudio.com/download](https://code.visualstudio.com/download)
    * Install the downloaded .exe file and follow the on-screen instructions.
#


> ## Building our Data Pipeline
<p align="justify">
The project involves refining the data pipeline to ensure efficiency and accuracy. It is focused on building a data pipeline capable of ingesting and cleaning data with minimal manual intervention. The process includes:
</p>

- Data ingestion from databases and CSV files.
- Transformations and cleanup of field-related data.
- Processing and cleanup of weather station data.
- Automated data validation checks.

The data pipeline is organized into modular components to enhance code readability, maintainability, and extensibility. The main modules include:
</p>

### **`Data Ingestion`**
<p align="justify">
This module handles data retrieval from various sources, including:
</p>

- **create_db_engine:** Creates a database engine for interacting with a database.
- **query_data:** Executes a SQL query and returns the resulting DataFrame.
- **read_csv_from_url:** Reads a CSV file from a URL and returns the DataFrame.
#
### **`Field data processor`**
<p align="justify">

This module provides a `FieldDataProcessor` class for cleaning and processing field data based on configuration parameters, including:
</p>

- **Data Ingestion:** Reads data from a SQL database using provided configuration parameters.
- **Column Renaming:** Renames columns in the DataFrame based on a mapping dictionary.
- **Data Correction:** Applies specific corrections to designated columns.
- **Weather Station Mapping:** Merges weather station data with the field data based on a mapping file.
- **Process Orchestration:** Provides a `process` method that executes the entire data processing workflow.
#
### **`Weather data processor`**
<p align="justify">

This module provides the `WeatherDataProcessor` class for processing weather data, which handles various weather data processing tasks, including:
</p>

- **Data Ingestion:** Reads weather station data from a URL using configuration parameters.
- **Message Processing:** Extracts measurements from weather station messages using regular expressions.
- **Mean Calculation:** Calculates the mean of each measurement type grouped by weather station.
- **Process:** The main function that performs cleaning and transformation steps on the weather data. It provides a `process` method that executes the entire data processing workflow.

These modules encapsulate specific functionalities, making the code more organized and easier to understand.

#
> ## Testing
<p align="justify">
After building the pipeline, we need to to validate the output of the pipeline. That is where testing comes in.

The `validate_data.py` script uses the `unittest` framework to perform several unit tests on the `field_data_processor.py` and `weather_data_processor.py` dataframes. 
</p>

**Some of the tests included are:**
- Ensure both weather and field DataFrames have the expected number of rows and columns after processing.
- Verify  that the weather DataFrame and field DataFrame has the expected columns after processing.
- Confirm that all crop types in the field DataFrame are present in the valid list (`cassava`, `tea`, `wheat`, etc.).
- Ensures that all elevation values in the field and weather DataFrames are non-negative.
- Verify that both weather and field DataFrames are not empty after processing.

#
> ## Conclusion

By building a modular data pipeline, we achieve several benefits:

* **Improved code organization:** The code is more structured and easier to navigate.
* **Enhanced maintainability:** Modifying or extending functionalities becomes easier within specific modules.
* **Increased reusability:** Modules can be reused in other projects with similar data processing needs.

This approach ensures a clean, maintainable, and scalable data pipeline for our agricultural data analysis.

#
> ## Contributing to the project
<p align="justify">
If you find something worth contributing, please fork the repo, make a pull request and add valid and well-reasoned explanations about your changes or comments.
</p>

Before adding a pull request, please note:

- This is an open source project.
- Your contributions should be inviting and clear.
- Any additions should be relevant.
- New features should be easy to contribute to.

All **`suggestions`** are welcome!
#


<!-- git commit -am "message" && git push origin branch_name -->
##### README Created by `pauline-banye`
