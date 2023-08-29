import os
import logging
import requests
import urllib.parse

import pandas as pd
from ..common import utils
from dotenv import load_dotenv

load_dotenv()

# Global variables
API_KEY = os.getenv("WEATHER_DATA_API_KEY")
START_DATE = "2022-08-01"
END_DATE = "2023-06-01"
BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
TIME = f"{START_DATE}/{END_DATE}"
PARAMS = f"?unitGroup=metric&key={API_KEY}&contentType=json"

# Create logger
logger = logging.getLogger()


def get_cities(city_csv_path):
    """
    Fetches unique cities from a CSV file where football matches were played.

    This function reads a CSV file specified by the input path, extracting unique city names 
    where football matches were played. Subsequently, it URL-encodes each city name to handle 
    any special characters and returns the URL-encoded city names in a set.

    Parameters:
    city_csv_path (str): Path to the directory where 'pl_teams.csv' is located.

    Returns:
    set: A set of URL-encoded city names where football matches were played.
    """
    logging.info("Getting all the cities where football matches were played")
    unique_cities = set(pd.read_csv(city_csv_path)["city"])

    logging.info(f"Found {len(unique_cities)} cities")

    #URL encode the cities
    return {urllib.parse.quote(city) for city in unique_cities}


def collect_data(city_data, city):
    """
    Collects specific weather data for a given city.

    Parameters:
    city_data (DataFrame): A pandas DataFrame containing the city data.
    city (str): The URL-encoded city name.

    Returns:
    DataFrame: A pandas DataFrame with the specific columns and the decoded city name.
    """
    required_cols = ["datetime", "temp", "humidity", "precip", "windspeed"]
    city_data_df = pd.DataFrame(city_data)
    city_data_df = city_data_df[required_cols]
    city_data_df.loc[:, 'city'] = urllib.parse.unquote(city)

    return city_data_df

def extract_weather():
    """
    Extracts weather data for all cities from a specific API and saves it to a CSV file.

    The function does not take any parameters and does not return any value.
    """
    city_dfs = []
    csv_file_path = "./resources/data/city_weather.csv"
    city_csv_path = "./resources/data/pl_teams.csv"

    # get all the cities where matches were played
    cities = get_cities(city_csv_path)

    # for each city get the weather data and add the df into a list
    for city in cities:
        logging.info(f"Extracting weather for {urllib.parse.unquote(city)} from {START_DATE} to {END_DATE}")
        BASE_URL = f"{BASE}/{city}/{TIME}?{PARAMS}"

        response = requests.get(BASE_URL)
        city_data = collect_data(response.json()['days'], city)
        city_dfs.append(city_data)
    
    # create a df containing weather data of all cities
    all_cities_weather_data = pd.concat(city_dfs).reset_index(drop=True)

    logging.info(f"Saving {len(all_cities_weather_data)} record(s) as csv")
    utils.save_dataframe_to_csv(all_cities_weather_data, csv_file_path)


def extract():
    extract_weather()
