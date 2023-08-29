import requests
import logging
import os

from . import pl_utils
from ..common import utils
from dotenv import load_dotenv

load_dotenv()

#Global variables
API_KEY = os.getenv("FOOTBALL_DATA_API_TOKEN")
BASE_URL = "https://api.football-data.org/v4"
PL_TEAM_URL = f"{BASE_URL}/competitions/PL/teams?season=2022"
PL_SEASON_URL = f"{BASE_URL}/competitions/PL/matches?season=2022"
HEADERS = {"X-Auth-Token": f"{API_KEY}"} #TODO: get from env variable

#Configurations
# Create logger
logger = logging.getLogger()


def extract_teams():
    """
    Extracts and saves Premier League teams' information for the 2022/2023 season to a CSV file.

    This function requests team information from the Premier League API and saves the formatted data
    to a CSV file for further processing.

    Returns:
    None
    """    
    csv_file_path = "./resources/data/pl_teams.csv"
    
    logging.info("Requesting info for teams participating in Premier League Season 2022/2023")
    team_response = requests.get(PL_TEAM_URL, headers=HEADERS)

    pl_teams_df = pl_utils.format_pl_teams(team_response.json())

    utils.save_dataframe_to_csv(pl_teams_df, csv_file_path)


def extract_matches():
    """
    Extracts and saves Premier League season matches information for the 2022/2023 season to a CSV file.

    This function requests match information from the Premier League API and saves the formatted data
    to a CSV file for further processing.

    Returns:
    None
    """
    csv_file_path = "./resources/data/pl_season.csv"

    logging.info("Requesting info for matches played in Premier League Season 2022/2023")
    season_response = requests.get(PL_SEASON_URL, headers=HEADERS)

    pl_season_matches_df = pl_utils.format_pl_season_matches(
        season_response.json()["matches"])
    
    utils.save_dataframe_to_csv(pl_season_matches_df, csv_file_path)


def extract():
    extract_teams()
    extract_matches()