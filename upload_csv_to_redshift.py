import os
import boto3
import logging
import redshift_connector
import pandas as pd


PL_SEASON_FILE_NAME = "pl_season.csv"
PL_TEAM_FILE_NAME = "pl_teams.csv"
WEATHER_FILE_NAME = "city_weather.csv"

RESOURCES_PATH = "./resources/data"
PL_SEASON_DATA_PATH = f"{RESOURCES_PATH}/{PL_SEASON_FILE_NAME}"
PL_TEAM_DATA_PATH = f"{RESOURCES_PATH}/{PL_TEAM_FILE_NAME}"
WEATHER_DATA_PATH = f"{RESOURCES_PATH}/{WEATHER_FILE_NAME}"

BUCKET_NAME = os.getenv("BUCKET_NAME")
HOST = os.getenv("REDSHIFT_HOST")
PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
DATABASE = os.getenv("REDSHIFT_DATABASE")
USER = os.getenv("REDSHIFT_USER")
PASSWORD = os.getenv("REDSHIFT_PASSWORD")
IAMROLE = os.getenv("IAM_ROLE")

logger = logging.getLogger()


def upload_to_s3(file_path, file_name):
    """
    Uploads a file to an Amazon S3 bucket.

    This function takes a file from a specified file path and uploads it to an Amazon S3 bucket
    under the provided file name.

    Parameters:
        file_path (str): The local path of the file to be uploaded.
        file_name (str): The name to give to the file in the S3 bucket.

    Returns:
        None
    """
    s3 = boto3.client('s3')
    with open(file_path, 'rb') as data:
        s3.upload_fileobj(data, BUCKET_NAME, file_name)

def execute_redshift_query(queries):
    """
    Executes a list of SQL queries on an Amazon Redshift cluster.

    This function connects to a Redshift cluster using the connection parameters set as global variables.
    It then executes a list of SQL queries in the order they are given.

    Parameters:
        queries (list): A list of SQL queries to be executed.

    Returns:
        None
    """
    # Connect to Redshift
    try:
        # Connect to Redshift
        with redshift_connector.connect(
            host=HOST,
            database=DATABASE,
            port=PORT,
            user=USER,
            password=PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                # Create the table
                for query in queries:
                    cursor.execute(query)
                conn.commit()
    
    except redshift_connector.Error as e:
        # Handle the exception
        logger.error(f"Error connecting to Redshift: {str(e)}")
    
    except Exception as e:
        # Handle other exceptions
        logger.error(f"An error occurred: {str(e)}")


def upload_premier_league_season_data():
    """
    Uploads Premier League season data to Amazon Redshift.

    This function uploads a CSV file containing Premier League season data to an S3 bucket, creates a new table
    in Redshift if it does not exist, and then copies the data from the S3 bucket into the Redshift table.

    Parameters:
        None

    Returns:
        None
    """
    # Upload the CSV file to S3
    upload_to_s3(PL_SEASON_DATA_PATH, PL_SEASON_FILE_NAME)

    create_table_query = """
        CREATE TABLE IF NOT EXISTS premier_league_matches(
            id INTEGER PRIMARY KEY,
            date DATE,
            status VARCHAR(10),
            home_team_id INTEGER,
            home_team_name VARCHAR(50),
            away_team_id INTEGER,
            away_team_name VARCHAR(50),
            winner_name VARCHAR(50),
            ft_score_away_team INTEGER,
            ft_score_home_team INTEGER
        );
        """

    copy_query = f"""
    COPY premier_league_matches
    FROM 's3://{BUCKET_NAME}/{PL_SEASON_FILE_NAME}'
    IAM_ROLE '{IAMROLE}'
    CSV
    IGNOREHEADER 1;
    """

    queries = [create_table_query, copy_query]

    execute_redshift_query(queries)


def upload_premier_league_team_data():
    """
    Uploads Premier League team data to Amazon Redshift.

    This function uploads a CSV file containing Premier League team data to an S3 bucket, creates a new table
    in Redshift if it does not exist, and then copies the data from the S3 bucket into the Redshift table.

    Parameters:
        None

    Returns:
        None
    """
    # Upload the CSV file to S3
    upload_to_s3(PL_TEAM_DATA_PATH, PL_TEAM_FILE_NAME)

    create_table_query = """
        CREATE TABLE IF NOT EXISTS premier_league_teams(
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            shortName VARCHAR(50),
            address VARCHAR(200),
            city VARCHAR(100)
        );
        """

    copy_query = f"""
    COPY premier_league_teams
    FROM 's3://{BUCKET_NAME}/{PL_TEAM_FILE_NAME}'
    IAM_ROLE '{IAMROLE}'
    CSV
    IGNOREHEADER 1;
    """

    queries = [create_table_query, copy_query]

    execute_redshift_query(queries)


def upload_weather_data():
    """
    Uploads weather data to Amazon Redshift.

    This function uploads a CSV file containing weather data to an S3 bucket, creates a new table
    in Redshift if it does not exist, and then copies the data from the S3 bucket into the Redshift table.

    Parameters:
        None

    Returns:
        None
    """
    # Upload the CSV file to S3
    upload_to_s3(WEATHER_DATA_PATH, WEATHER_FILE_NAME)

    create_table_query = """
        CREATE TABLE IF NOT EXISTS weather(
            datetime DATE,
            temp FLOAT,
            humidity FLOAT,
            precip FLOAT,
            windspeed FLOAT,
            city VARCHAR(100)
        );
        """

    copy_query = f"""
    COPY weather
    FROM 's3://{BUCKET_NAME}/{WEATHER_FILE_NAME}'
    IAM_ROLE '{IAMROLE}'
    CSV
    IGNOREHEADER 1;
    """

    queries = [create_table_query, copy_query]

    execute_redshift_query(queries)

def main():
    upload_premier_league_season_data()
    upload_premier_league_team_data()
    upload_weather_data()

if __name__ == "__main__":
    main()