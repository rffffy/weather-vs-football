import pandas as pd

MULTI_WORD_CITIES = {"Tyne": "Newcastle upon Tyne", "Hove": "Brighton & Hove"}


def extract_city(address):
    """
    Extracts the city name from the given address.

    This function takes an address and extracts the city name from it. 
    It returns the city name after processing any special cases defined in `MULTI_WORD_CITIES`.

    Parameters:
        address (str): The address string.

    Returns:
        str: The extracted city name.
    """
    city = address.split()[-3]

    if city in MULTI_WORD_CITIES:
        return MULTI_WORD_CITIES[city]
    
    return city


def extract_team_data(teams):
    """
    Extracts specific information from a list of team data.

    This function takes a list of team data and extracts specific information (id, name, shortName, address)
    for each team, returning a list of dictionaries containing the extracted data.

    Parameters:
        teams (dict): A dictionary containing team information.

    Returns:
        list: A list of dictionaries containing specific team information.
    """

    return [
        {
            "id": team["id"], 
            "name": team["name"],
            "shortName": team["shortName"],
            "address": team["address"]
        } 
        for team in teams["teams"]
    ]


def get_date(datetime_str):
    """
    Function to extract the date from a datetime string in 'YYYY-MM-DDTHH:MM:SSZ' format.

    Parameters:
    datetime_str (str): The datetime string.

    Returns:
    str: The date extracted from the datetime string.
    """
    return datetime_str.split('T')[0]


def extract_match_data(match):
    """
    Extracts specific information from a match data dictionary.

    This function takes a dictionary containing match data and extracts specific information 
    (id, date, status, home_team_id, home_team_name, away_team_id, away_team_name, winner_name,
    ft_score_away_team, ft_score_home_team) from the dictionary.

    Parameters:
        match (dict): A dictionary containing match data.

    Returns:
        dict: A dictionary containing specific match information.
    """
    match_dict = {}

    match_dict["id"] = match["id"]
    match_dict["date"] = get_date(match["utcDate"])
    match_dict["status"] = match["status"]

    home_team = match["homeTeam"]
    match_dict["home_team_id"] = home_team["id"]
    match_dict["home_team_name"] = home_team["name"]

    away_team = match["awayTeam"]
    match_dict["away_team_id"] = away_team["id"]
    match_dict["away_team_name"] = away_team["name"]

    score = match["score"]
    winner_mapping = {
    "AWAY_TEAM": away_team["name"],
    "HOME_TEAM": home_team["name"]
    }
    match_dict["winner_name"] = winner_mapping.get(score["winner"], score["winner"])
    match_dict["ft_score_away_team"] = score["fullTime"]["away"]
    match_dict["ft_score_home_team"] = score["fullTime"]["home"]

    return match_dict


def format_pl_teams(teams):
    """
    Formats Premier League teams data.

    This function takes a list of team data and extracts relevant information for each team.
    It then returns a DataFrame containing the formatted team data.

    Parameters:
        teams (list): A list of team data containing information about each team.

    Returns:
        pandas.DataFrame: A DataFrame containing formatted team data.
    """
    pl_teams = extract_team_data(teams)
    pl_teams_df = pd.DataFrame(pl_teams)

    pl_teams_df["city"] = pl_teams_df["address"].apply(extract_city)

    return pl_teams_df


def format_pl_season_matches(matches):
    """
    Formats Premier League season match data.

    This function takes a list of match data and extracts relevant information for each match.
    It then returns a DataFrame containing the formatted match data.

    Parameters:
        matches (list): A list of match data containing information about each match.

    Returns:
        pandas.DataFrame: A DataFrame containing formatted match data.
    """
    matches_data = [extract_match_data(match) for match in matches]

    return pd.DataFrame(matches_data)