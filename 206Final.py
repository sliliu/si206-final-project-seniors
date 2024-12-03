# Name: Nikola Bogoevich
# Uniquename: Nikobogo
# Email: Nikobogo@umich.edu

import requests
import sqlite3
from time import sleep

# Your API key here
API_KEY = "8thV0vyCOvX0BUKPqpBnAYNYfrDsURZdV1eVF/st5yidAwOFx7qUChQiUMk/f9m8"

# Base URL for the API
BASE_URL = "https://api.collegefootballdata.com"

# Headers with API key for authentication
HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

def create_database():
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()
    
    # Create GameResults table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS GameResults (
        gameID INTEGER PRIMARY KEY,
        date TEXT,
        home_away TEXT,
        opponent TEXT,
        total_points INTEGER
    )
    ''')

    # Create GameStats table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS GameStats (
        gameID INTEGER,
        rushingAttempts INTEGER,
        completionAttempts INTEGER,
        C_ATT TEXT,
        rushingYards INTEGER,
        passingYards INTEGER,
        FOREIGN KEY (gameID) REFERENCES GameResults(gameID)
    )
    ''')

    conn.commit()
    conn.close()

    # Function to insert game results
def insert_game_results(gameID, date, home_away, opponent, total_points):
    """Insert game results into the database, avoiding duplicates."""
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # Check if the game result already exists
    cursor.execute('SELECT gameID FROM GameResults WHERE gameID = ?', (gameID,))
    if cursor.fetchone():
        conn.close()
        return  # Avoid duplicates
    
    # Insert the new game result into the GameResults table
    cursor.execute('''
    INSERT INTO GameResults (gameID, date, home_away, opponent, total_points)
    VALUES (?, ?, ?, ?, ?)
    ''', (gameID, date, home_away, opponent, total_points))
    
    conn.commit()
    conn.close()

# Function to insert game stats
def insert_game_stats(gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards):
    """Insert game stats into the database, avoiding duplicates."""
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # Check if the game stats for the specific game already exist
    cursor.execute('SELECT gameID FROM GameStats WHERE gameID = ?', (gameID,))
    if cursor.fetchone():
        conn.close()
        return  # Avoid duplicates
    
    # Insert the new game stats into the GameStats table
    cursor.execute('''
    INSERT INTO GameStats (gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards))

    conn.commit()
    conn.close()


def fetch_data(endpoint, params=None):
    """
    Fetch data from the College Football API.
    :param endpoint: API endpoint (e.g., '/games', '/teams')
    :param params: Dictionary of query parameters
    :return: Response JSON or error message
    """
    try:
        url = f"{BASE_URL}{endpoint}"
        headers = {
            "Authorization": f"Bearer {API_KEY}"
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None


def get_michigan_game_results(year):
    endpoint = "/games"
    params = {"year": year, "team": "Michigan"}
    data = fetch_data(endpoint, params)  # Fetch the data from the endpoint
    
    if data:
        for game in data:
            game_id = game.get('id')
            date = game.get('start_date')
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            home_points = game.get('home_points')
            away_points = game.get('away_points')

            # Determine if the game is home or away for Michigan
            if home_team == 'Michigan':
                home_away = 'Home'
                opponent = away_team
                total_points = home_points
            else:
                home_away = 'Away'
                opponent = home_team
                total_points = away_points

            # Insert the game stats into the database
            insert_game_results(game_id, date.split('T')[0], home_away, opponent, total_points)


def get_michigan_team_results(year):
    """
    Fetch Michigan game results for a given year, extract stats.
    :param year: The year for which to fetch the game results
    """
    endpoint = "/games/teams"
    params = {"year": year, "seasonType": "regular", "team": "Michigan"}
    data = fetch_data(endpoint, params)  # Fetch the data from the endpoint
    
    if data:
        for game in data:
            game_id = game.get('id')

            # Find Michigan's team data
            michigan_data = next((team for team in game['teams'] if team['school'] == 'Michigan'), None)
            if michigan_data:
                # Extract the relevant stats
                rushing_attempts = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'rushingAttempts'), None)
                completion_attempts = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'completionAttempts'), None)
                rushing_yards = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'rushingYards'), None)
                passing_yards = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'netPassingYards'), None)
                cAtt = completion_attempts
                passAttempts = completion_attempts.split('-')[1]
                insert_game_stats(game_id, rushing_attempts, passAttempts, cAtt, rushing_yards, passing_yards)

def get_percentage_for_wind_speed():
    """
    Retrieve and calculate the percentage of rushing attempts relative to completion attempts
    for each game, along with game details.
    """
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()
    
    # SQL query to join GameResults and GameStats and retrieve required columns
    query = '''
    SELECT 
        gr.gameID, 
        gr.date, 
        gr.home_away, 
        gr.opponent, 
        gs.rushingAttempts, 
        gs.completionAttempts
    FROM 
        GameResults AS gr
    JOIN 
        GameStats AS gs 
    ON 
        gr.gameID = gs.gameID
    '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Process and calculate percentage
    results = {}

    for row in rows:
        gameID, date, home_away, opponent, rushingAttempts, completionAttempts = row

        if rushingAttempts is not None and completionAttempts not in (None, 0):
            total_attempts = rushingAttempts + completionAttempts
            passPercentage = (completionAttempts / total_attempts) * 100
            rushPercentage = (rushingAttempts / total_attempts) * 100
        else:
            passPercentage = 0 
            rushPercentage = 0

        results[date] = [passPercentage, rushPercentage, home_away, opponent, gameID]
    
    #Write results to a file
    with open('rushing_percentage.txt', 'w') as file:
        file.write("-------------Rushing and passing percentage based on wind speed-------------")
        file.write('\n')
        file.write("Date\tGame ID\tHome/Away\tOpponent\tPass Percentage\tRush Percentage\n")
        
        # Iterate through the dictionary
        for date, values in results.items():
            passPercentage, rushPercentage, home_away, opponent, gameID = values
            # Write each entry in the desired format
            file.write(f"{date}\t{gameID}\t{home_away}\t{opponent}\t"
                    f"{passPercentage:.2f}%\t{rushPercentage:.2f}%\n")
    
    conn.close()

def get_total_points_for_tempurature():
    """
    Retrieve and calculate total points for each game based on temperature,
    along with game details, and write the results to a file.
    """
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to fetch required data from the GameResults table
    query = '''
    SELECT 
        date, 
        gameID, 
        home_away, 
        opponent, 
        total_points
    FROM 
        GameResults
    '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Process the data and store it in a dictionary
    results = {}

    for row in rows:
        date, gameID, home_away, opponent, total_points = row
        results[date] = {
            "Game ID": gameID,
            "Home/Away": home_away,
            "Opponent": opponent,
            "Total Points": total_points
        }
    
    # Write results to a file
    with open('total_points_temperature.txt', 'w') as file:
        file.write("-------------Total Points Based on Temperature-------------\n")
        file.write("Date\tGame ID\tHome/Away\tOpponent\tTotal Points\n")
        
        # Iterate through the dictionary
        for date, values in results.items():
            gameID = values["Game ID"]
            home_away = values["Home/Away"]
            opponent = values["Opponent"]
            total_points = values["Total Points"]
            
            # Write each entry in the desired format
            file.write(f"{date}\t{gameID}\t{home_away}\t{opponent}\t{total_points}\n")
    
    conn.close()

def get_completion_for_wind_chill():
    """
    Retrieve and calculate the completion percentage for each game based on C_ATT,
    along with game details, and write the results to a file.
    """
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to fetch required data from the GameStats table
    query = '''
    SELECT 
        gr.date, 
        gs.C_ATT, 
        gr.home_away, 
        gr.opponent
    FROM 
        GameStats AS gs
    JOIN 
        GameResults AS gr
    ON 
        gs.gameID = gr.gameID
    '''

    cursor.execute(query)
    rows = cursor.fetchall()

    # Process the data and store it in a dictionary
    results = {}

    for row in rows:
        date, c_att, home_away, opponent = row

        if c_att is not None:
            # Split the C_ATT field by '-' and calculate the completion percentage
            try:
                completed, attempted = map(int, c_att.split('-'))
                if attempted != 0:
                    completion_percentage = (completed / attempted) * 100
                else:
                    completion_percentage = 0
            except ValueError:
                completion_percentage = 0
        else:
            completion_percentage = 0

        # Store the results in the dictionary
        results[date] = {
            "Home/Away": home_away,
            "Opponent": opponent,
            "Completion Percentage": completion_percentage
        }

    # Write results to a file
    with open('completion_percentage_wind_chill.txt', 'w') as file:
        file.write("-------------Completion Percentage Based on Wind Chill-------------\n")
        file.write("Date\tHome/Away\tOpponent\tCompletion Percentage\n")

        # Iterate through the dictionary
        for date, values in results.items():
            home_away = values["Home/Away"]
            opponent = values["Opponent"]
            completion_percentage = values["Completion Percentage"]

            # Write each entry in the desired format
            file.write(f"{date}\t{home_away}\t{opponent}\t{completion_percentage:.2f}%\n")

    conn.close()

def main():
    create_database()  # Ensure the database is created

    # Loop through the years 2024 to 2015
    for year in range(2024, 2015, -1):  # 2024 down to 2015
        print(f"Fetching data for {year}...")
        get_michigan_game_results(year)
        get_michigan_team_results(year)
        sleep(1)

    get_percentage_for_wind_speed()
    get_total_points_for_tempurature()
    get_completion_for_wind_chill()
    
    
if __name__ == "__main__":
    main()
