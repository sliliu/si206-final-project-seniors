import requests
import sqlite3
from time import sleep
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

class CollegeFootballData:
    def __init__(self, api_key):
        """
        Initialize the CollegeFootballData class with API key and base URL.
        :param api_key: API key for authentication
        :param base_url: Base URL of the API
        """
        self.api_key = api_key
        self.college_football_base_url = "https://api.collegefootballdata.com"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        self.weather_base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.database = 'michigan_football.db'
        self.games_list = []
        self.teams_list = []
        self.weather_list = []
        self.home_location = (42.2808, 83.7430) 
        self.away_locations = {
            "Rutgers": (40.5018, 74.4479),
            "Ohio State": (40.0061, 83.0283),
            "Michigan State": (42.7251, 84.4791),
            "Iowa": (42.7251, 84.4791),
            "Michigan State": (42.7251, 84.4791),
            "Florida": (29.6465, 82.3533),
            "Purdue": (40.4237, 86.9212),
            "Indiana": (39.1682, 86.5230),
            "Penn State": (40.7982, 77.8599),
            "Maryland": (38.9869, 76.9426),
            "Wisconsin": (43.0753, 89.4081),
            "Notre Dame": (41.7052, 86.2352),
            "Northwestern": (42.0565, 87.6753),
            "Illinois": (40.1020, 88.2272),
            "Minnesota": (44.9740, 93.2277),
            "Nebraska": (40.8202, 96.7005),
            "Washington": (47.6567, 122.3066),
            "Utah": (40.7649, 111.8421),
        }

    def create_database(self):
        """
        Creates the SQLite database with tables for HomeAway and Games.

        Args:
            self: Instance of the class 

        Returns:
            None
        """
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS HomeAway (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT UNIQUE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Games (
                gameID INTEGER PRIMARY KEY,
                date_timestamp INTEGER,
                home_away_id INTEGER,
                total_points INTEGER,
                rushingAttempts INTEGER,
                completionAttempts INTEGER,
                completed INTEGER,
                attempted INTEGER,
                rushingYards INTEGER,
                passingYards INTEGER,
                max_temperature REAL,
                min_temperature REAL,
                mean_temperature REAL,
                precipitation_sum REAL,
                rain_sum REAL,
                snowfall_sum REAL,
                max_wind_speed REAL,
                max_wind_gust REAL,
                FOREIGN KEY (home_away_id) REFERENCES HomeAway (id)
            )
            ''')

            conn.commit()

    def insert_game_data(self, game_data):
        """
        Inserts up to 25 game results and stats into the database, avoiding duplicates.

        Args:
            game_data (list): A list of dictionaries, each containing game results 
                            and stats with keys such as 'gameID', 'date', 
                            'home_away', and various performance metrics.

        Returns:
            None
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        to_insert = []
        for game in game_data:
            if len(to_insert) >= 25:  # Limit to 25 records
                break

            game_id = game['gameID']
            # Check if the record already exists
            cursor.execute('SELECT gameID FROM Games WHERE gameID = ?', (game_id,))
            if not cursor.fetchone():
                to_insert.append(game)

        # Insert the batch of up to 25 records that are not duplicates
        for game in to_insert:
            cursor.execute('INSERT OR IGNORE INTO HomeAway (label) VALUES ("Home")')
            cursor.execute('INSERT OR IGNORE INTO HomeAway (label) VALUES ("Away")')
            
            # Fetch the ID for the `home_away` value
            cursor.execute('SELECT id FROM HomeAway WHERE label = ?', (game['home_away'],))
            home_away_id = cursor.fetchone()[0]
            
            date = game['date']
            date_format = "%Y-%m-%d"
            date_object = datetime.strptime(date, date_format)
            date_object_timestap = date_object.timestamp()
            
            cursor.execute('''
            INSERT INTO Games (gameID, date_timestamp, home_away_id, total_points,
                               rushingAttempts, completionAttempts, completed, attempted, 
                               rushingYards, passingYards, max_temperature, min_temperature, 
                               mean_temperature, precipitation_sum, rain_sum, snowfall_sum,
                               max_wind_speed, max_wind_gust)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                game['gameID'], 
                date_object_timestap, 
                home_away_id, 
                game['total_points'],
                game.get('rushingAttempts'), 
                game.get('completionAttempts'), 
                game.get('completed'), 
                game.get('attempted'), 
                game.get('rushingYards'), 
                game.get('passingYards'),
                game.get("max_temperature"),
                game.get("min_temperature"),
                game.get("mean_temperature"),
                game.get("precipitation_sum"),
                game.get("rain_sum"),
                game.get("snowfall_sum"),
                game.get("max_wind_speed"),
                game.get("max_wind_gust")
            ))
            
        conn.commit() 
        conn.close()

    def fetch_football_data(self, endpoint, params=None):
        """
        Fetches data from the College Football API.

        Args:
            endpoint (str): The API endpoint to query (e.g., "/games").
            params (dict, optional): Query parameters to include in the API request.

        Returns:
            dict: The JSON response from the API if successful.
            None: If the API request fails or encounters an error.
        """
        try:
            url = f"{self.college_football_base_url}{endpoint}"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
        
    def fetch_weather_data(self, latitude, longitude, start_date, end_date):
        """
        Fetches weather data from the Open-Meteo API.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.

        Returns:
            dict: The weather data returned by the API as a JSON object.
            None: If the API request fails.
        """
        url = self.weather_base_url
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max,wind_gusts_10m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "auto"
        }

        # Make the API request
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Weather data fetch failed for {start_date} to {end_date}")

    def get_michigan_game_results(self, year):
        """
        Fetches and stores Michigan game results for a specified year.

        Args:
            year (int): The year for which to fetch Michigan game results.

        Returns:
            list: A list of dictionaries representing Michigan game results, 
                with each dictionary containing keys like 'gameID', 'date', 
                'home_away', 'opponent', and 'total_points'.
        """
        endpoint = "/games"
        params = {"year": year, "team": "Michigan"}
        data = self.fetch_football_data(endpoint, params)
        
        if data:
            for game in data:
                game_id = game.get('id')
                date = game.get('start_date')
                home_team = game.get('home_team')
                away_team = game.get('away_team')
                home_points = game.get('home_points')
                away_points = game.get('away_points')
                
                if home_team == 'Michigan':
                    home_away = 'Home'
                    opponent = away_team
                    total_points = home_points
                else:
                    home_away = 'Away'
                    opponent = home_team
                    total_points = away_points
                
                date = date.split('T')[0]

                self.games_list.append({
                    "gameID": game_id,
                    "date": date,
                    "home_away": home_away,
                    "opponent": opponent,
                    "total_points": total_points,
                })
        
        return self.games_list

    def get_michigan_team_results(self, year):
        """
        Fetches and stores Michigan team statistics for a given year.

        Args:
            year (int): The year for which to fetch Michigan team stats.

        Returns:
            list: A list of dictionaries containing Michigan team stats for each game,
                including keys like 'gameID', 'rushingAttempts', 'completionAttempts',
                'completed', 'attempted', 'rushingYards', and 'passingYards'.
        """
        endpoint = "/games/teams"
        params = {"year": year, "seasonType": "regular", "team": "Michigan"}
        data = self.fetch_football_data(endpoint, params)
        
        if data:
            for game in data:
                game_id = game.get('id')
                michigan_data = next((team for team in game['teams'] if team['school'] == 'Michigan'), None)
                if michigan_data:
                    rushing_attempts = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'rushingAttempts'), None)
                    completion_attempts = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'completionAttempts'), None)
                    rushing_yards = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'rushingYards'), None)
                    passing_yards = next((stat['stat'] for stat in michigan_data['stats'] if stat['category'] == 'netPassingYards'), None)
                    c_att = completion_attempts
                    completed, attempted = map(int, c_att.split('-'))
                    pass_attempts = completion_attempts.split('-')[1]
                    
                self.teams_list.append({
                    "gameID": game_id,
                    "rushingAttempts": rushing_attempts,
                    "completionAttempts": pass_attempts,
                    "completed": completed,
                    "attempted": attempted,
                    "rushingYards": rushing_yards,
                    "passingYards": passing_yards,
                })
                    
        return self.teams_list

    def fetch_michigan_data(self, start_year, end_year):
        """
        Fetches and stores Michigan game data, team stats, and weather data for a range of years.

        Args:
            start_year (int): The starting year of the range.
            end_year (int): The ending year of the range.

        Returns:
            None
        """
        self.create_database()


        for year in range(start_year, end_year - 1, -1):
            print(f"Fetching data for {year}...")
            michigan_game_results = self.get_michigan_game_results(year)
            michigan_team_results = self.get_michigan_team_results(year)
        
        for game in michigan_game_results:
            gameID = game.get('gameID')
            if game['home_away'] == 'Home':
                latitude, longitude = self.home_location
            else:
                latitude, longitude = self.away_locations.get(game['opponent'], (None, None))
                
            game_date = game['date']
            
            if latitude is not None and longitude is not None:
                data = self.fetch_weather_data(
                    latitude=latitude,
                    longitude=longitude,
                    start_date=game_date,
                    end_date=game_date
                )
                
                daily_data = data.get("daily", {})
                dates = daily_data.get("time", [])
                print(dates)
                max_temp = daily_data.get("temperature_2m_max", [])
                min_temp = daily_data.get("temperature_2m_min", [])
                mean_temp = daily_data.get("temperature_2m_mean", [])
                precipitation = daily_data.get("precipitation_sum", [])
                rain = daily_data.get("rain_sum", [])
                snowfall = daily_data.get("snowfall_sum", [])
                max_wind_speed = daily_data.get("wind_speed_10m_max", [])
                max_wind_gust = daily_data.get("wind_gusts_10m_max", [])
                
                for i in range(len(dates)):
                    max_temperature = max_temp[i]
                    min_temperature = min_temp[i]
                    mean_temperature = mean_temp[i]
                    precipitation_sum = precipitation[i]
                    rain_sum = rain[i]
                    snowfall_sum = snowfall[i]
                    max_wind_speed = max_wind_speed[i]
                    max_wind_gust = max_wind_gust[i]
                
                self.weather_list.append({
                    "gameID": gameID,
                    "max_temperature": max_temperature,
                    "min_temperature": min_temperature,
                    "mean_temperature": mean_temperature,
                    "precipitation_sum": precipitation_sum,
                    "rain_sum": rain_sum,
                    "snowfall_sum": snowfall_sum,
                    "max_wind_speed": max_wind_speed,
                    "max_wind_gust": max_wind_gust,
                })
            
        # Combine results and stats into one list
        combined_data = []
        for game_result in michigan_game_results:
            game_id = game_result['gameID']
            game_stats = next((stats for stats in michigan_team_results if stats['gameID'] == game_id), {})
            weather_data = next((weather for weather in self.weather_list if weather['gameID'] == game_id), {})
            combined_data.append({**game_result, **game_stats, **weather_data})
        
        print(combined_data)
        
        sleep(1)
        self.insert_game_data(combined_data)

def main():
    """
    The main function for the program.

    Args:
        None

    Returns:
        None
    """
    football_api_key = "8thV0vyCOvX0BUKPqpBnAYNYfrDsURZdV1eVF/st5yidAwOFx7qUChQiUMk/f9m8"    
    football_data = CollegeFootballData(football_api_key)
    football_data.fetch_michigan_data(2024, 2015)

if __name__ == "__main__":
    main()
