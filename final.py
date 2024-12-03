import requests
import sqlite3
from time import sleep
from datetime import datetime, timedelta

class CollegeFootballData:
    def __init__(self, api_key, base_url="https://api.collegefootballdata.com"):
        """
        Initialize the CollegeFootballData class with API key and base URL.
        :param api_key: API key for authentication
        :param base_url: Base URL of the API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        self.database = 'football_stats.db'

    def create_database(self):
        """
        Create the SQLite database with GameResults and GameStats tables.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS GameResults (
            gameID INTEGER PRIMARY KEY,
            date TEXT,
            home_away TEXT,
            opponent TEXT,
            total_points INTEGER
        )
        ''')
        
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

    def insert_game_results(self, gameID, date, home_away, opponent, total_points):
        """
        Insert game results into the database, avoiding duplicates.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        cursor.execute('SELECT gameID FROM GameResults WHERE gameID = ?', (gameID,))
        if cursor.fetchone():
            conn.close()
            return  # Avoid duplicates
        
        cursor.execute('''
        INSERT INTO GameResults (gameID, date, home_away, opponent, total_points)
        VALUES (?, ?, ?, ?, ?)
        ''', (gameID, date, home_away, opponent, total_points))
        
        conn.commit()
        conn.close()

    def insert_game_stats(self, gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards):
        """
        Insert game stats into the database, avoiding duplicates.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        cursor.execute('SELECT gameID FROM GameStats WHERE gameID = ?', (gameID,))
        if cursor.fetchone():
            conn.close()
            return  # Avoid duplicates
        
        cursor.execute('''
        INSERT INTO GameStats (gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (gameID, rushingAttempts, completionAttempts, C_ATT, rushingYards, passingYards))
        
        conn.commit()
        conn.close()

    def fetch_data(self, endpoint, params=None):
        """
        Fetch data from the College Football API.
        :param endpoint: API endpoint
        :param params: Dictionary of query parameters
        :return: Response JSON or error message
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None

    def get_michigan_game_results(self, year):
        """
        Fetch and store Michigan game results for a given year.
        :param year: The year for which to fetch the game results
        """
        endpoint = "/games"
        params = {"year": year, "team": "Michigan"}
        data = self.fetch_data(endpoint, params)
        
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

                self.insert_game_results(game_id, date.split('T')[0], home_away, opponent, total_points)

    def get_michigan_team_results(self, year):
        """
        Fetch and store Michigan team stats for a given year.
        :param year: The year for which to fetch the stats
        """
        endpoint = "/games/teams"
        params = {"year": year, "seasonType": "regular", "team": "Michigan"}
        data = self.fetch_data(endpoint, params)
        
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
                    pass_attempts = completion_attempts.split('-')[1]
                    self.insert_game_stats(game_id, rushing_attempts, pass_attempts, c_att, rushing_yards, passing_yards)

    def fetch_and_store_michigan_data(self, start_year, end_year):
        """
        Fetch and store Michigan data for a range of years.
        :param start_year: The starting year
        :param end_year: The ending year
        """
        self.create_database()
        for year in range(start_year, end_year - 1, -1):
            print(f"Fetching data for {year}...")
            self.get_michigan_game_results(year)
            self.get_michigan_team_results(year)
            sleep(1)

class Weather:
    def __init__(self, db_path="football_stats.db"):
        """Initialize the Weather class with a database path."""
        self.db_path = db_path
        self.create_weather_table()

    def create_weather_table(self):
        """Create the WeatherData table in the database if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WeatherData (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                location TEXT,
                max_temperature REAL,
                min_temperature REAL,
                mean_temperature REAL,
                precipitation_sum REAL,
                rain_sum REAL,
                snowfall_sum REAL,
                max_wind_speed REAL,
                max_wind_gust REAL,
                UNIQUE(date, location) -- Prevent duplicate entries
            )
        """)
        conn.commit()
        conn.close()

    def fetch_and_store_weather_data(self, latitude, longitude, start_date, end_date):
        """
        Fetch weather data from the Open-Meteo API and store it in the database.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
        """
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                     "precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max,wind_gusts_10m_max",
            "temperature_unit": "fahrenheit",
            "timezone": "auto"
        }

        # Make the API request
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            self.store_weather_data(latitude, longitude, data)
        else:
            print(f"Error: Failed to fetch data. HTTP Status Code: {response.status_code}")

    def store_weather_data(self, latitude, longitude, data):
        """
        Store weather data in the SQLite database.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            data (dict): Parsed JSON data from the API.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        location = f"{latitude},{longitude}"

        # Insert daily weather data
        daily_data = data.get("daily", {})
        dates = daily_data.get("time", [])
        max_temp = daily_data.get("temperature_2m_max", [])
        min_temp = daily_data.get("temperature_2m_min", [])
        mean_temp = daily_data.get("temperature_2m_mean", [])
        precipitation = daily_data.get("precipitation_sum", [])
        rain = daily_data.get("rain_sum", [])
        snowfall = daily_data.get("snowfall_sum", [])
        max_wind_speed = daily_data.get("wind_speed_10m_max", [])
        max_wind_gust = daily_data.get("wind_gusts_10m_max", [])

        for i in range(len(dates)):
            cursor.execute("""
                INSERT OR IGNORE INTO WeatherData (
                    date, location, max_temperature, min_temperature, mean_temperature,
                    precipitation_sum, rain_sum, snowfall_sum, max_wind_speed, max_wind_gust
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dates[i], location, max_temp[i], min_temp[i], mean_temp[i],
                precipitation[i], rain[i], snowfall[i], max_wind_speed[i], max_wind_gust[i]
            ))

        conn.commit()
        conn.close()
        print(f"Successfully stored weather data for location {latitude},{longitude}!")

    def weather_data_exists(self, date, latitude, longitude):
        """
        Check if weather data already exists for a specific date and location.

        Args:
            date (str): Date in YYYY-MM-DD format.
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.

        Returns:
            bool: True if weather data exists, False otherwise.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        location = f"{latitude},{longitude}"
        cursor.execute("SELECT 1 FROM WeatherData WHERE date = ? AND location = ?", (date, location))
        exists = cursor.fetchone() is not None
        conn.close()

        return exists

    def fetch_weather_for_games(self, home_location, away_locations):
        """
        Fetch weather data for all games in the GameResults table.

        Args:
            home_location (tuple): (latitude, longitude) for home games.
            away_locations (dict): Mapping of opponents to their (latitude, longitude).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Query game data from GameResults
        cursor.execute("SELECT date, home_away, opponent FROM GameResults")
        games = cursor.fetchall()
        conn.close()

        for game in games:
            game_date, home_away, opponent = game

            # Determine latitude and longitude based on home/away
            if home_away.lower() == "home":
                latitude, longitude = home_location
            else:
                latitude, longitude = away_locations.get(opponent, (None, None))

            if latitude is not None and longitude is not None:
                # Check if weather data for this date and location already exists
                if not self.weather_data_exists(game_date, latitude, longitude):
                    self.fetch_and_store_weather_data(
                        latitude=latitude,
                        longitude=longitude,
                        start_date=game_date,
                        end_date=game_date
                    )
                else:
                    print(f"Weather data for {game_date} at {latitude},{longitude} already exists.")



def main():
    football_api_key = "8thV0vyCOvX0BUKPqpBnAYNYfrDsURZdV1eVF/st5yidAwOFx7qUChQiUMk/f9m8"
    football_data = CollegeFootballData(football_api_key)
    football_data.fetch_and_store_michigan_data(2024, 2015)
    
    weather = Weather()
    home_location = (42.2808, 83.7430)  # Ann Arbor

    # dictionary of away locations (opponent -> (latitude, longitude))
    away_locations = {
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
    }
    
    weather.fetch_weather_for_games(home_location, away_locations)

if __name__ == "__main__":
    main()
