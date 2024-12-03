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

import requests
import sqlite3
from datetime import datetime

class Weather:
    def __init__(self, db_path="football_stats.db"):
        """Initialize the Weather class with a database path."""
        self.db_path = db_path
        self.create_table()

    def create_table(self):
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
                max_wind_gust REAL
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

        entries_to_store = min(len(dates), 25)  # Limit to 25 entries

        for i in range(entries_to_store):
            cursor.execute("""
                INSERT INTO WeatherData (
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
        print(f"Successfully stored {entries_to_store} daily weather data entries!")

    def retrieve_weather_data(self, latitude, longitude, start_date, end_date):
        """
        Retrieve weather data from the SQLite database for a specific location and date range.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.

        Returns:
            list: List of dictionaries containing weather data.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        location = f"{latitude},{longitude}"
        cursor.execute("""
            SELECT date, max_temperature, min_temperature, mean_temperature,
                   precipitation_sum, rain_sum, snowfall_sum, max_wind_speed, max_wind_gust
            FROM WeatherData
            WHERE location = ? AND date BETWEEN ? AND ?
        """, (location, start_date, end_date))
        rows = cursor.fetchall()
        conn.close()

        return [
            {
                "date": row[0],
                "max_temperature": row[1],
                "min_temperature": row[2],
                "mean_temperature": row[3],
                "precipitation_sum": row[4],
                "rain_sum": row[5],
                "snowfall_sum": row[6],
                "max_wind_speed": row[7],
                "max_wind_gust": row[8]
            }
            for row in rows
        ]

    def print_weather_data(self, latitude, longitude, start_date, end_date):
        """
        Print weather data for a specific location and date range.

        Args:
            latitude (float): Latitude of the location.
            longitude (float): Longitude of the location.
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
        """
        weather_data = self.retrieve_weather_data(latitude, longitude, start_date, end_date)
        if weather_data:
            print(f"Weather data for {latitude},{longitude} from {start_date} to {end_date}:")
            for entry in weather_data:
                print(f"Date: {entry['date']}, Max Temp: {entry['max_temperature']}°C, "
                      f"Min Temp: {entry['min_temperature']}°F, Mean Temp: {entry['mean_temperature']}°F, "
                      f"Precipitation: {entry['precipitation_sum']}mm, Rain: {entry['rain_sum']}mm, "
                      f"Snowfall: {entry['snowfall_sum']}mm, Max Wind Speed: {entry['max_wind_speed']}km/h, "
                      f"Max Wind Gust: {entry['max_wind_gust']}km/h")
        else:
            print("No data found for the specified location and date range.")


def main():
    football_api_key = "8thV0vyCOvX0BUKPqpBnAYNYfrDsURZdV1eVF/st5yidAwOFx7qUChQiUMk/f9m8"
    football_data = CollegeFootballData(football_api_key)
    football_data.fetch_and_store_michigan_data(2024, 2015)
    
    weather = Weather()
    weather.fetch_and_store_weather_data(
        latitude=42.2808,
        longitude=83.7430,
        start_date="2024-11-17",
        end_date="2024-11-17"
    )
    # Retrieve and print the stored data
    weather.print_weather_data(
        latitude=42.2808,
        longitude=83.7430,
        start_date="2024-11-17",
        end_date="2024-11-17"
    )

if __name__ == "__main__":
    main()
