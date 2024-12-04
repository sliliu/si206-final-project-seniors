import requests
import sqlite3
from time import sleep
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

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
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"                  "precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max,wind_gusts_10m_max",
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

def get_average_percentage_by_wind_speed():
    """
    Retrieve and calculate the average percentage of rushing and completion attempts 
    for each game based on three categories of wind speed, and plot the results as a bar chart with data labels.
    """
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to join GameResults, GameStats, and WeatherData
    query = '''
    SELECT 
        gr.gameID, 
        gr.date, 
        gr.home_away, 
        gr.opponent, 
        gs.rushingAttempts, 
        gs.completionAttempts, 
        wd.max_wind_speed
    FROM 
        GameResults AS gr
    JOIN 
        GameStats AS gs 
    ON 
        gr.gameID = gs.gameID
    JOIN 
        WeatherData AS wd 
    ON 
        gr.date = wd.date
    '''
    
    cursor.execute(query)
    rows = cursor.fetchall()

    # Initialize data structure to store totals and counts for averages
    results = {
        "low": {"passTotal": 0, "rushTotal": 0, "count": 0},
        "moderate": {"passTotal": 0, "rushTotal": 0, "count": 0},
        "high": {"passTotal": 0, "rushTotal": 0, "count": 0}
    }

    # Process and categorize data
    for row in rows:
        gameID, date, home_away, opponent, rushingAttempts, completionAttempts, max_wind_speed = row

        if rushingAttempts is not None and completionAttempts not in (None, 0):
            total_attempts = rushingAttempts + completionAttempts
            passPercentage = (completionAttempts / total_attempts) * 100
            rushPercentage = (rushingAttempts / total_attempts) * 100
        else:
            passPercentage = 0
            rushPercentage = 0

        # Categorize based on wind speed
        if max_wind_speed is None:
            continue  # Skip games with unknown wind speed
        elif max_wind_speed < 10:
            category = "low"
        elif 10 <= max_wind_speed < 20:
            category = "moderate"
        else:
            category = "high"

        # Accumulate totals and count for averages
        results[category]["passTotal"] += passPercentage
        results[category]["rushTotal"] += rushPercentage
        results[category]["count"] += 1

    # Compute averages
    averages = {
        category: {
            "averagePass": (values["passTotal"] / values["count"]) if values["count"] > 0 else 0,
            "averageRush": (values["rushTotal"] / values["count"]) if values["count"] > 0 else 0,
            "count": values["count"]
        }
        for category, values in results.items()
    }

    # Write results to a file
    with open('data.txt', 'w') as file:
        file.write("-------------Average Rushing and Passing Percentage by Wind Speed-------------\n")
        file.write("Category\tGames\tAverage Pass Percentage\tAverage Rush Percentage\n")
        
        for category, data in averages.items():
            file.write(f"{category.capitalize()}\t{data['count']}\t"
                       f"{data['averagePass']:.2f}%\t{data['averageRush']:.2f}%\n")

    # Prepare data for plotting
    categories = ["Low", "Moderate", "High"]
    avg_pass = [averages["low"]["averagePass"], averages["moderate"]["averagePass"], averages["high"]["averagePass"]]
    avg_rush = [averages["low"]["averageRush"], averages["moderate"]["averageRush"], averages["high"]["averageRush"]]

    # Bar plot
    x = np.arange(len(categories))  # X-axis positions for the categories
    width = 0.35  # Bar width

    plt.figure(figsize=(8, 5))
    pass_bars = plt.bar(x - width / 2, avg_pass, width, label='Average Pass Percentage', color='blue')
    rush_bars = plt.bar(x + width / 2, avg_rush, width, label='Average Rush Percentage', color='green')

    # Add labels, title, and legend
    plt.title('Average Rushing and Passing Percentages by Wind Speed')
    plt.xlabel('Wind Speed Category')
    plt.ylabel('Percentage (%)')
    plt.xticks(x, categories)  # Add category names to x-axis
    plt.ylim(0, 100)  # Percentages range from 0 to 100
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    # Add data labels
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2.0, height + 1,  # Position above bar
                     f'{height:.2f}%', ha='center', va='bottom', fontsize=10)

    add_labels(pass_bars)
    add_labels(rush_bars)

    # Save and show the plot
    plt.savefig('average_rushing_passing_bar_plot_with_labels.png')
    plt.show()
    
    conn.close()


def plot_average_points_by_temperature(categories):
    """
    Calculate the average total points for each temperature category and create a line plot.

    Parameters:
        categories (dict): A dictionary where keys are temperature categories ("Cold", "Moderate", "Warm")
                          and values are lists of tuples containing game data.
    """
    # Calculate average total points for each category
    averages = {}
    for category, games in categories.items():
        if games:  # Ensure there are games in the category
            total_points = sum(game[4] for game in games)  # Total points are in the 5th element of each tuple
            averages[category] = total_points / len(games)
        else:
            averages[category] = 0  # Handle empty categories

    # Extract data for plotting
    categories_list = list(averages.keys())
    averages_list = list(averages.values())

    # Create a line plot
    plt.figure(figsize=(8, 6))
    plt.plot(categories_list, averages_list, marker='o', linestyle='-', color='b', label='Average Total Points')

    # Customize the plot
    plt.title('Average Total Points by Temperature Category')
    plt.xlabel('Temperature Category')
    plt.ylabel('Average Total Points')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()

    # Show the plot
    plt.tight_layout()
    plt.savefig('average_points_by_temperature_plot.png')
    plt.show()

def get_total_points_by_temperature():
    """
    Retrieve and calculate total points for each game based on temperature ranges,
    along with game details, and write the results to a file.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to fetch data from GameResults and WeatherData
    query = '''
    SELECT 
        gr.date, 
        gr.gameID, 
        gr.home_away, 
        gr.opponent, 
        gr.total_points, 
        wd.mean_temperature
    FROM 
        GameResults AS gr
    JOIN 
        WeatherData AS wd 
    ON 
        gr.date = wd.date
    '''
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Define temperature ranges and categories
    categories = {
        "Cold": [],
        "Moderate": [],
        "Warm": []
    }
    
    # Categorize games based on temperature
    for row in rows:
        date, gameID, home_away, opponent, total_points, temperature = row
        
        if temperature is not None:
            if temperature < 32:
                categories["Cold"].append((date, gameID, home_away, opponent, total_points, temperature))
            elif 32 <= temperature <= 50:
                categories["Moderate"].append((date, gameID, home_away, opponent, total_points, temperature))
            else:
                categories["Warm"].append((date, gameID, home_away, opponent, total_points, temperature))
    
    plot_average_points_by_temperature(categories)
    
    # Write results to a file
    with open('data.txt', 'a') as file:
        file.write("-------------Total Points Based on Temperature-------------\n")
        
        for category, games in categories.items():
            file.write(f"\n--- {category} Games ---\n")
            file.write("Date\tGame ID\tHome/Away\tOpponent\tTotal Points\tTemperature\n")
            
            for game in games:
                date, gameID, home_away, opponent, total_points, temperature = game
                file.write(f"{date}\t{gameID}\t{home_away}\t{opponent}\t{total_points}\t{temperature}°F\n")
    
    # Close the database connection
    conn.close()
    

def plot_average_completion_by_wind_speed(categories):
    """
    Calculate the average completion percentage for each wind speed category and create a bar graph.

    Parameters:
        categories (dict): A dictionary where keys are wind speed categories ("Low Wind", "Moderate Wind", "High Wind")
                          and values are lists of tuples containing game data.
    """
    # Calculate average completion percentage for each category
    averages = {}
    for category, games in categories.items():
        if games:  # Ensure there are games in the category
            total_completion_percentage = sum(game[3] for game in games)  # Completion percentage is in the 4th element
            averages[category] = total_completion_percentage / len(games)
        else:
            averages[category] = 0  # Handle empty categories

    # Extract data for plotting
    categories_list = list(averages.keys())
    averages_list = list(averages.values())

    # Create a bar graph
    plt.figure(figsize=(8, 6))
    plt.bar(categories_list, averages_list, color=['green', 'orange', 'red'], alpha=0.7)

    # Customize the plot
    plt.title('Average Completion Percentage by Wind Speed Category')
    plt.xlabel('Wind Speed Category')
    plt.ylabel('Average Completion Percentage (%)')
    plt.ylim(0, 100)  # Assuming completion percentage ranges from 0 to 100
    plt.grid(axis='y', linestyle='--', alpha=0.6)

    # Add data labels to each bar
    for i, avg in enumerate(averages_list):
        plt.text(i, avg + 1, f'{avg:.2f}%', ha='center', va='bottom', fontsize=10)

    # Show the plot
    plt.tight_layout()
    plt.savefig('average_completion_by_wind_speed_plot.png')
    plt.show()

def get_completion_by_wind_speed():
    """
    Retrieve and calculate the completion percentage for each game based on C_ATT by max_wind_speed range category,
    along with game details, and write the results to a file.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to fetch data from GameStats, GameResults, and WeatherData
    query = '''
    SELECT 
        gr.date, 
        gs.C_ATT, 
        gr.home_away, 
        gr.opponent, 
        wd.max_wind_speed
    FROM 
        GameStats AS gs
    JOIN 
        GameResults AS gr 
    ON 
        gs.gameID = gr.gameID
    JOIN 
        WeatherData AS wd 
    ON
        gr.date = wd.date
    '''

    cursor.execute(query)
    rows = cursor.fetchall()

    # Define wind speed ranges and categories
    categories = {
        "Low Wind": [],
        "Moderate Wind": [],
        "High Wind": []
    }

    # Process the data
    for row in rows:
        date, c_att, home_away, opponent, max_wind_speed = row

        # Handle C_ATT and calculate completion percentage
        if c_att is not None:
            try:
                completed, attempted = map(int, c_att.split('-'))
                completion_percentage = (completed / attempted) * 100 if attempted > 0 else 0
            except ValueError:
                completion_percentage = 0
        else:
            completion_percentage = 0

        # Categorize games based on wind speed
        if max_wind_speed is not None:
            if max_wind_speed < 10:
                categories["Low Wind"].append((date, home_away, opponent, completion_percentage, max_wind_speed))
            elif 10 <= max_wind_speed <= 20:
                categories["Moderate Wind"].append((date, home_away, opponent, completion_percentage, max_wind_speed))
            else:
                categories["High Wind"].append((date, home_away, opponent, completion_percentage, max_wind_speed))

    plot_average_completion_by_wind_speed(categories)

    # Write results to a file
    with open('data.txt', 'a') as file:
        file.write("-------------Completion Percentage Based on Wind Speed-------------\n")

        for category, games in categories.items():
            file.write(f"\n--- {category} ---\n")
            file.write("Date\tHome/Away\tOpponent\tCompletion Percentage\tMax Wind Speed\n")

            for game in games:
                date, home_away, opponent, completion_percentage, max_wind_speed = game
                file.write(f"{date}\t{home_away}\t{opponent}\t{completion_percentage:.2f}%\t{max_wind_speed} mph\n")

    # Close the database connection
    conn.close()

def visual_completion_avg_total_points(averages):
    """
    Create a line plot to visualize the average total points for each completion range.
    
    Parameters:
    averages (dict): A dictionary with completion ranges as keys and average total points as values.
    """
    # Extract x and y values from the averages dictionary
    x_values = list(averages.keys())  # Completion ranges
    y_values = list(averages.values())  # Average total points

    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.plot(x_values, y_values, marker='o', linestyle='-', color='b', label='Avg Total Points')

    # Customize the plot
    plt.title('Average Total Points by Completion Percentage Range', fontsize=14)
    plt.xlabel('Completion Percentage Range', fontsize=12)
    plt.ylabel('Average Total Points', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()

    # Show the plot
    plt.savefig('completion_avg_total_points_plot.png')
    plt.show()
    
def get_avg_score_per_percentage():
    """
    Calculate and write the average total points for games grouped by completion percentage ranges.
    """
    import sqlite3

    conn = sqlite3.connect('football_stats.db')
    cursor = conn.cursor()

    # SQL query to fetch gameID, totalPoints, and C_ATT
    query = '''
    SELECT 
        gr.gameID, 
        gr.total_points, 
        gs.C_ATT
    FROM 
        GameResults AS gr
    JOIN 
        GameStats AS gs 
    ON 
        gr.gameID = gs.gameID
    '''

    cursor.execute(query)
    rows = cursor.fetchall()

    # Create a dictionary to group games by completion percentage ranges
    completion_ranges = {
        "0-50": [],
        "51-60": [],
        "61-70": [],
        "71-100": []
    }

    # Process the data
    for row in rows:
        game_id, total_points, c_att = row

        if c_att is not None:
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

        # Assign the game to the appropriate completion percentage range
        if completion_percentage <= 50:
            completion_ranges["0-50"].append(total_points)
        elif 51 <= completion_percentage <= 60:
            completion_ranges["51-60"].append(total_points)
        elif 61 <= completion_percentage <= 70:
            completion_ranges["61-70"].append(total_points)
        elif 71 <= completion_percentage <= 100:
            completion_ranges["71-100"].append(total_points)

    # Calculate the average total points for each range
    averages = {
        range_key: (sum(points) / len(points) if points else 0)
        for range_key, points in completion_ranges.items()
    }

    visual_completion_avg_total_points(averages)

    # Write the results to a file
    with open('data.txt', 'a') as file:
        file.write("-------------Average Score Per Completion Percentage Range-------------\n")
        file.write("Range\tAverage Total Points\n")

        for range_key, avg_points in averages.items():
            file.write(f"{range_key}\t{avg_points:.2f}\n")

    conn.close()


def main():
    football_api_key = "8thV0vyCOvX0BUKPqpBnAYNYfrDsURZdV1eVF/st5yidAwOFx7qUChQiUMk/f9m8"
    football_data = CollegeFootballData(football_api_key)
    football_data.fetch_and_store_michigan_data(2024, 2015)
    
    weather = Weather()
    home_location = (42.2808, 83.7430)  # Ann Arbor Latitude and Longitude

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
        "Utah": (40.7649, 111.8421),
    }
    
    weather.fetch_weather_for_games(home_location, away_locations)
    
    get_average_percentage_by_wind_speed()
    get_total_points_by_temperature()
    get_completion_by_wind_speed()
    get_avg_score_per_percentage()

if __name__ == "__main__":
    main()
