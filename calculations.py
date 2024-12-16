import requests
import sqlite3
from time import sleep
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

def get_average_percentage_by_wind_speed():
    """
    Calculates and visualizes the average passing and rushing percentages for 
    football games based on wind speed categories. 

    This function connects to a SQLite database to retrieve game data, processes 
    the data to calculate average passing and rushing percentages grouped by wind 
    speed categories (low, moderate, high), writes these averages to a text file, 
    and generates a bar plot with the results.

    Wind Speed Categories:
        - Low: Wind speed < 10 mph
        - Moderate: 10 mph ≤ Wind speed < 20 mph
        - High: Wind speed ≥ 20 mph
    
    Inputs: None

    Outputs:
        - A text file `data.txt` summarizing average percentages for each wind category.
        - A bar chart `average_rushing_passing_bar_plot_with_labels.png` illustrating the percentages.
    """
    conn = sqlite3.connect('michigan_football.db')
    cursor = conn.cursor()

    # SQL query to join GameResults, GameStats, and WeatherData
    query = '''
    SELECT 
        gr.gameID, 
        gr.date_timestamp, 
        gr.home_away_id, 
        gr.rushingAttempts, 
        gr.completionAttempts, 
        gr.max_wind_speed
    FROM 
        Games AS gr
    JOIN 
        HomeAway h ON gr.home_away_id = h.id

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
        gameID, date, home_away, rushingAttempts, completionAttempts, max_wind_speed = row

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
    Generates a line plot showing the average total points scored in games categorized by temperature.

    Parameters:
        categories (dict): A dictionary where keys are temperature categories 
                           ("Cold", "Moderate", "Warm") and values are lists of tuples 
                           containing game data (date, game ID, home/away ID, total points, temperature).

    This function calculates the average total points for each temperature category, 
    and creates a line plot with markers for visualization. The plot is saved as an 
    image file (`average_points_by_temperature_plot.png`).
    """
    # Calculate average total points for each category
    averages = {}
    for category, games in categories.items():
        if games:  # Ensure there are games in the category
            total_points = sum(game[3] for game in games)  # Total points are in the 4th element of each tuple
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
    Retrieves game data from a SQLite database, categorizes games based on temperature ranges, 
    calculates total points for each category, and generates a line plot of the average points 
    for each category. The detailed game data is also written to a file (`data.txt`).
    
    Inputs: None

    Outputs:
        - A text file (`data.txt`) with game data categorized by temperature.
        - A line plot (`average_points_by_temperature_plot.png`) showing average points by temperature.

    Temperature Categories:
        - "Cold": Temperature < 32°F
        - "Moderate": 32°F ≤ Temperature ≤ 50°F
        - "Warm": Temperature > 50°F
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('michigan_football.db')
    cursor = conn.cursor()

    # SQL query to fetch data from GameResults and WeatherData
    query = '''
    SELECT 
        gr.date_timestamp, 
        gr.gameID, 
        gr.home_away_id, 
        gr.total_points, 
        gr.mean_temperature
    FROM 
        Games AS gr
    JOIN 
        HomeAway h ON gr.home_away_id = h.id
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
        date, gameID, home_away, total_points, temperature = row
        
        if temperature is not None:
            if temperature < 32:
                categories["Cold"].append((date, gameID, home_away, total_points, temperature))
            elif 32 <= temperature <= 50:
                categories["Moderate"].append((date, gameID, home_away, total_points, temperature))
            else:
                categories["Warm"].append((date, gameID, home_away, total_points, temperature))
    
    plot_average_points_by_temperature(categories)
    
    # Write results to a file
    with open('data.txt', 'a') as file:
        file.write("\n-------------Total Points Based on Temperature-------------\n")
        
        for category, games in categories.items():
            file.write(f"\n--- {category} Games ---\n")
            file.write("Date\tGame ID\tHome/Away\tTotal Points\tTemperature\n")
            
            for game in games:
                date, gameID, home_away, total_points, temperature = game
                file.write(f"{date}\t{gameID}\t{home_away}\t{total_points}\t{temperature}°F\n")
    
    # Close the database connection
    conn.close()
    

def plot_average_completion_by_wind_speed(categories):
    """
    Generates a bar graph showing the average completion percentage categorized by wind speed.

    Parameters:
        categories (dict): A dictionary where:
                           - Keys are wind speed categories ("Low Wind", "Moderate Wind", "High Wind").
                           - Values are lists of tuples, each containing:
                             (date, home/away, completion percentage, max wind speed).

    Functionality:
    - Calculates the average completion percentage for each wind speed category.
    - Creates a bar graph with distinct colors for each category.
    - Displays completion percentages as data labels above the bars.
    - Saves the graph to a file named `average_completion_by_wind_speed_plot.png`.
    """
    # Calculate average completion percentage for each category
    averages = {}
    for category, games in categories.items():
        if games:  # Ensure there are games in the category
            total_completion_percentage = sum(game[2] for game in games)  # Completion percentage is in the 3th element
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
    Retrieves game data from a SQLite database, calculates the completion percentage for each game, 
    categorizes games based on wind speed ranges, and writes detailed results to a file. 
    It also generates a bar graph showing the average completion percentage by wind speed.
    
    Inputs: None

    Outputs:
        - A text file (`data.txt`) containing game data categorized by wind speed.
        - A bar graph (`average_completion_by_wind_speed_plot.png`) showing average completion percentages.

    Wind Speed Categories:
        - "Low Wind": Wind speed < 10 mph.
        - "Moderate Wind": 10 mph ≤ Wind speed ≤ 20 mph.
        - "High Wind": Wind speed > 20 mph.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('michigan_football.db')
    cursor = conn.cursor()

    # SQL query to fetch data from GameStats, GameResults, and WeatherData
    query = '''
    SELECT 
        gr.date_timestamp, 
        gr.completed,
        gr.attempted, 
        gr.home_away_id, 
        gr.max_wind_speed
    FROM 
        Games AS gr
    JOIN 
        HomeAway h ON gr.home_away_id = h.id
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
        date, completed, attempted, home_away, max_wind_speed = row

        # Handle C_ATT and calculate completion percentage
        if completed is not None and attempted is not None:
            try:
                completion_percentage = (completed / attempted) * 100 if attempted > 0 else 0
            except ValueError:
                completion_percentage = 0
        else:
            completion_percentage = 0

        # Categorize games based on wind speed
        if max_wind_speed is not None:
            if max_wind_speed < 10:
                categories["Low Wind"].append((date, home_away, completion_percentage, max_wind_speed))
            elif 10 <= max_wind_speed <= 20:
                categories["Moderate Wind"].append((date, home_away, completion_percentage, max_wind_speed))
            else:
                categories["High Wind"].append((date, home_away, completion_percentage, max_wind_speed))

    plot_average_completion_by_wind_speed(categories)

    # Write results to a file
    with open('data.txt', 'a') as file:
        file.write("\n-------------Completion Percentage Based on Wind Speed-------------\n")

        for category, games in categories.items():
            file.write(f"\n--- {category} ---\n")
            file.write("Date\tHome/Away\tCompletion Percentage\tMax Wind Speed\n")

            for game in games:
                date, home_away, completion_percentage, max_wind_speed = game
                file.write(f"{date}\t{home_away}\t{completion_percentage:.2f}%\t{max_wind_speed} mph\n")

    # Close the database connection
    conn.close()

def visual_completion_avg_total_points(averages):
    """
    Creates a line plot to visualize the relationship between completion percentage ranges 
    and the average total points scored in games.

    Parameters:
        averages (dict): A dictionary where:
                         - Keys are completion percentage ranges as strings (e.g., "0-50").
                         - Values are the average total points scored for games in that range.

    Functionality/Outputs:
    - Generates a line plot with the completion percentage ranges on the x-axis and average total points on the y-axis.
    - Saves the plot as a PNG file named `completion_avg_total_points_plot.png`.
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
    Calculates the average total points for games grouped by completion percentage ranges,
    visualizes the data in a line plot, and writes the results to a file.
    
    Inputs: None

    Outputs:
        - A line plot (`completion_avg_total_points_plot.png`) showing average total points 
        for each completion percentage range.
        - Appends the results to a text file (`data.txt`).

    Completion Percentage Ranges:
        - "0-50": Completion percentage ≤ 50%.
        - "51-60": 51% ≤ Completion percentage ≤ 60%.
        - "61-70": 61% ≤ Completion percentage ≤ 70%.
        - "71-100": 71% ≤ Completion percentage ≤ 100%.
    """
    import sqlite3

    conn = sqlite3.connect('michigan_football.db')
    cursor = conn.cursor()

    # SQL query to fetch gameID, totalPoints, and C_ATT
    query = '''
    SELECT 
        gr.gameID, 
        gr.total_points, 
        gr.completed,
        gr.attempted
    FROM 
        Games AS gr
    JOIN 
        HomeAway h ON gr.home_away_id = h.id
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
        game_id, total_points, completed, attempted = row

        if completed is not None and attempted is not None:
            try:
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
        file.write("\n-------------Average Score Per Completion Percentage Range-------------\n")
        file.write("Range\tAverage Total Points\n")

        for range_key, avg_points in averages.items():
            file.write(f"{range_key}\t{avg_points:.2f}\n")

    conn.close()


def main():   
    """
    Serves as the main for executing the program.
    
    Inputs: None

    Functionality:
    - Calls the following functions:
        1. `get_average_percentage_by_wind_speed`: Analyzes completion percentages based on wind speed.
        2. `get_total_points_by_temperature`: Analyzes total points scored based on temperature ranges.
        3. `get_completion_by_wind_speed`: Analyzes completion percentages based on wind speed.
        4. `get_avg_score_per_percentage`: Analyzes average total points based on completion percentage ranges.
    """
    get_average_percentage_by_wind_speed()
    get_total_points_by_temperature()
    get_completion_by_wind_speed()
    get_avg_score_per_percentage()

if __name__ == "__main__":
    main()
