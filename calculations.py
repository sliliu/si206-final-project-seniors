import requests
import sqlite3
from time import sleep
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

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
    get_average_percentage_by_wind_speed()
    get_total_points_by_temperature()
    get_completion_by_wind_speed()
    get_avg_score_per_percentage()

if __name__ == "__main__":
    main()
