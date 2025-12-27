
import requests
from dotenv import load_dotenv
import os

load_dotenv()



def fetch_weather_data(location="New York"):
    """Fetch weather data from API"""
    api_key = os.getenv("api_key")
    api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query={location}"
    
    print(f"Fetching weather data for {location}...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        raise

# REMOVE any function calls at module level!
# fetch_weather_data()  # ← DELETE THIS IF IT EXISTS