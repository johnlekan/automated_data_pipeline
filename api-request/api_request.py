import requests
api_key = "5655c7ff380f2d0887097abac38155d5"
api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"



def fetch_data():
   print("Fetching weather data from API...")
   try:
       response = requests.get(api_url)
       response.raise_for_status()  # Raise an error for bad responses
       print("Data fetched successfully")
       return response.json()
   except requests.RequestException as e:
       print(f"Error fetching weather data: {e}")
       raise

fetch_data()

def mock_fetch_data():
    print("Mocking weather data fetch...")
    return {
        "location": {
            "name": "New York",
            "country": "United States",
            "region": "New York",
            "lat": 40.7128,
            "lon": -74.0060,
            "timezone_id": "America/New_York",
            "localtime": "2023-10-01 12:00",
        },
        "current": {
            "temperature": 20,
            "weather_descriptions": ["Sunny"],
            "wind_speed": 10,
            "humidity": 50,
        }
    }