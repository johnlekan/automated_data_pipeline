# insert_records.py
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
# Don't import api_request here - we'll handle it in main()
# This prevents the fetch_data() call on import

def connect_to_db():
    """Connect to PostgreSQL database"""
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="weather_data",
            user="postgres",
            password="password"
        )
        print("Database connection established.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table(conn):
    """Create table if it doesn't exist"""
    print("Creating table if it does not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_description TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                utc_offset TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        print("Table created/verified successfully.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def insert_records(conn, data):
    """Insert records into the database"""
    print("Inserting records into the database...")
    try:
        weather = data["current"]
        location = data["location"]
        cursor = conn.cursor()
        
        # Convert localtime string to timestamp
        localtime_str = location.get("localtime", datetime.now().strftime("%Y-%m-%d %H:%M"))
        
        cursor.execute("""
            INSERT INTO dev.weather_data (
                city,
                temperature,
                weather_description,
                wind_speed,
                time,
                utc_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            location["name"],
            weather.get('temperature', 0),
            ', '.join(weather.get('weather_descriptions', ['Unknown'])),
            weather.get('wind_speed', 0),
            localtime_str,
            location.get('utc_offset', '+00:00')
        ))
        conn.commit()
        print(f"Record inserted for {location['name']}.")
    except psycopg2.Error as e:
        print(f"Error inserting records: {e}")
        raise

def mock_fetch_data():
    """Mock weather data for testing"""
    print("Mocking weather data fetch...")
    return {
        "location": {
            "name": "New York",
            "country": "United States",
            "region": "New York",
            "lat": 40.7128,
            "lon": -74.0060,
            "timezone_id": "America/New_York",
            "localtime": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "utc_offset": "-05:00"
        },
        "current": {
            "temperature": 20,
            "weather_descriptions": ["Sunny"],
            "wind_speed": 10,
            "humidity": 50,
        }
    }

def fetch_actual_data():
    """Fetch real weather data from API"""
    import requests
    import time 
    api_key = os.getenv("api_key")  # Consider moving to environment variable
    api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New%20York"
    
    print("Fetching weather data from API...")
    time.sleep(30)
    try:
        response = requests.get(api_url)

        if response.status_code == 429:
            print('Rate limited! using mock data.')
            return mock_fetch_data()
        response.raise_for_status()  # Raise an error for bad responses
        print("Data fetched successfully")
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        # Fall back to mock data
        print("Falling back to mock data...")
        return mock_fetch_data()

def main():
    """Main function to be called by Airflow"""
    try:
        print("Starting the weather data insertion process...")
        
        # Use mock data for now to avoid API rate limits
        data = mock_fetch_data()
        # Uncomment for real data when ready:
        # data = fetch_actual_data()
        
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, data)
        print("Weather data insertion completed successfully.")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        raise  # Re-raise so Airflow knows the task failed
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

# Only run if executed directly (for testing)
if __name__ == "__main__":
    main()