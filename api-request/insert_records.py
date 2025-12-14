from api_request import mock_fetch_data, fetch_data
import psycopg2

print(mock_fetch_data())

def connect_to_db():
    # Placeholder for database connection logic
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            host = "postgres",
            port = 5432,
            dbname = "weather_data",
            user = "postgres",
            password = "password"
        )
        print(conn)
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise



def create_table():
    # Placeholder for table creation logic
    print("Creating table if it does not exist...")
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT
                temperature FLOAT,
                weather_description TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW()
                utc_offset TEXT
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def insert_records(conn, data):
    # Placeholder for record insertion logic
    print("Inserting records into the database...")
    try:
        weather = data["current"]
        location = data["location"]
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.weather_data (
                city,
                temperature,
                weather_description,
                wind_speed,
                time,
                inserted_at,
                utc_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            location["name"],
            weather['temperature'],
            weather['weather_description'],
            weather['wind_speed'],
            location['localtime'],
            location['utc_offset']
        ))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error inserting records: {e}")
        raise


def main():
    try:
        print("Starting the weather data insertion process...")
        # data = mock_fetch_data()
        data = fetch_data()
        conn = connect_to_db()
        create_table()
        insert_records(conn, data)
        print("Weather data insertion completed successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if "conn" in locals():
            conn.close()
            print("Database connection closed.")
    
    main()

