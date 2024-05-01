import psycopg2
import openmeteo_requests
import pandas as pd
from retry_requests import retry
import requests_cache
from datetime import datetime
from sqlalchemy import create_engine

try:
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # API endpoint and parameters
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 41.85,
        "longitude": -87.65,
        "start_date": "2019-01-01",
        "end_date": datetime.now().strftime('%Y-%m-%d'),
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max"],
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/Chicago"
    }

    # Fetch data from the API
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]

    # Process daily data
    daily = response.Daily()

    # Create a DataFrame
    daily_data = {
        "date": pd.date_range(start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                              end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                              freq=pd.Timedelta(seconds=daily.Interval()), inclusive="left"),
        "weather_code": daily.Variables(0).ValuesAsNumpy(),
        "temperature_2m_max": daily.Variables(1).ValuesAsNumpy(),
        "temperature_2m_min": daily.Variables(2).ValuesAsNumpy(),
        "temperature_2m_mean": daily.Variables(3).ValuesAsNumpy(),
        "sunshine_duration": daily.Variables(4).ValuesAsNumpy(),
        "precipitation_sum": daily.Variables(5).ValuesAsNumpy(),
        "rain_sum": daily.Variables(6).ValuesAsNumpy(),
        "snowfall_sum": daily.Variables(7).ValuesAsNumpy(),
        "precipitation_hours": daily.Variables(8).ValuesAsNumpy(),
        "wind_speed_10m_max": daily.Variables(9).ValuesAsNumpy()
    }

    daily_dataframe = pd.DataFrame(data=daily_data)

    # Database connection parameters
    dbname = 'postgres'
    user = 'postgres'
    password = 'naukee'
    host = 'localhost'
    port = '5432'

    # Establish a connection to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print("Connected to Database")

    # Insert data into PostgreSQL table using SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    daily_dataframe.to_sql('weather_data', engine, if_exists='append', index=False)

    # Commit the transaction
    conn.commit()
    print("Weather data successfully inserted")

except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the connection
    if conn:
        conn.close()
