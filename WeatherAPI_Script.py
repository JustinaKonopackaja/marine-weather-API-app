import requests
import pandas as pd
import numpy as np
import psycopg2 as psql
import os
from dotenv import load_dotenv

load_dotenv()

# DB Configuration
db_name = os.getenv('db_name')
password = os.getenv('sql_password')
user = os.getenv('sql_user')
my_host = os.getenv('host')
db_port = os.getenv('port')

# API Configuration
location = "Bermuda"
api_key = os.getenv('api_key')

# Establish database connection
def connect_to_db():
  conn = psql.connect(database = db_name,
                      user = user,
                      host = my_host,
                      password = password,
                      port = db_port
                      )
  return conn

# Create SQL tables
def create_tables(conn):
  create_jk_current_weather = """
    CREATE TABLE IF NOT EXISTS student.jk_current_weather (
      last_updated VARCHAR(30) PRIMARY KEY,
      temp_c FLOAT,
      is_day INTEGER,
      wind_kph FLOAT,
      wind_degree INTEGER,
      wind_dir VARCHAR(10),
      pressure_mb FLOAT,
      precip_mm FLOAT,
      humidity INTEGER,
      cloud INTEGER,
      feelslike_c FLOAT,
      windchill_c FLOAT,
      heatindex_c FLOAT,
      dewpoint_c FLOAT,
      vis_km FLOAT,
      uv FLOAT,
      gust_kph FLOAT,
      condition_text VARCHAR(255),
      condition_icon VARCHAR(255)
    );
    """

  create_jk_daily_data = """
    CREATE TABLE IF NOT EXISTS student.jk_daily_data (
      date VARCHAR(50) PRIMARY KEY,
      maxtemp_c FLOAT,
      mintemp_c FLOAT,
      avgtemp_c FLOAT,
      maxwind_kph FLOAT,
      totalprecip_mm FLOAT,
      totalsnow_cm FLOAT,
      avgvis_km FLOAT,
      avghumidity FLOAT,
      sunrise VARCHAR(50),
      sunset VARCHAR(50),
      moonrise VARCHAR(50),
      moonset VARCHAR(50),
      moon_phase VARCHAR(50),
      moon_illumination INTEGER
    );
    """

  create_jk_tides = """
    CREATE TABLE IF NOT EXISTS student.jk_tides (
      date VARCHAR(50),
      tide_time VARCHAR(50),
      tide_height_mt VARCHAR(50),
      tide_type VARCHAR(50),
      PRIMARY KEY (date, tide_time)
    );
    """

  cur = conn.cursor()
  cur.execute(create_jk_current_weather)
  cur.execute(create_jk_daily_data)
  cur.execute(create_jk_tides)
  conn.commit()
  conn.close()

# Fetching data from API and populating SQL tables
def fetch_and_store_data(endpoint, conn):
  cur = conn.cursor()
  url = f"http://api.weatherapi.com/v1/{endpoint}.json?key={api_key}&q={location}&days=1"
  response = requests.get(url)

  if response.status_code == 200:
    data = response.json()

    # Handle 'marine/forecast' endpoint for current weather data
    if endpoint == 'marine/forecast':
      current = data['current']
      condition = current['condition']

      # Insert/Update current weather
      populate_jk_current_weather = """
        INSERT INTO student.jk_current_weather(
          last_updated, temp_c, is_day,
          wind_kph, wind_degree, wind_dir,
          pressure_mb, precip_mm, humidity,
          cloud, feelslike_c, windchill_c,
          heatindex_c, dewpoint_c, vis_km,
          uv, gust_kph, condition_text, condition_icon
          ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (last_updated) DO UPDATE SET
          temp_c = EXCLUDED.temp_c, is_day = EXCLUDED.is_day,
          wind_kph = EXCLUDED.wind_kph, wind_degree = EXCLUDED.wind_degree,
          wind_dir = EXCLUDED.wind_dir, pressure_mb = EXCLUDED.pressure_mb,
          precip_mm = EXCLUDED.precip_mm, humidity = EXCLUDED.humidity,
          cloud = EXCLUDED.cloud, feelslike_c = EXCLUDED.feelslike_c,
          windchill_c = EXCLUDED.windchill_c, heatindex_c = EXCLUDED.heatindex_c,
          dewpoint_c = EXCLUDED.dewpoint_c, vis_km = EXCLUDED.vis_km,
          uv = EXCLUDED.uv, gust_kph = EXCLUDED.gust_kph,
          condition_text = EXCLUDED.condition_text,
          condition_icon = EXCLUDED.condition_icon
        """
      current_weather_values = (
        current['last_updated'], current['temp_c'], current['is_day'],
        current['wind_kph'], current['wind_degree'], current['wind_dir'],
        current['pressure_mb'], current['precip_mm'], current['humidity'],
        current['cloud'], current['feelslike_c'], current['windchill_c'],
        current['heatindex_c'], current['dewpoint_c'], current['vis_km'],
        current['uv'], current['gust_kph'], condition['text'], condition['icon']
        )
      cur.execute(populate_jk_current_weather, current_weather_values)
      conn.commit()
      conn.close()

    # Handle 'marine' endpoint for daily data and tides
    elif endpoint == 'marine':
      forecastday = data['forecast']['forecastday'][0]
      day = forecastday['day']
      astro = forecastday['astro']

      # Insert/Update daily_data
      populate_jk_daily_data = f"""
        INSERT INTO student.jk_daily_data(
          date, maxtemp_c, mintemp_c,
          avgtemp_c, maxwind_kph, totalprecip_mm,
          totalsnow_cm, avgvis_km, avghumidity,
          sunrise, sunset, moonrise,
          moonset, moon_phase, moon_illumination
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
          maxtemp_c = EXCLUDED.maxtemp_c, mintemp_c = EXCLUDED.mintemp_c,
          avgtemp_c = EXCLUDED.avgtemp_c, maxwind_kph = EXCLUDED.maxwind_kph,
          totalprecip_mm = EXCLUDED.totalprecip_mm, totalsnow_cm = EXCLUDED.totalsnow_cm,
          avgvis_km = EXCLUDED.avgvis_km, avghumidity = EXCLUDED.avghumidity,
          sunrise = EXCLUDED.sunrise, sunset = EXCLUDED.sunset,
          moonrise = EXCLUDED.moonrise, moonset = EXCLUDED.moonset,
          moon_phase = EXCLUDED.moon_phase,
          moon_illumination = EXCLUDED.moon_illumination
        """
      daily_data_values = (
        forecastday['date'], day['maxtemp_c'], day['mintemp_c'],
        day['avgtemp_c'], day['maxwind_kph'], day['totalprecip_mm'],
        day['totalsnow_cm'], day['avgvis_km'], day['avghumidity'],
        astro['sunrise'], astro['sunset'], astro['moonrise'],
        astro['moonset'], astro['moon_phase'], astro['moon_illumination']
      )
      cur.execute(populate_jk_daily_data, daily_data_values)
      conn.commit()

      # Insert/Update tides
      for tide in day['tides'][0]['tide']:
        populate_jk_tides = f"""
          INSERT INTO student.jk_tides(
            date, tide_time, tide_height_mt, tide_type
          ) VALUES (%s, %s, %s, %s)
          ON CONFLICT (date, tide_time) DO UPDATE SET
            tide_height_mt = EXCLUDED.tide_height_mt,
            tide_type = EXCLUDED.tide_type
          """
        tides_values = (
          forecastday['date'], tide['tide_time'],
          tide['tide_height_mt'], tide['tide_type']
        )
        cur.execute(populate_jk_tides, tides_values)

      conn.commit()
      conn.close()

  else:
    return(f"Error fetching data from {endpoint}: {response.status_code}")

create_tables(connect_to_db())
fetch_and_store_data('marine', connect_to_db()) # Daily Data and Tides
fetch_and_store_data('marine/forecast', connect_to_db()) # Current Weather
