import datetime
import io
import requests
import streamlit as st
import pandas as pd
import numpy as np
import psycopg2

import plotly.express as px
from streamlit_extras.let_it_rain import rain
from streamlit_extras.stodo import to_do
from psycopg2 import sql
import datetime
from PIL import Image

conn = psycopg2.connect(database = st.secrets['db_name'],
                        user = st.secrets['sql_user'],
                        host = st.secrets['host'],
                        password = st.secrets['sql_password'],
                        port = st.secrets['port']
                        )

# Establish database connection
def fetch_data_to_dataframe(table, date_time):
    try:
        primary_column = ""
        if table == "jk_current_weather":
            primary_column = "last_updated"
        else:
            primary_column = "date"
        cur = conn.cursor()
        sql_query = sql.SQL(
            f"""
            SELECT * FROM student.{table}
            WHERE {primary_column} = '{date_time}'
            """
            )
        cur.execute(sql_query)
        data = cur.fetchall()
        if not data:
            if table == "jk_current_weather":
                st.warning("No current weather data found for the selected date and time.")
            else:
                st.warning("No daily data found for the selected date.")
            return None
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(data, columns = colnames)
    
    except psycopg2.Error as e:
        st.error(f"Error fetching data from database: {e}")
        return None

def half_hourly_for_day(date):
    try:
        cur = conn.cursor()
        sql_query = sql.SQL(
            f"""
            SELECT * FROM student.jk_current_weather
            WHERE last_updated LIKE '{date}%'
            """
            )
        cur.execute(sql_query)
        data = cur.fetchall()
        if not data:
            st.warning("No hoourly data found for the selected date.")
            return None
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(data, columns = colnames)
    
    except psycopg2.Error as e:
        st.error(f"Error fetching data from database: {e}")
        return None

st.title("Weather in Bermuda Island 🏝️")

# Date and Time Input
selected_date = st.date_input("Select date", datetime.date(2024, 7, 4))
selected_time = st.time_input("Select Time", datetime.time(12, 0))  # Default to 12:00 PM

# Display Results
if selected_date and selected_time:
    time_str = str(selected_time)[:5]
    date_time = f"{selected_date} {time_str}"
    cur_weather_df = fetch_data_to_dataframe("jk_current_weather", date_time)
    daily_data_df = fetch_data_to_dataframe("jk_daily_data", selected_date)
    tides_df = fetch_data_to_dataframe("jk_tides", selected_date)
    half_hourly_df = half_hourly_for_day(selected_date)

    if cur_weather_df is not None:
        st.subheader("Weather")
        
        # Fetch image from URL
        image_url = cur_weather_df['condition_icon'][0]
        image_url = "https:" + image_url
        response = requests.get(image_url)
        response.raise_for_status()
        st.image(Image.open(io.BytesIO(response.content)))
        st.write(cur_weather_df["condition_text"][0])
        st.dataframe(cur_weather_df)
        columns = list(cur_weather_df.columns)
        for x in columns:
            if x not in ("condition_text", "condition_icon"):
                st.write(f"{x}: {cur_weather_df[x][0]}")

    if daily_data_df is not None:
        st.subheader("Daily Forecast")
        st.dataframe(daily_data_df)

    if tides_df is not None:
        st.subheader("Tides")
        del tides_df['date']
        st.dataframe(tides_df)

    if half_hourly_df is not None:
        st.subheader("Half-Hourly")
        st.dataframe(half_hourly_df)
        fig = px.line(half_hourly_df,
                      x=half_hourly_df['last_updated'],
                      y='temp_c',
                      title='Temperature Change',
                      labels={'last_updated': 'Time',
                              'temp_c': 'Temperature'})
        st.plotly_chart(fig)

conn.close()

st.write(
    "Check out [weather API](https://www.weatherapi.com/docs/)."
)
