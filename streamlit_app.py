import datetime
import streamlit as st
import pandas as pd
import numpy as np
import psycopg2

from streamlit_extras.let_it_rain import rain
from streamlit_extras.stodo import to_do
from psycopg2 import sql
import os
from datetime import datetime, time


conn = psycopg2.connect(database = st.secrets('db_name'),
                        user = st.secrets('sql_user'),
                        host = st.secrets('host'),
                        password = st.secrets('sql_password'),
                        port = st.secrets('port')
                        )

# Establish database connection
def fetch_data_to_dataframe(table, date_time):
    try:
        primary_column = ""
        if table == "current_weather":
            primary_column = "last_updated"
        else:
            primary_column = "date"
        cur = conn.cursor()
        get_daily_query = sql.SQL(
            f"""
            SELECT * FROM student.{table}
            WHERE {primary_column} = {date_time}
            """
            )
        cur.execute(get_daily_query)
        data = cur.fetchall()
        if not data:
            st.warning("No weather data found for the selected date and time.")
            return None
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(data, columns = colnames)
    
    except psycopg2.Error as e:
        st.error(f"Error fetching data from database: {e}")
        return None

st.title("Weather in Bermuda Island 🏝️")

# Date and Time Input
selected_date = st.date_input("Select Date", datetime.date.today())
selected_time = st.time_input("Select Time", datetime.time(12, 0))  # Default to 12:00 PM

# Display Results
if selected_date and selected_time:
    date_time = f"{selected_date} {selected_time}"
    daily_data_df = fetch_data_to_dataframe("daily_data", selected_date)
    tides_df = fetch_data_to_dataframe("tides", selected_date)
    cur_weather_df = fetch_data_to_dataframe("current_weather", date_time)

    if daily_data_df is not None:
        st.subheader("Daily Weather Data")
        st.dataframe(daily_data_df)

    if tides_df is not None:
        st.subheader("Tides")
        st.dataframe(tides_df)

    if cur_weather_df is not None:
        st.subheader("Current Weather")
        st.dataframe(cur_weather_df)

conn.close()

# st.set_page_config(
#    page_icon="🌞"
# )

# key = st.secrets['api_key']
# st.title("Weather in Bermuda Island 🏝️")

# cola, colb = st.columns([3, 1])

# with colb:
#     sun = st.button("Let it shine")

# if sun:
#     st.balloons()


# days = st.slider("Select a day from this mounth:", 1, 31, step=1)

# if days < 3:
#     st.write("No data on this day")
# elif days == 3:
#     st.write(f"July the {days}'rd weather")
# else:
#     st.write(f"July the {days}'th weather")


# time = st.slider("Select a time:", 0, 24, step=1)






st.write(
    "Check out [weather API](https://www.weatherapi.com/docs/)."
)
