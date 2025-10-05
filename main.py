import streamlit as st
import pandas as pd
from data_fetch.fetch_weather import OpenWeatherFetcher
from database.db_manager import DBManager
from datetime import datetime
from alert.alert import check_alerts
from numpy.random import default_rng as rng
try:
   from dotenv import load_dotenv
   load_dotenv()
except Exception:
   pass
import os

st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌤 Real-Time Weather Dashboard")
user_input=st.text_input("Enter the city or its coordinates (lat, lon):", placeholder="e.g., London or 51.5074,-0.1278", key="location")
recent_readings=None
latest=None
if user_input and user_input.strip():
    key=os.getenv("OPENWEATHER_KEY")
    main_key=OpenWeatherFetcher(key)
    db = DBManager("weather.db")
    if ',' in user_input:
        try:
            lat, lon = map(float, user_input.split(','))
            reading = main_key.fetch_current_by_coords(lat, lon)
        except ValueError:
            st.error("Invalid coordinates format. Please enter as lat,lon (e.g., 51.5074,-0.1278).")
            st.stop()
    else:
        try:
            reading = main_key.fetch_current_by_city(user_input)
        except:
            st.error('Invalid city name or no data found.')
            st.stop()
    db.insert_reading(reading)
    recent_readings = db.fetch_recent_for_city(reading['city'],limit=100)

    # Show latest reading
    latest = recent_readings[0] if recent_readings else None
    if latest:
        st.subheader(f"Latest Weather in {latest['city']}, {latest['country']}")
        st.write(f"Temperature: {latest['temp_c']} °C")
        st.write(f"Feels like: {latest['feels_like_c']} °C")
        st.write(f"Humidity: {latest['humidity_pct']}%")
        st.write(f"Wind: {latest['wind_speed_kmh']} km/h")
        st.write(f"Rain (1h): {latest['rain_1h_mm']} mm")
        st.write(f"Snow (1h): {latest['snow_1h_mm']} mm")

    df=pd.DataFrame(recent_readings)
    df['timestamp_utc']=pd.to_datetime(df['timestamp_utc'])
    df = df.set_index("timestamp_utc")
    chart_data = df[["temp_c", "feels_like_c", "humidity_pct", "wind_speed_kmh", "rain_1h_mm", "snow_1h_mm"]]
    st.line_chart(chart_data)

    alert_message = check_alerts(latest)
    if alert_message:
        st.warning(alert_message)