import smtplib
from email.mime.text import MIMEText

thresholds = {
    "temp_c": 38,        
    "wind_speed_kmh": 30, 
    "rain_1h_mm": 10,   
    "snow_1h_mm": 5      
}

def check_alerts(record, thresholds=thresholds):
    alerts=[]
    if record['temp_c'] is not None and record['temp_c'] > thresholds['temp_c']:
        alerts.append(f"High Temperature Alert: {record['temp_c']}°C exceeds threshold of {thresholds['temp_c']}°C")
    if record['wind_speed_kmh'] is not None and record['wind_speed_kmh'] > thresholds['wind_speed_kmh']:
        alerts.append(f"High Wind Speed Alert: {record['wind_speed_kmh']} km/h exceeds threshold of {thresholds['wind_speed_kmh']} km/h")
    if record['rain_1h_mm'] is not None and record['rain_1h_mm'] > thresholds['rain_1h_mm']:
        alerts.append(f"Heavy Rain Alert: {record['rain_1h_mm']} mm in last hour exceeds threshold of {thresholds['rain_1h_mm']} mm")
    if record['snow_1h_mm'] is not None and record['snow_1h_mm'] > thresholds['snow_1h_mm']:
        alerts.append(f"Heavy Snow Alert: {record['snow_1h_mm']} mm in last hour exceeds threshold of {thresholds['snow_1h_mm']} mm")   
    return alerts

