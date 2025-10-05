import sqlite3
from sqlite3 import Connection
from typing import Dict, List
from pathlib import Path

DB_PATH=Path(__file__).parent/'weather.db'

class DBManager:
    def __init__(self,db_path=DB_PATH):
        self.db_path=db_path
        self.conn: Connection=sqlite3.connect(self.db_path)
        self.conn.row_factory=sqlite3.Row
        self._create_table()
    def _create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS weather_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    country TEXT,
    lat REAL,
    lon REAL,
    timestamp_utc TEXT,
    temp_c REAL,
    feels_like_c REAL,
    humidity_pct INTEGER,
    pressure_hpa INTEGER,
    wind_speed_mps REAL,
    wind_speed_kmh REAL,
    wind_deg INTEGER,
    rain_1h_mm REAL,
    rain_3h_mm REAL,
    snow_1h_mm REAL,
    clouds_pct INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    raw_json TEXT
);
        """
        self.conn.execute(query)
        self.conn.commit()
    def insert_reading(self,reading:Dict):
        key=','.join(reading.keys())
        placeholders=','.join('?' for _ in reading)
        values=tuple(reading.values())
        query=f"INSERT INTO weather_readings ({key}) VALUES ({placeholders})"
        self.conn.execute(query,values)
        self.conn.commit()

    # def fetch_recent(self, limit=24) -> List[Dict]:
    #     query = f"SELECT * FROM weather_readings ORDER BY timestamp_utc DESC LIMIT {limit}"
    #     cursor = self.conn.execute(query)
    #     return [dict(row) for row in cursor]

    def fetch_recent_for_city(self, city: str, limit=24) -> List[Dict]:
        query = """
        SELECT * FROM weather_readings 
        WHERE city = ? 
        ORDER BY timestamp_utc DESC 
        LIMIT ?
        """
        cursor = self.conn.execute(query, (city, limit))
        return [dict(row) for row in cursor]

    def close(self):
        self.conn.close()