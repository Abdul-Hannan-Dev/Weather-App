import os
import time
import json
import random
import logging
import functools
from datetime import datetime, timezone
from typing import Dict, List, Optional
import requests
from requests import Session



LOG=logging.getLogger("fetch_weather")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
try:
   from dotenv import load_dotenv
   load_dotenv()
except Exception:
   pass
OPENWEATHER_KEY = os.getenv("OPENWEATHER_KEY")
BASE_URL="https://api.openweathermap.org/data/2.5"
DEFAULT_UNITS="metric"
DEFAULT_TIMEOUT=30


def retry(max_attempts=3, backoff_factor=1.0, allowed_exceptions=requests.exceptions.RequestException):
   def deco(func):
      @functools.wraps(func)
      def wrapper(*args, **kwargs):
         attempts=0
         while True:
            try:
               return func(*args, **kwargs)
            except allowed_exceptions as exc:
               attempts+=1
               if attempts > max_attempts:
                  LOG.exception("Max attempts reached for %s", func.__name__)
                  raise
               sleep=backoff_factor * (2 ** (attempts - 1)) + random.uniform(0, 1)
               LOG.warning("Call failed (%s). Retrying in %.2fs (attempt %d/%d)", exc, sleep, attempts, max_attempts)
               time.sleep(sleep)
      return wrapper
   return deco


class OpenWeatherFetcher:
   def __init__(self, api_key: str, units: str=DEFAULT_UNITS, timeout: int=DEFAULT_TIMEOUT):
      if not api_key:
         raise ValueError("API key must be provided")
      self.api_key = api_key
      self.units = units
      self.timeout = timeout
      self.session: Session = requests.Session()
   
   @retry(max_attempts=3, backoff_factor=1.0)
   def fetch_current_by_city(self, city:str, country: Optional[str]=None) -> Dict:
      q=f"{city},{country}" if country else city
      url=f'{BASE_URL}/weather'
      params = {
         'q':q,
         'appid':self.api_key,
         'units':self.units
      }
      LOG.info("Fetched weather for %s", q)
      resp=self.session.get(url, params=params, timeout=self.timeout)
      resp.raise_for_status()
      data=resp.json()
      return self._normalize(data)
   

   @retry(max_attempts=3, backoff_factor=1.0)
   def fetch_current_by_coords(self, lat:float, lon:float) -> Dict:
      url=f'{BASE_URL}/weather'
      params = {
         'lat':lat,
         'lon':lon,
         'appid':self.api_key,
         'units':self.units
      }
      LOG.info("Fetched weather for coords %d,%d", lat, lon )
      resp=self.session.get(url, params=params, timeout=self.timeout)
      resp.raise_for_status()
      data=resp.json()
      return self._normalize(data)

   def _normalize(self, raw_json: Dict) -> Dict:
      main=raw_json.get("main", {})
      wind=raw_json.get("wind", {})
      clouds=raw_json.get("clouds", {})
      sys=raw_json.get("sys", {})
      weather_arr=raw_json.get("weather", []) or []
      weather0=weather_arr[0] if weather_arr else {}
      dt_unix = raw_json.get("dt")
      ts = datetime.fromtimestamp(dt_unix, tz=timezone.utc).isoformat() if dt_unix else datetime.now(timezone.utc).isoformat()

      rain = raw_json.get("rain", {}) or {}
      snow = raw_json.get("snow", {}) or {}
      
      record = {
            "city": raw_json.get("name"),
            "country": sys.get("country"),
            "lat": raw_json.get("coord", {}).get("lat"),
            "lon": raw_json.get("coord", {}).get("lon"),
            "timestamp_utc": ts,
            "temp_c": main.get("temp"),
            "feels_like_c": main.get("feels_like"),
            "humidity_pct": main.get("humidity"),
            "pressure_hpa": main.get("pressure"),
            "wind_speed_mps": wind.get("speed"),
            "wind_speed_kmh": (wind.get("speed") * 3.6) if wind.get("speed") is not None else None,
            "wind_deg": wind.get("deg"),
            "rain_1h_mm": rain.get("1h", 0.0),
            "rain_3h_mm": rain.get("3h", 0.0),
            "snow_1h_mm": snow.get("1h", 0.0),
            "clouds_pct": clouds.get("all"),
            "weather_main": weather0.get("main"),
            "weather_description": weather0.get("description"),
            "raw_json": json.dumps(raw_json)
        }
      return record
   
   # def fetch_bulk_sequential(self, cities: List[str], delay_seconds: float = 1.0) -> List[Dict]:
   #      results = []
   #      for c in cities:
   #          try:
   #              r = self.fetch_current_by_city(c)
   #              results.append(r)
   #          except Exception as exc:
   #              LOG.error("Failed to fetch %s: %s", c, exc)
   #          time.sleep(delay_seconds)
   #      return results


# if __name__ == "__main__":
#     # quick manual test
#     key = OPENWEATHER_KEY
#     fetcher = OpenWeatherFetcher(key)
#     sample = fetcher.fetch_current_by_coords(24.8607,67.0011)
#     sample = fetcher.fetch_current_by_city('Karachi')
#     print(json.dumps(sample, indent=2))