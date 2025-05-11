from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import ast
import pandas as pd
import threading
from datetime import datetime

latest_weather = {}

def predict(data):
    location = data.get('location', {})
    current = data.get('current', {})

    # Build a single record with your desired columns
    record = {
        'location_name': location.get('name'),
        'time': location.get('localtime'),
        'temp_c': current.get('temp_c'),
        'is_day': current.get('is_day'),
        'condition_text': current.get('condition', {}).get('text'),
        'wind_mph': current.get('wind_mph'),
        'wind_degree': current.get('wind_degree'),
        'wind_dir': current.get('wind_dir'),
        'pressure_mb': current.get('pressure_mb'),
        'precip_mm': current.get('precip_mm'),
        'humidity': current.get('humidity'),
        'cloud': current.get('cloud'),
        'windchill_c': current.get('windchill_c'),
        'heatindex_c': current.get('heatindex_c'),
        'dewpoint_c': current.get('dewpoint_c'),
        'vis_km': current.get('vis_km'),
        'uv': current.get('uv'),
        'gust_mph': current.get('gust_mph'),
        'will_it_rain': None,         # Not available in current data
        'chance_of_rain': None        # Not available in current data
    }

    preprocessedData = pd.DataFrame([record])

    return preprocessedData

def consume():
    global latest_weather
    consumer = KafkaConsumer('weather-topic',
                             bootstrap_servers='localhost:9092',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["weather_db"]
    collection = db["weather_data"]
    
    for message in consumer:
        latest_weather = message.value  # already a dict
        predicted_data = predict(latest_weather)
        record = predicted_data.to_dict(orient='records')[0]

        # Convert 'time' string to a datetime object for BSON
        if 'time' in record:
            try:
                record['time'] = datetime.strptime(record['time'], "%Y-%m-%d %H:%M")
            except Exception as e:
                print("Error parsing time:", e)
                record['time'] = None

        collection.insert_one(record)

        latest_weather = {
            "location": { "name": record["location_name"] },
            "time": record["time"],
            "current": { "temp_c": record["temp_c"] }
        }
        print("Consumed weather data:", latest_weather)

def get_latest_weather():
    return latest_weather

# Run this only when called by backend
weather_thread = threading.Thread(target=consume)
weather_thread.daemon = True
weather_thread.start()
