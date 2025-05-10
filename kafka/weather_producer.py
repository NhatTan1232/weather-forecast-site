from kafka import KafkaProducer
import requests
import json
import time
import os
from cities import city_to_id
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")  # from weatherapi.com or any API
cities = list(city_to_id.keys())
print(API_KEY)
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_weather(city):
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}&aqi=no"
    response = requests.get(url)
    data = response.json()
    print(data)
    data['location']['name'] = city_to_id[city]
    return data

if __name__ == "__main__":
    while True:
        for city in cities:
            weather_data = fetch_weather(city)
            if weather_data:
                producer.send('weather-topic', weather_data)
                print("Produced weather data:", weather_data)
