from kafka import KafkaProducer
import requests
import json
import time
import os

API_KEY = os.getenv("WEATHER_API_KEY")  # from weatherapi.com or any API
CITY = "Hanoi"
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_weather():
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"
    response = requests.get(url)
    data = response.json()
    data['location']['name'] = 'VNHN'
    return data

if __name__ == "__main__":
    while True:
        weather_data = fetch_weather()
        producer.send('weather-topic', weather_data)
        print("Produced weather data:", weather_data)
        time.sleep(10)  # every 30 mins (or shorter for testing)
