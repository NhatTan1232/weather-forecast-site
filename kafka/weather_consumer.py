from kafka import KafkaConsumer
import json
import threading

latest_weather = {}

def consume():
    global latest_weather
    consumer = KafkaConsumer('weather-topic',
                             bootstrap_servers='localhost:9092',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for message in consumer:
        latest_weather = message.value
        print("Consumed weather data:", latest_weather)

def get_latest_weather():
    return latest_weather

# Run this only when called by backend
weather_thread = threading.Thread(target=consume)
weather_thread.daemon = True
weather_thread.start()
