from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import pandas as pd
import threading
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model
import joblib

# Load trained model and scalers
model_temp = load_model("model/model_rnn/model_temp.h5", compile=False)
scaler_X_temp = joblib.load("model/model_rnn/scaler_X_temp.pkl")
scaler_y_temp = joblib.load("model/model_rnn/scaler_y_temp.pkl")

# If you used LabelEncoder or StringIndexer-like mappings, load them
with open("model/model_rnn/condition_index_map.json", "r") as f:
    condition_mapping = json.load(f)

with open("model/model_rnn/wind_index_map.json", "r") as f:
    wind_dir_mapping = json.load(f)

latest_weather = {}

# List of features used for prediction
temp_features = [
    'temp_c', 'is_day', 'wind_mph', 'wind_degree', 'precip_mm', 'humidity',
    'cloud', 'vis_km', 'uv', 'gust_mph', 'condition_index', 'wind_dir_index'
]

def preprocess_record(data):
    location = data.get('location', {})
    current = data.get('current', {})

    record = {
        'location_name': location.get('name'),
        'location_id': location.get('id'),
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
    }

    df = pd.DataFrame([record])

    # Map categorical features
    df['condition_index'] = df['condition_text'].map(condition_mapping).fillna(-1)
    df['wind_dir_index'] = df['wind_dir'].map(wind_dir_mapping).fillna(-1)

    return df

def predict_single_point(data):
    df = preprocess_record(data)

    # Select and scale features
    X = df[temp_features]
    X_scaled = scaler_X_temp.transform(X).reshape(1, 1, -1)

    # Predict next 24 hours
    y_scaled = model_temp.predict(X_scaled)
    y = scaler_y_temp.inverse_transform(y_scaled)
    return y.tolist()  # Return as list of floats

def consume():
    global latest_weather
    consumer = KafkaConsumer(
        'weather-topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["weather_db"]
    collection = db["weather_data"]

    for message in consumer:
        latest_weather = message.value
        try:
            # Predict next 24 hours temperature
            predicted_temps_24h = predict_single_point(latest_weather)  # Shape: (1, 24)

            # Process full weather data into a flat dict
            record_df = preprocess_record(latest_weather)
            record = record_df.to_dict(orient='records')[0]

            # Attach prediction
            record['predicted_temp_next_24h'] = predicted_temps_24h  # List of 24 predicted float values

            # Convert 'time' field to datetime
            if 'time' in record:
                try:
                    record['time'] = datetime.strptime(record['time'], "%Y-%m-%d %H:%M")
                except Exception as e:
                    print("Error parsing time:", e)
                    record['time'] = None

            # Save to MongoDB
            collection.insert_one(record)

            print(f"✅ Inserted | Location: {record['location_name']} | Time: {record['time']}")

        except Exception as e:
            print("❌ Error in processing:", e)

def get_latest_weather():
    return latest_weather

# Run the consumer in a separate daemon thread
weather_thread = threading.Thread(target=consume)
weather_thread.daemon = True
weather_thread.start()
