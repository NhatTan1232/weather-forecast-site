from flask import Flask, render_template, jsonify, send_from_directory
from pymongo import MongoClient
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'kafka')))
import weather_consumer

app = Flask(__name__)

# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["weather_db"]
collection = db["weather_data"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/weather')
def get_weather():
    return jsonify(weather_consumer.get_latest_weather())

@app.route('/vietnam.svg')
def serve_svg():
    return send_from_directory(app.static_folder, 'vietnam.svg')

@app.route("/province/<province_id>")
def get_province_prediction(province_id):
    # Fetch the latest prediction for the province
    result = collection.find_one(
        {"location_id": province_id},
        sort=[("_id", -1)]  # Latest document
    )
    if result:
        result["_id"] = str(result["_id"])  # Convert ObjectId to string
        return jsonify(result)
    else:
        return jsonify({"error": "No prediction data found for " + province_id}), 404

if __name__ == '__main__':
    app.run(debug=True)
