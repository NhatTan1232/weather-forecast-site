from flask import Flask, render_template, jsonify, send_from_directory
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'kafka')))
import weather_consumer

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/weather')
def get_weather():
    return jsonify(weather_consumer.get_latest_weather())

@app.route('/vietnam.svg')
def serve_svg():
    return send_from_directory(app.static_folder, 'vietnam.svg')

if __name__ == '__main__':
    app.run(debug=True)
