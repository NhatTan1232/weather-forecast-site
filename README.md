# Weather Web Application

A real-time weather forecasting application that combines weather data streaming with machine learning predictions for temperature forecasting in Vietnam.

## Features

- Real-time weather data streaming using Apache Kafka
- Machine learning-based temperature predictions for the next 24 hours
- Interactive visualization dashboard with Vietnam map integration
- MongoDB database for historical weather data storage
- RESTful API endpoints for weather data access

## Technology Stack

- **Backend**: Python Flask
- **Data Streaming**: Apache Kafka
- **Database**: MongoDB
- **Machine Learning**: TensorFlow (RNN model for temperature prediction)
- **Frontend**: HTML, JavaScript with SVG visualization
- **Data Processing**: Pandas, Scikit-learn

## Prerequisites

- Python 3.11+
- Apache Kafka
- MongoDB
- Required Python packages (see Installation section)

## Installation

1. Clone the repository
2. Install Python dependencies:
```bash
pip install flask kafka-python pymongo pandas scikit-learn tensorflow joblib
```

3. Set up Kafka:
   - Extract the Kafka archive:
   ```bash
   cd kafka
   tar -xzf kafka_2.12-3.9.0.tgz
   ```
   - Start Zookeeper and Kafka servers (see Running section)

4. Start MongoDB server on your local machine (default port: 27017)

## Project Structure

```
Weather_Web/
├── backend/
│   ├── app.py              # Flask server with API endpoints
│   ├── static/             # Static files
│   └── templates/          # HTML templates
├── kafka/
│   ├── weather_producer.py # Kafka producer for weather data
│   ├── weather_consumer.py # Kafka consumer with ML predictions
│   └── cities.py          # City data configuration
└── model/
    └── model_rnn/         # ML model files and scalers
```

## Running the Application

1. Start Kafka services:
```bash
# Start Zookeeper
cd kafka/kafExd/bin/windows
./zookeeper-server-start.bat ../../config/zookeeper.properties

# In a new terminal, start Kafka
cd kafka/kafExd/bin/windows
./kafka-server-start.bat ../../config/server.properties
```

2. Start the Kafka producer:
```bash
python kafka/weather_producer.py
```

3. Start the Kafka consumer:
```bash
python kafka/weather_consumer.py
```

4. Start the Flask application:
```bash
python backend/app.py
```

5. Access the application at `http://localhost:5000`

## Machine Learning Models

The application uses two main machine learning models:
- RNN (Recurrent Neural Network) for temperature prediction
- Models are pre-trained and stored in the `model/model_rnn/` directory

## Data Flow

1. Weather Producer fetches real-time weather data
2. Data is streamed through Kafka
3. Consumer processes the data and makes predictions
4. Results are stored in MongoDB
5. Flask backend serves the processed data
6. Frontend visualizes the data on an interactive map

## API Documentation

- GET `/`: Main dashboard
- GET `/api/weather`: Get latest weather data
- GET `/api/predictions`: Get temperature predictions

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
