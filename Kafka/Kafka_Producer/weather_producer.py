# weather_api_producer.py
# Fetches weather data from Open-Meteo API (FREE, NO KEY NEEDED)
# Sends to Kafka for real-time processing

import json
import logging
import requests
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "weather_data"

# Dublin coordinates
DUBLIN_LAT = 53.3498
DUBLIN_LON = -6.2603

# Weather API (Free, no authentication needed)
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"

FETCH_INTERVAL = 300  # Fetch every 5 minutes (weather changes slowly)

def create_kafka_producer():
    """Create and connect to Kafka broker"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='gzip',
            acks='all',
            retries=3,
            batch_size=16384,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✓ Connected to Kafka broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"✗ Failed to connect to Kafka: {e}")
        raise

def fetch_weather():
    """Fetch weather data from Open-Meteo API (FREE)"""
    try:
        params = {
            'latitude': DUBLIN_LAT,
            'longitude': DUBLIN_LON,
            'current': 'temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m,precipitation',
            'timezone': 'UTC'
        }
        
        response = requests.get(WEATHER_API_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.warning(f"API returned {response.status_code}")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
        return None

def parse_weather(weather_response):
    """Parse weather response into clean record"""
    try:
        current = weather_response.get('current', {})
        
        weather_record = {
            # Location
            'latitude': DUBLIN_LAT,
            'longitude': DUBLIN_LON,
            'location': 'Dublin, Ireland',
            
            # Current conditions
            'temperature': current.get('temperature_2m'),
            'humidity': current.get('relative_humidity_2m'),
            'wind_speed': current.get('wind_speed_10m'),
            'precipitation': current.get('precipitation'),
            'weather_code': current.get('weather_code'),
            
            # Timestamps
            'observation_time': current.get('time'),
            'ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
            
            # Data source
            'source': 'open_meteo_api'
        }
        
        return weather_record
    
    except Exception as e:
        logger.error(f"Error parsing weather: {e}")
        return None

def main():
    logger.info("=" * 80)
    logger.info("Starting Weather Data Producer (Open-Meteo)")
    logger.info("=" * 80)
    logger.info(f"Location: Dublin ({DUBLIN_LAT}, {DUBLIN_LON})")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Fetch Interval: {FETCH_INTERVAL} seconds\n")
    
    producer = create_kafka_producer()
    
    request_count = 0
    weather_count = 0
    error_count = 0
    
    try:
        logger.info("Starting to fetch weather data...\n")
        
        while True:
            try:
                # Fetch weather
                response = fetch_weather()
                
                if response is None:
                    error_count += 1
                    logger.warning("Failed to fetch weather, retrying...")
                    time.sleep(FETCH_INTERVAL)
                    continue
                
                request_count += 1
                
                # Parse weather
                weather_record = parse_weather(response)
                
                if weather_record:
                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, value=weather_record).get(timeout=5)
                    weather_count += 1
                    
                    logger.info(f"\n[Request #{request_count}]")
                    logger.info(f"  ✓ Temperature: {weather_record.get('temperature')}°C")
                    logger.info(f"  ✓ Humidity: {weather_record.get('humidity')}%")
                    logger.info(f"  ✓ Wind Speed: {weather_record.get('wind_speed')} km/h")
                    logger.info(f"  ✓ Precipitation: {weather_record.get('precipitation')} mm")
                    logger.info(f"✓ Sent weather data to Kafka\n")
                
                # Wait before next fetch (weather doesn't change that fast)
                time.sleep(FETCH_INTERVAL)
            
            except Exception as e:
                error_count += 1
                logger.error(f"✗ Error in fetch loop: {e}", exc_info=False)
                time.sleep(FETCH_INTERVAL)
                continue
    
    except KeyboardInterrupt:
        logger.info(f"\n\n{'=' * 80}")
        logger.info("Weather Producer stopped by user")
        logger.info(f"{'=' * 80}")
        logger.info(f"Total API requests: {request_count}")
        logger.info(f"Total weather records sent: {weather_count}")
        logger.info(f"Total errors: {error_count}")
    
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("✓ Kafka producer closed")

if __name__ == "__main__":
    main()