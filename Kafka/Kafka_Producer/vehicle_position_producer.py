import json
import logging
import requests
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import os
#logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "vehicle_positions"

#TFI API Config
TFI_VEHICLE_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles?format=json"
TFI_API_KEY = os.getenv("TFI_API_KEY", "a8d1f68c87cc441d9ba7fda4bb6989d3")  #calling primary api

FETCH_INTERVAL = 30  #Fetch every 30 seconds

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            max_request_size=52428800,
            compression_type='gzip',
            acks='all',
            retries=3,
            request_timeout_ms=30000
        )
        logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

def fetch_vehicle_positions():
    try:
        headers = {
            "x-api-key": TFI_API_KEY,
            "Accept": "application/json"
        }
        
        response = requests.get(TFI_VEHICLE_URL, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data
        elif response.status_code == 429:
            logger.warning("API rate limited if 429: waiting longer before retry")
            return None
        else:
            logger.warning(f"API returned {response.status_code}")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching vehicle positions: {e}")
        return None

def parse_vehicle_position(entity):
    try:
        vehicle_data = entity.get('vehicle', {})
        trip_data = vehicle_data.get('trip', {})
        position_data = vehicle_data.get('position', {})
        vehicle_id_data = vehicle_data.get('vehicle', {})
        
        if not trip_data or not position_data:
            return None
        
        #Skip if coordinates are missing because coordinates are playing crucial role in project 
        latitude = position_data.get('latitude')
        longitude = position_data.get('longitude')
        
        if latitude is None or longitude is None:
            return None
        
        parsed = {
            #Entity ID
            'entity_id': entity.get('id'),
            
            #Trip Information
            'trip_id': trip_data.get('trip_id'),
            'route_id': trip_data.get('route_id'),
            'direction_id': trip_data.get('direction_id'),
            'start_date': trip_data.get('start_date'),
            'start_time': trip_data.get('start_time'),
            'schedule_relationship': trip_data.get('schedule_relationship'),
            
            #Position Infornation
            'latitude': float(latitude),
            'longitude': float(longitude),
            
            #Vehicle Information
            'vehicle_id': vehicle_id_data.get('id'),
            
            #Timestamps
            'vehicle_timestamp': vehicle_data.get('timestamp'),
            'ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
            
            #Data source
            'source': 'tfi_vehicle_positions_api'
        }
        
        #Validate required fields
        required = ['trip_id', 'latitude', 'longitude', 'vehicle_timestamp']
        if all(parsed.get(field) is not None for field in required):
            return parsed
        
        return None
    
    except Exception as e:
        logger.debug(f"Error parsing vehicle entity: {e}")
        return None

def main():
    logger.info("=" * 80)
    logger.info("Starting TFI Vehicle Positions Producer")
    logger.info("=" * 80)
    logger.info(f"API URL: {TFI_VEHICLE_URL}")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Fetch Interval: {FETCH_INTERVAL} seconds\n")
    
    producer = create_kafka_producer()
    
    request_count = 0
    vehicle_count = 0
    error_count = 0
    
    try:
        logger.info("Starting to fetch vehicle positions...\n")
        
        while True:
            try:
                #Fetch data
                response = fetch_vehicle_positions()
                
                if response is None:
                    error_count += 1
                    time.sleep(FETCH_INTERVAL)
                    continue
                
                request_count += 1
                header = response.get('header', {})
                entities = response.get('entity', [])
                feed_timestamp = header.get('timestamp')
                
                logger.info(f"\n[Request #{request_count}] Feed timestamp: {feed_timestamp}")
                logger.info(f"Processing {len(entities)} vehicles")
                
                batch_count = 0
                for entity in entities:
                    parsed = parse_vehicle_position(entity)
                    
                    if parsed:
                        #Add feed timestamp to parsed data
                        parsed['feed_timestamp'] = feed_timestamp
                        
                        #Send to Kafka
                        producer.send(KAFKA_TOPIC, value=parsed)
                        batch_count += 1
                        vehicle_count += 1
                        
                        #Log sample data (first 3 only)
                        if batch_count <= 3:
                            route_id = parsed.get('route_id', 'N/A')
                            vehicle_id = parsed.get('vehicle_id', 'N/A')
                            lat = parsed.get('latitude', 0)
                            lon = parsed.get('longitude', 0)
                            logger.info(f"  {str(route_id):10} | Vehicle: {str(vehicle_id):15} | Lat: {lat:8.4f} | Lon: {lon:8.4f}")
                
                if batch_count > 3:
                    logger.info(f"  ... and {batch_count - 3} more vehicles")
                
                logger.info(f"Sent {batch_count} vehicles to Kafka\n")
                
                # Wait before next fetch
                time.sleep(FETCH_INTERVAL)
            
            except Exception as e:
                error_count += 1
                logger.error(f"Error in fetch loop: {e}", exc_info=False)
                time.sleep(FETCH_INTERVAL)
                continue
    
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
        logger.info(f"Total API requests: {request_count}")
        logger.info(f"Total vehicles sent: {vehicle_count}")
        logger.info(f"Total errors: {error_count}")
    
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()