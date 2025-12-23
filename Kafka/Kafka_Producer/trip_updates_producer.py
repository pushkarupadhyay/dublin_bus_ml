#Fetcheing Trip Updates from TFI GTFS Real time API Data and records

import json
import logging
import requests
import time
from kafka import KafkaProducer
from datetime import datetime, timezone
import os

#logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#Kafka config:
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") #my default broker
KAFKA_TOPIC = "trip_updates"                                #giving topic name

#TFI API config
TFI_TRIPUPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates?format=json"     #trip updates endpoint
TFI_API_KEY = os.getenv("TFI_API_KEY", "")                 #my default primary API key

FETCH_INTERVAL = 10  # Fetching every 10 seconds

#function for creating kafka producer and connecting to broker
def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            compression_type='gzip',   #compressing messages to reduce size
            acks='all', #giving all acknowledgments for reliability
            retries=3,  #3 maximum retries we are following here
            batch_size=16384,  #taking smaller 16kb batch size
            max_in_flight_requests_per_connection=1  #Sequential sending without batching issues
        )
        logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

#function for fetching trip updates from TFI GTFS Real time API
def fetch_trip_updates():
    try:
        headers = {
            "x-api-key": TFI_API_KEY,
            "Accept": "application/json"
        }
        
        response = requests.get(TFI_TRIPUPDATES_URL, headers=headers, timeout=10)
        
        if response.status_code == 200:  #taking successful response
            data = response.json()  #taking json response
            return data
        elif response.status_code == 429:  #handling rate limiting
            logger.warning("API rate limited (429):waiting longer before retry")
            return None
        else:
            logger.warning(f"API returned {response.status_code}")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching trip updates: {e}")
        return None

def flatten_stop_updates(trip_id, route_id, direction_id, start_date, start_time,
                         schedule_relationship, vehicle_id, trip_timestamp,
                         stop_time_updates):
    """Flatten nested stop_time_updates into individual records"""
    flattened = []
    
    for stop_update in stop_time_updates:
        try:
            arrival_delay = stop_update.get('arrival', {}).get('delay')
            departure_delay = stop_update.get('departure', {}).get('delay')
            
            #Skip if no delay information
            if arrival_delay is None and departure_delay is None:
                continue
            
            stop_record = {
                #Trip Information
                'trip_id': trip_id,
                'route_id': route_id,
                'direction_id': direction_id,
                'start_date': start_date,
                'start_time': start_time,
                'schedule_relationship': schedule_relationship,
                
                #Stop Information
                'stop_sequence': stop_update.get('stop_sequence'),
                'stop_id': stop_update.get('stop_id'),
                'stop_schedule_relationship': stop_update.get('schedule_relationship'),
                
                #Delays (in seconds) - our ML target col
                'arrival_delay': arrival_delay,
                'departure_delay': departure_delay,
                
                #Times
                'arrival_time': stop_update.get('arrival', {}).get('time'),
                'departure_time': stop_update.get('departure', {}).get('time'),
                
                #Vehicle ID
                'vehicle_id': vehicle_id,
                
                #Timestamps
                'trip_timestamp': trip_timestamp,
                'ingestion_timestamp': datetime.now(timezone.utc).isoformat(),
                
                #Data source
                'source': 'tfi_trip_updates_api'
            }
            
            flattened.append(stop_record)
        
        except Exception as e:
            logger.debug(f"Error processing stop update: {e}")
            continue
    
    return flattened

def parse_trip_update(entity):
    try:
        trip_update = entity.get('trip_update', {})
        trip_data = trip_update.get('trip', {})
        stop_time_updates = trip_update.get('stop_time_update', [])
        vehicle_data = trip_update.get('vehicle', {})
        
        if not trip_data or not stop_time_updates:
            return []
        
        #Extract trip-level information
        trip_id = trip_data.get('trip_id')
        route_id = trip_data.get('route_id')
        direction_id = trip_data.get('direction_id')
        start_date = trip_data.get('start_date')
        start_time = trip_data.get('start_time')
        schedule_relationship = trip_data.get('schedule_relationship')
        vehicle_id = vehicle_data.get('id')
        trip_timestamp = trip_update.get('timestamp')
        
        #Validate required fields
        if not all([trip_id, route_id, trip_timestamp]):
            return []
        
        #Flatten stop updates
        flattened = flatten_stop_updates(
            trip_id=trip_id,
            route_id=route_id,
            direction_id=direction_id,
            start_date=start_date,
            start_time=start_time,
            schedule_relationship=schedule_relationship,
            vehicle_id=vehicle_id,
            trip_timestamp=trip_timestamp,
            stop_time_updates=stop_time_updates
        )
        
        return flattened
    
    except Exception as e:
        logger.debug(f"Error parsing trip_update entity: {e}")
        return []

def main():
    logger.info("Starting TFI Trip Updates Producer")
    logger.info(f"API URL: {TFI_TRIPUPDATES_URL}")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Fetch Interval: {FETCH_INTERVAL} seconds\n")
    
    producer = create_kafka_producer()
    
    request_count = 0
    stop_update_count = 0
    trip_update_count = 0
    error_count = 0
    
    try:
        logger.info("Starting to fetch trip updates\n")
        
        while True:
            try:
                #Fetch data
                response = fetch_trip_updates()
                
                if response is None:
                    error_count += 1
                    time.sleep(FETCH_INTERVAL)
                    continue
                
                request_count += 1
                header = response.get('header', {})
                entities = response.get('entity', [])
                feed_timestamp = header.get('timestamp')
                
                logger.info(f"\n[Request #{request_count}] Feed timestamp: {feed_timestamp}")
                logger.info(f"Processing {len(entities)} trips...")
                
                batch_count = 0
                trip_count = 0
                
                for entity in entities:
                    flattened = parse_trip_update(entity)
                    
                    for stop_record in flattened:
                        #Add feed timestamp
                        stop_record['feed_timestamp'] = feed_timestamp
                        
                        #Send to Kafka - SYNCHRONOUS (no callbacks)
                        try:
                            producer.send(KAFKA_TOPIC, value=stop_record).get(timeout=5)
                        except Exception as send_err:
                            logger.debug(f"Send error (will retry): {send_err}")
                            continue
                        
                        batch_count += 1
                        stop_update_count += 1
                        
                        #Log sample data (first 3 only)
                        if batch_count <= 3:
                            arr_delay = stop_record.get('arrival_delay', 'N/A')
                            dep_delay = stop_record.get('departure_delay', 'N/A')
                            trip_id = stop_record.get('trip_id', 'N/A')
                            stop_id = stop_record.get('stop_id', 'N/A')
                            logger.info(f"  Trip: {str(trip_id)[:25]:25} | Stop: {str(stop_id)[:10]:10} | Arr: {str(arr_delay):8} | Dep: {str(dep_delay):8}")
                    
                    if flattened:
                        trip_count += 1
                
                if batch_count > 3:
                    logger.info(f"   and {batch_count - 3} more stop updates")
                
                logger.info(f"Sent {batch_count} stop updates from {trip_count} trips to Kafka\n")
                trip_update_count += trip_count
                
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
        logger.info(f"Total trips processed: {trip_update_count}")
        logger.info(f"Total stop updates sent: {stop_update_count}")
        logger.info(f"Total errors: {error_count}")
    
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()
