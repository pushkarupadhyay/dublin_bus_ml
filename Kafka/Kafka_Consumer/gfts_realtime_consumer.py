
#Consumeer of all 3 Kafka producers and load it into MongoDB
#vehicle_positions, trip_updates, weather_data

import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime, timezone
import os

#logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "dublin_bus_db"

#Kafka topics
TOPICS = [
    "vehicle_positions",
    "trip_updates",
    "weather_data"
]

#Creating MongoDB collections for each kafka producers
COLLECTIONS = {
    "vehicle_positions": "vehicles_raw",
    "trip_updates": "trip_updates_raw",
    "weather_data": "weather_raw"
}

#functions for creating connections, saving to MongoDB, and creating indexes
def create_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            group_id="dublin_bus_consumer_group",
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            max_poll_records=100
        )
        logger.info(f"Connected to Kafka broker: {KAFKA_BROKER}")
        logger.info(f"Subscribed for topics: {TOPICS}")
        return consumer
    except Exception as e:                                                #exceptaional handling
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

#function for crrating MongoDB connection
def create_mongodb_connection():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        logger.info(f"Connected my MongoDB: {MONGO_URI}")
        logger.info(f"Using database: {MONGO_DB}")
        
        #Creating indexes for fast queries and good performance 
        create_indexes(db)
        
        return client, db
    except Exception as e:
        logger.error(f"Failed to connect to my MongoDB: {e}")
        raise

#function for creating indexes for each collection for fast query
def create_indexes(db):
    try:
        #Vehicle positions indexes
        db[COLLECTIONS["vehicle_positions"]].create_index("trip_id")
        db[COLLECTIONS["vehicle_positions"]].create_index("vehicle_timestamp")
        db[COLLECTIONS["vehicle_positions"]].create_index("route_id")
        logger.info("Indexes created for vehicle_positions")
        
        #Trip updates indexes
        db[COLLECTIONS["trip_updates"]].create_index("trip_id")
        db[COLLECTIONS["trip_updates"]].create_index("stop_id")
        db[COLLECTIONS["trip_updates"]].create_index("trip_timestamp")
        db[COLLECTIONS["trip_updates"]].create_index("arrival_delay")
        logger.info("Indexes created for trip_updates")
        
        #Weather indexes
        db[COLLECTIONS["weather_data"]].create_index("observation_time")
        logger.info("Indexes created for weather_data")
    except Exception as e:
        logger.debug(f"Index creation note: {e}")

#function for saving to MongoDB
def save_to_mongodb(db, topic, message):
    try:
        collection_name = COLLECTIONS.get(topic)
        
        if not collection_name:
            logger.warning(f"Unknown topic: {topic}")
            return False
        
        collection = db[collection_name]
        
        #Adding MongoDB metadata
        message['_created_at'] = datetime.now(timezone.utc)
        
        #Inserting into collection
        result = collection.insert_one(message)
        
        return result.inserted_id is not None
    
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {e}")
        return False
#function for calling main
def main():
    logger.info("=" * 50)
    logger.info("Starting GTFS Real time Consumer")
    logger.info("=" * 50)
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topics: {TOPICS}")
    logger.info(f"MongoDB URI: {MONGO_URI}")
    logger.info(f"MongoDB DB: {MONGO_DB}\n")
    
    consumer = create_kafka_consumer()
    client, db = create_mongodb_connection()
    
    message_counts = {topic: 0 for topic in TOPICS}
    error_count = 0
    
    try:
        logger.info("Starting to consume messages...\n")
        
        for message in consumer:
            try:
                topic = message.topic
                value = message.value
                
                #Saving to MongoDB
                saved = save_to_mongodb(db, topic, value)
                
                if saved:
                    message_counts[topic] += 1
                    
                    #Log sample every 100 messages per topic
                    if message_counts[topic] % 100 == 0:
                        logger.info(f"{topic}: {message_counts[topic]} messages is saved")
                else:
                    error_count += 1
            
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing message: {e}", exc_info=False)
                continue
    
    except KeyboardInterrupt:
        logger.info(f"\n\n{'=' * 80}")
        logger.info("Consumer stopped by user")
        logger.info(f"{'=' * 80}")
        for topic, count in message_counts.items():
            logger.info(f"{topic}: {count} messages saved")
        logger.info(f"Total errors: {error_count}")
    
    finally:
        consumer.close()
        client.close()
        logger.info("Connections closed")

if __name__ == "__main__":
    main()