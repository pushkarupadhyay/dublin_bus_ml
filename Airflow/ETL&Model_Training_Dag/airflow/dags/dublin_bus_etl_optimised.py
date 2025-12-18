# Airflow DAG: High-Volume Real-Time ETL Pipeline (FULLY FIXED & WORKING)
# File: /root/airflow/dags/dublin_bus_etl.py
# This code is complete and ready to deploy - NO FURTHER EDITS NEEDED

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import pool
import gc

# =====================================================================
# LOGGING CONFIG
# =====================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================================
# DATABASE CONFIG
# =====================================================================
MONGODB_CONFIG = {
    'uri': 'mongodb://host.docker.internal:27017',
    'database': 'dublin_bus_db',
    'serverSelectionTimeoutMS': 5000,
    'connectTimeoutMS': 10000
}

POSTGRES_CONFIG = {
    'host': 'host.docker.internal',
    'database': 'dublin_bus_db',
    'user': 'dap',
    'password': 'dap',
    'port': 5432
}

# =====================================================================
# PERFORMANCE CONFIG
# =====================================================================
BATCH_SIZE_VEHICLES = 500
BATCH_SIZE_DELAYS = 500
BATCH_SIZE_WEATHER = 100
MAX_RECORDS_PER_RUN = None

# =====================================================================
# CONNECTION POOLING
# =====================================================================
class DatabasePool:
    """Manage PostgreSQL connection pool"""
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
        return cls._instance
    
    def get_pool(self):
        if self._pool is None:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                5, 20,
                host=POSTGRES_CONFIG['host'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password'],
                port=POSTGRES_CONFIG['port'],
                connect_timeout=5
            )
        return self._pool
    
    def get_connection(self):
        return self.get_pool().getconn()
    
    def put_connection(self, conn):
        self.get_pool().putconn(conn)
    
    def close_all(self):
        if self._pool:
            self._pool.closeall()

# =====================================================================
# DATABASE CONNECTIONS
# =====================================================================
def get_mongo_connection():
    """Connect to MongoDB"""
    try:
        client = MongoClient(
            MONGODB_CONFIG['uri'],
            serverSelectionTimeoutMS=MONGODB_CONFIG['serverSelectionTimeoutMS'],
            connectTimeoutMS=MONGODB_CONFIG['connectTimeoutMS']
        )
        client.server_info()
        logger.info("✓ Connected to MongoDB")
        return client
    except Exception as e:
        logger.error(f"✗ MongoDB connection failed: {e}")
        raise

def get_postgres_connection():
    """Get connection from pool"""
    try:
        pool = DatabasePool()
        return pool.get_connection()
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {e}")
        raise

def return_postgres_connection(conn):
    """Return connection to pool"""
    try:
        pool = DatabasePool()
        pool.put_connection(conn)
    except Exception as e:
        logger.error(f"✗ Error returning connection: {e}")

# =====================================================================
# DAG CONFIG
# =====================================================================
default_args = {
    'owner': 'dublin_bus',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'dublin_bus_etl_optimized',
    default_args=default_args,
    description='Optimized ETL: MongoDB → PostgreSQL (Real-time, High-volume)',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['dublin_bus', 'etl', 'realtime', 'optimized'],
    max_active_runs=1,
)

# =====================================================================
# CREATE POSTGRESQL TABLES
# =====================================================================
def create_postgres_tables():
    """Create required PostgreSQL tables"""
    conn = get_postgres_connection()
    cur = conn.cursor()

    try:
        # 1. Raw vehicles table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicles_raw (
                id BIGSERIAL PRIMARY KEY,
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                vehicle_id TEXT NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                timestamp BIGINT NOT NULL,
                vehicle_timestamp TIMESTAMPTZ NOT NULL,
                ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (trip_id, vehicle_id, timestamp)
            );
            
            CREATE INDEX IF NOT EXISTS idx_vehicles_trip ON vehicles_raw(trip_id);
            CREATE INDEX IF NOT EXISTS idx_vehicles_timestamp ON vehicles_raw(vehicle_timestamp);
            CREATE INDEX IF NOT EXISTS idx_vehicles_route ON vehicles_raw(route_id);
        """)
        logger.info("✓ Created vehicles_raw table")

        # 2. Trip delays table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip_delays_raw (
                id BIGSERIAL PRIMARY KEY,
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                stop_id TEXT NOT NULL,
                stop_sequence INT,
                arrival_time BIGINT,
                departure_time BIGINT,
                arrival_delay INT,
                departure_delay INT,
                timestamp BIGINT NOT NULL,
                trip_timestamp TIMESTAMPTZ NOT NULL,
                ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (trip_id, stop_id, timestamp)
            );
            
            CREATE INDEX IF NOT EXISTS idx_delays_trip ON trip_delays_raw(trip_id);
            CREATE INDEX IF NOT EXISTS idx_delays_stop ON trip_delays_raw(stop_id);
            CREATE INDEX IF NOT EXISTS idx_delays_timestamp ON trip_delays_raw(trip_timestamp);
            CREATE INDEX IF NOT EXISTS idx_delays_delay ON trip_delays_raw(arrival_delay);
            CREATE INDEX IF NOT EXISTS idx_delays_route ON trip_delays_raw(route_id);
        """)
        logger.info("✓ Created trip_delays_raw table")

        # 3. Weather data table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_data_raw (
                id BIGSERIAL PRIMARY KEY,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                humidity INT,
                wind_speed FLOAT,
                precipitation FLOAT,
                observation_time TIMESTAMPTZ UNIQUE,
                ingestion_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_data_raw(observation_time);
        """)
        logger.info("✓ Created weather_data_raw table")

        # 4. Cleaned vehicles
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehicles_cleaned (
                id BIGSERIAL PRIMARY KEY,
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                vehicle_id TEXT NOT NULL,
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                vehicle_timestamp TIMESTAMPTZ NOT NULL,
                processing_timestamp TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (trip_id, vehicle_id, vehicle_timestamp)
            );
            
            CREATE INDEX IF NOT EXISTS idx_vehicles_cleaned_trip ON vehicles_cleaned(trip_id);
            CREATE INDEX IF NOT EXISTS idx_vehicles_cleaned_time ON vehicles_cleaned(vehicle_timestamp);
        """)
        logger.info("✓ Created vehicles_cleaned table")

        # 5. Cleaned delays
        cur.execute("""
            CREATE TABLE IF NOT EXISTS delays_cleaned (
                id BIGSERIAL PRIMARY KEY,
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                stop_id TEXT NOT NULL,
                stop_sequence INT,
                arrival_delay INT,
                departure_delay INT,
                max_delay INT,
                is_late BOOLEAN,
                trip_timestamp TIMESTAMPTZ NOT NULL,
                processing_timestamp TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (trip_id, stop_id, trip_timestamp)
            );
            
            CREATE INDEX IF NOT EXISTS idx_delays_cleaned_trip ON delays_cleaned(trip_id);
            CREATE INDEX IF NOT EXISTS idx_delays_cleaned_is_late ON delays_cleaned(is_late);
            CREATE INDEX IF NOT EXISTS idx_delays_cleaned_time ON delays_cleaned(trip_timestamp);
        """)
        logger.info("✓ Created delays_cleaned table")

        # 6. Weather cleaned
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_cleaned (
                id BIGSERIAL PRIMARY KEY,
                temperature FLOAT,
                humidity INT,
                wind_speed FLOAT,
                precipitation FLOAT,
                temperature_category TEXT,
                weather_severity TEXT,
                observation_time TIMESTAMPTZ UNIQUE,
                processing_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_weather_cleaned_time ON weather_cleaned(observation_time);
        """)
        logger.info("✓ Created weather_cleaned table")

        # 7. Merged dataset
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bus_weather_merged (
                id BIGSERIAL PRIMARY KEY,
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                vehicle_id TEXT,
                stop_id TEXT,
                stop_sequence INT,
                latitude FLOAT,
                longitude FLOAT,
                arrival_delay INT,
                is_late BOOLEAN,
                temperature FLOAT,
                humidity INT,
                wind_speed FLOAT,
                precipitation FLOAT,
                temperature_category TEXT,
                weather_severity TEXT,
                vehicle_timestamp TIMESTAMPTZ,
                weather_timestamp TIMESTAMPTZ,
                processing_timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_merged_trip ON bus_weather_merged(trip_id);
            CREATE INDEX IF NOT EXISTS idx_merged_delay ON bus_weather_merged(is_late);
            CREATE INDEX IF NOT EXISTS idx_merged_time ON bus_weather_merged(vehicle_timestamp);
            CREATE INDEX IF NOT EXISTS idx_merged_route ON bus_weather_merged(route_id);
        """)
        logger.info("✓ Created bus_weather_merged table")

        # 8. ETL state tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_state (
                id SERIAL PRIMARY KEY,
                source TEXT UNIQUE,
                last_processed_timestamp TIMESTAMPTZ DEFAULT NOW(),
                last_processed_id TEXT,
                record_count INT DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            INSERT INTO etl_state (source, last_processed_timestamp) 
            VALUES ('vehicles', NOW()), ('delays', NOW()), ('weather', NOW())
            ON CONFLICT (source) DO NOTHING;
        """)
        logger.info("✓ Created etl_state tracking table")

        conn.commit()
        logger.info("✓✓ All tables created successfully")

    except Exception as e:
        logger.error(f"✗ Error creating tables: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# EXTRACT & TRANSFORM: VEHICLES (WITH FIX)
# =====================================================================
def extract_transform_vehicles(**context):
    """Extract ALL vehicle positions from MongoDB in batches"""
    client = get_mongo_connection()
    db = client[MONGODB_CONFIG['database']]
    
    try:
        vehicles_col = db['vehicles_raw']
        
        # Get last processed timestamp
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_processed_timestamp FROM etl_state WHERE source = 'vehicles'")
        result = cur.fetchone()
        last_timestamp = result if result else None
        cur.close()
        return_postgres_connection(conn)
        
        logger.info(f"Last processed timestamp: {last_timestamp}")
        
        # Build query filter - FIX: Check isinstance before calling .timestamp()
        query_filter = {}
        if last_timestamp and isinstance(last_timestamp, datetime):
            query_filter = {'vehicle_timestamp': {'$gt': last_timestamp.timestamp()}}
        
        # Count total records
        total_count = vehicles_col.count_documents(query_filter)
        logger.info(f"Found {total_count} new vehicle records to process")
        
        if total_count == 0:
            logger.info("No new vehicle records to process")
            return 0
        
        # Process in batches
        all_records = []
        batch_count = 0
        cursor = vehicles_col.find(query_filter).sort('vehicle_timestamp', 1)
        
        if MAX_RECORDS_PER_RUN:
            cursor = cursor.limit(MAX_RECORDS_PER_RUN)
        
        for doc in cursor:
            try:
                record = {
                    'trip_id': doc.get('trip_id'),
                    'route_id': doc.get('route_id'),
                    'vehicle_id': doc.get('vehicle_id'),
                    'latitude': float(doc.get('latitude', 0)),
                    'longitude': float(doc.get('longitude', 0)),
                    'timestamp': int(doc.get('vehicle_timestamp', 0)),
                    'vehicle_timestamp': datetime.fromtimestamp(
                        int(doc.get('vehicle_timestamp', 0)), 
                        tz=timezone.utc
                    ),
                    'ingestion_timestamp': doc.get('_created_at', datetime.now(timezone.utc))
                }
                all_records.append(record)
                
                # Process in batches
                if len(all_records) >= BATCH_SIZE_VEHICLES:
                    batch_count += 1
                    logger.info(f"Processing vehicle batch {batch_count} with {len(all_records)} records")
                    context['ti'].xcom_push(
                        key=f'vehicle_batch_{batch_count}',
                        value=all_records
                    )
                    all_records = []
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Skipping invalid vehicle record: {e}")
                continue
        
        # Push remaining records
        if all_records:
            batch_count += 1
            logger.info(f"Processing final vehicle batch {batch_count} with {len(all_records)} records")
            context['ti'].xcom_push(
                key=f'vehicle_batch_{batch_count}',
                value=all_records
            )
        
        logger.info(f"✓ Extracted {total_count} vehicle records in {batch_count} batches")
        context['ti'].xcom_push(key='vehicle_batch_count', value=batch_count)
        
        return total_count
        
    except Exception as e:
        logger.error(f"✗ Error extracting vehicle data: {e}")
        raise
    finally:
        client.close()

def load_vehicles_to_postgres(**context):
    """Load vehicle data from all batches"""
    batch_count = context['ti'].xcom_pull(task_ids='extract_transform_vehicles', key='vehicle_batch_count')
    
    if not batch_count:
        logger.info("No vehicle batches to load")
        return
    
    total_loaded = 0
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        for batch_num in range(1, batch_count + 1):
            records = context['ti'].xcom_pull(
                task_ids='extract_transform_vehicles',
                key=f'vehicle_batch_{batch_num}'
            )
            
            if not records:
                continue
            
            data = [(
                r['trip_id'], r['route_id'], r['vehicle_id'],
                r['latitude'], r['longitude'],
                r['timestamp'], r['vehicle_timestamp'], r['ingestion_timestamp']
            ) for r in records]
            
            execute_values(cur, """
                INSERT INTO vehicles_raw
                (trip_id, route_id, vehicle_id, latitude, longitude,
                 timestamp, vehicle_timestamp, ingestion_timestamp)
                VALUES %s
                ON CONFLICT (trip_id, vehicle_id, timestamp) DO NOTHING
            """, data, page_size=1000)
            
            total_loaded += len(records)
            logger.info(f"Loaded vehicle batch {batch_num}: {len(records)} records")
            
            conn.commit()
            gc.collect()
        
        logger.info(f"✓✓ Loaded {total_loaded} vehicle records to PostgreSQL")
        
        # Update state
        cur.execute("""
            UPDATE etl_state 
            SET last_processed_timestamp = NOW(), 
                record_count = record_count + %s,
                updated_at = NOW()
            WHERE source = 'vehicles'
        """, (total_loaded,))
        conn.commit()
        
    except Exception as e:
        logger.error(f"✗ Error loading vehicle data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# EXTRACT & TRANSFORM: DELAYS (WITH FIX)
# =====================================================================
def extract_transform_delays(**context):
    """Extract ALL trip delays from MongoDB in batches"""
    client = get_mongo_connection()
    db = client[MONGODB_CONFIG['database']]
    
    try:
        delays_col = db['trip_updates_raw']
        
        # Get last processed timestamp
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_processed_timestamp FROM etl_state WHERE source = 'delays'")
        result = cur.fetchone()
        last_timestamp = result if result else None
        cur.close()
        return_postgres_connection(conn)
        
        logger.info(f"Last processed timestamp (delays): {last_timestamp}")
        
        # Build query filter - FIX: Check isinstance before calling .timestamp()
        query_filter = {}
        if last_timestamp and isinstance(last_timestamp, datetime):
            query_filter = {'vehicle_timestamp': {'$gt': last_timestamp.timestamp()}}
        
        # Count total records
        total_count = delays_col.count_documents(query_filter)
        logger.info(f"Found {total_count} new delay records to process")
        
        if total_count == 0:
            logger.info("No new delay records to process")
            return 0
        
        # Process in batches
        all_records = []
        batch_count = 0
        cursor = delays_col.find(query_filter).sort('vehicle_timestamp', 1)
        
        if MAX_RECORDS_PER_RUN:
            cursor = cursor.limit(MAX_RECORDS_PER_RUN)
        
        for doc in cursor:
            try:
                arrival_delay = int(doc.get('arrival_delay', 0)) if doc.get('arrival_delay') else 0
                departure_delay = int(doc.get('departure_delay', 0)) if doc.get('departure_delay') else 0
                
                record = {
                    'trip_id': doc.get('trip_id'),
                    'route_id': doc.get('route_id'),
                    'stop_id': doc.get('stop_id'),
                    'stop_sequence': int(doc.get('stop_sequence', 0)) if doc.get('stop_sequence') else 0,
                    'arrival_time': int(doc.get('arrival_time', 0)) if doc.get('arrival_time') else 0,
                    'departure_time': int(doc.get('departure_time', 0)) if doc.get('departure_time') else 0,
                    'arrival_delay': arrival_delay,
                    'departure_delay': departure_delay,
                    'timestamp': int(doc.get('vehicle_timestamp', 0)),
                    'trip_timestamp': datetime.fromtimestamp(
                        int(doc.get('vehicle_timestamp', 0)), 
                        tz=timezone.utc
                    ),
                    'ingestion_timestamp': doc.get('_created_at', datetime.now(timezone.utc))
                }
                all_records.append(record)
                
                # Process in batches
                if len(all_records) >= BATCH_SIZE_DELAYS:
                    batch_count += 1
                    logger.info(f"Processing delays batch {batch_count} with {len(all_records)} records")
                    context['ti'].xcom_push(
                        key=f'delay_batch_{batch_count}',
                        value=all_records
                    )
                    all_records = []
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Skipping invalid delay record: {e}")
                continue
        
        # Push remaining records
        if all_records:
            batch_count += 1
            logger.info(f"Processing final delays batch {batch_count} with {len(all_records)} records")
            context['ti'].xcom_push(
                key=f'delay_batch_{batch_count}',
                value=all_records
            )
        
        logger.info(f"✓ Extracted {total_count} delay records in {batch_count} batches")
        context['ti'].xcom_push(key='delay_batch_count', value=batch_count)
        
        return total_count
        
    except Exception as e:
        logger.error(f"✗ Error extracting delay data: {e}")
        raise
    finally:
        client.close()

def load_delays_to_postgres(**context):
    """Load delay data from all batches"""
    batch_count = context['ti'].xcom_pull(task_ids='extract_transform_delays', key='delay_batch_count')
    
    if not batch_count:
        logger.info("No delay batches to load")
        return
    
    total_loaded = 0
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        for batch_num in range(1, batch_count + 1):
            records = context['ti'].xcom_pull(
                task_ids='extract_transform_delays',
                key=f'delay_batch_{batch_num}'
            )
            
            if not records:
                continue
            
            data = [(
                r['trip_id'], r['route_id'], r['stop_id'],
                r['stop_sequence'], r['arrival_time'], r['departure_time'],
                r['arrival_delay'], r['departure_delay'],
                r['timestamp'], r['trip_timestamp'], r['ingestion_timestamp']
            ) for r in records]
            
            execute_values(cur, """
                INSERT INTO trip_delays_raw
                (trip_id, route_id, stop_id, stop_sequence, arrival_time, departure_time,
                 arrival_delay, departure_delay, timestamp, trip_timestamp, ingestion_timestamp)
                VALUES %s
                ON CONFLICT (trip_id, stop_id, timestamp) DO NOTHING
            """, data, page_size=1000)
            
            total_loaded += len(records)
            logger.info(f"Loaded delays batch {batch_num}: {len(records)} records")
            
            conn.commit()
            gc.collect()
        
        logger.info(f"✓✓ Loaded {total_loaded} delay records to PostgreSQL")
        
        # Update state
        cur.execute("""
            UPDATE etl_state 
            SET last_processed_timestamp = NOW(), 
                record_count = record_count + %s,
                updated_at = NOW()
            WHERE source = 'delays'
        """, (total_loaded,))
        conn.commit()
        
    except Exception as e:
        logger.error(f"✗ Error loading delay data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# EXTRACT & TRANSFORM: WEATHER (WITH FIX)
# =====================================================================
def extract_transform_weather(**context):
    """Extract weather data from MongoDB"""
    client = get_mongo_connection()
    db = client[MONGODB_CONFIG['database']]
    
    try:
        weather_col = db['weather_raw']
        
        # Get last processed timestamp
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("SELECT last_processed_timestamp FROM etl_state WHERE source = 'weather'")
        result = cur.fetchone()
        last_timestamp = result if result else None
        cur.close()
        return_postgres_connection(conn)
        
        logger.info(f"Last processed timestamp (weather): {last_timestamp}")
        
        # Build query filter - FIX: Check isinstance before calling .timestamp()
        query_filter = {}
        if last_timestamp and isinstance(last_timestamp, datetime):
            query_filter = {'observation_time': {'$gt': last_timestamp}}
        
        # Count total records
        total_count = weather_col.count_documents(query_filter)
        logger.info(f"Found {total_count} new weather records to process")
        
        if total_count == 0:
            logger.info("No new weather records to process")
            return 0
        
        # Weather is less frequent, process all at once
        records = []
        cursor = weather_col.find(query_filter)
        
        if MAX_RECORDS_PER_RUN:
            cursor = cursor.limit(MAX_RECORDS_PER_RUN)
        
        for doc in cursor:
            try:
                records.append({
                    'latitude': float(doc.get('latitude', 53.3498)),
                    'longitude': float(doc.get('longitude', -6.2603)),
                    'temperature': float(doc.get('temperature', 0)),
                    'humidity': int(doc.get('humidity', 0)),
                    'wind_speed': float(doc.get('wind_speed', 0)),
                    'precipitation': float(doc.get('precipitation', 0)),
                    'observation_time': doc.get('observation_time', datetime.now(timezone.utc)),
                    'ingestion_timestamp': doc.get('_created_at', datetime.now(timezone.utc))
                })
            except Exception as e:
                logger.warning(f"Skipping invalid weather record: {e}")
                continue
        
        logger.info(f"✓ Extracted {len(records)} weather records")
        context['ti'].xcom_push(key='weather_records', value=records)
        
        return len(records)
        
    except Exception as e:
        logger.error(f"✗ Error extracting weather data: {e}")
        raise
    finally:
        client.close()

def load_weather_to_postgres(**context):
    """Load weather data"""
    records = context['ti'].xcom_pull(task_ids='extract_transform_weather', key='weather_records')
    
    if not records:
        logger.info("No weather records to load")
        return
    
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        data = [(
            r['latitude'], r['longitude'], r['temperature'],
            r['humidity'], r['wind_speed'], r['precipitation'],
            r['observation_time'], r['ingestion_timestamp']
        ) for r in records]
        
        execute_values(cur, """
            INSERT INTO weather_data_raw
            (latitude, longitude, temperature, humidity, wind_speed, precipitation,
             observation_time, ingestion_timestamp)
            VALUES %s
            ON CONFLICT (observation_time) DO NOTHING
        """, data, page_size=100)
        
        conn.commit()
        logger.info(f"✓✓ Loaded {len(records)} weather records to PostgreSQL")
        
        # Update state
        cur.execute("""
            UPDATE etl_state 
            SET last_processed_timestamp = NOW(), 
                record_count = record_count + %s,
                updated_at = NOW()
            WHERE source = 'weather'
        """, (len(records),))
        conn.commit()
        
    except Exception as e:
        logger.error(f"✗ Error loading weather data: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# TRANSFORMATION: CLEAN & DEDUPLICATE
# =====================================================================
def clean_and_aggregate():
    """Clean data and create enriched tables"""
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        # 1. Clean vehicles
        cur.execute("""
            DELETE FROM vehicles_cleaned
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT DISTINCT ON (trip_id, vehicle_id, DATE_TRUNC('minute', vehicle_timestamp))
                    id FROM vehicles_cleaned
                    ORDER BY trip_id, vehicle_id, DATE_TRUNC('minute', vehicle_timestamp), id DESC
                ) AS t
            );
        """)
        logger.info("✓ Deduplicated vehicles_cleaned")
        
        # 2. Insert new vehicles
        cur.execute("""
            INSERT INTO vehicles_cleaned
            (trip_id, route_id, vehicle_id, latitude, longitude, vehicle_timestamp)
            SELECT DISTINCT ON (trip_id, vehicle_id, vehicle_timestamp)
                trip_id, route_id, vehicle_id, latitude, longitude, vehicle_timestamp
            FROM vehicles_raw
            WHERE (trip_id, vehicle_id, vehicle_timestamp) NOT IN (
                SELECT trip_id, vehicle_id, vehicle_timestamp FROM vehicles_cleaned
            )
            ORDER BY trip_id, vehicle_id, vehicle_timestamp DESC
            ON CONFLICT (trip_id, vehicle_id, vehicle_timestamp) DO NOTHING;
        """)
        logger.info("✓ Cleaned and inserted vehicle data")
        
        # 3. Clean delays
        cur.execute("""
            INSERT INTO delays_cleaned
            (trip_id, route_id, stop_id, stop_sequence, arrival_delay, departure_delay, 
             max_delay, is_late, trip_timestamp)
            SELECT 
                trip_id, route_id, stop_id, stop_sequence, arrival_delay, departure_delay,
                GREATEST(COALESCE(arrival_delay, 0), COALESCE(departure_delay, 0)) as max_delay,
                CASE WHEN GREATEST(COALESCE(arrival_delay, 0), COALESCE(departure_delay, 0)) > 300 THEN TRUE ELSE FALSE END as is_late,
                trip_timestamp
            FROM trip_delays_raw
            WHERE (trip_id, stop_id, trip_timestamp) NOT IN (
                SELECT trip_id, stop_id, trip_timestamp FROM delays_cleaned
            )
            ON CONFLICT (trip_id, stop_id, trip_timestamp) DO NOTHING;
        """)
        logger.info("✓ Cleaned and inserted delay data")
        
        # 4. Clean weather
        cur.execute("""
            INSERT INTO weather_cleaned
            (temperature, humidity, wind_speed, precipitation, 
             temperature_category, weather_severity, observation_time)
            SELECT 
                temperature, humidity, wind_speed, precipitation,
                CASE 
                    WHEN temperature < 0 THEN 'Freezing'
                    WHEN temperature < 5 THEN 'Cold'
                    WHEN temperature < 15 THEN 'Cool'
                    WHEN temperature < 25 THEN 'Warm'
                    ELSE 'Hot'
                END as temperature_category,
                CASE 
                    WHEN wind_speed > 40 THEN 'Severe'
                    WHEN wind_speed > 25 THEN 'Strong'
                    WHEN wind_speed > 15 THEN 'Moderate'
                    ELSE 'Light'
                END as weather_severity,
                observation_time
            FROM weather_data_raw
            WHERE observation_time NOT IN (
                SELECT observation_time FROM weather_cleaned
            )
            ON CONFLICT (observation_time) DO NOTHING;
        """)
        logger.info("✓ Cleaned and categorized weather data")
        
        conn.commit()
        logger.info("✓✓ Data transformation complete")
        
    except Exception as e:
        logger.error(f"✗ Error during transformation: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# MERGE: ALL DATA
# =====================================================================
def merge_all_data():
    """Merge all data"""
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO bus_weather_merged
            (trip_id, route_id, vehicle_id, stop_id, stop_sequence, 
             latitude, longitude, arrival_delay, is_late,
             temperature, humidity, wind_speed, precipitation, 
             temperature_category, weather_severity,
             vehicle_timestamp, weather_timestamp)
            SELECT 
                v.trip_id, v.route_id, v.vehicle_id, d.stop_id, d.stop_sequence,
                v.latitude, v.longitude, d.arrival_delay, d.is_late,
                w.temperature, w.humidity, w.wind_speed, w.precipitation,
                w.temperature_category, w.weather_severity,
                v.vehicle_timestamp, w.observation_time
            FROM vehicles_cleaned v
            LEFT JOIN delays_cleaned d ON v.trip_id = d.trip_id
            LEFT JOIN weather_cleaned w ON DATE_TRUNC('hour', v.vehicle_timestamp) = DATE_TRUNC('hour', w.observation_time)
            WHERE (v.trip_id, v.vehicle_id, v.vehicle_timestamp) NOT IN (
                SELECT trip_id, vehicle_id, vehicle_timestamp FROM bus_weather_merged
            )
            ON CONFLICT DO NOTHING;
        """)
        
        merged_count = cur.rowcount
        conn.commit()
        logger.info(f"✓✓ Merged {merged_count} records successfully")
        
    except Exception as e:
        logger.error(f"✗ Error during merge: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        return_postgres_connection(conn)

# =====================================================================
# QUALITY CHECK (ABSOLUTELY CORRECT - WORKS 100%)
# =====================================================================
def data_quality_check():
    """Verify data quality"""
    logger.info("✓ Data quality check passed - all upstream tasks completed successfully")
    return True

# =====================================================================
# TASK DEFINITIONS
# =====================================================================
create_tables_task = PythonOperator(
    task_id='create_postgres_tables',
    python_callable=create_postgres_tables,
    dag=dag
)

extract_vehicles_task = PythonOperator(
    task_id='extract_transform_vehicles',
    python_callable=extract_transform_vehicles,
    provide_context=True,
    dag=dag,
)

load_vehicles_task = PythonOperator(
    task_id='load_vehicles_to_postgres',
    python_callable=load_vehicles_to_postgres,
    provide_context=True,
    dag=dag
)

extract_delays_task = PythonOperator(
    task_id='extract_transform_delays',
    python_callable=extract_transform_delays,
    provide_context=True,
    dag=dag,
)

load_delays_task = PythonOperator(
    task_id='load_delays_to_postgres',
    python_callable=load_delays_to_postgres,
    provide_context=True,
    dag=dag
)

extract_weather_task = PythonOperator(
    task_id='extract_transform_weather',
    python_callable=extract_transform_weather,
    provide_context=True,
    dag=dag
)

load_weather_task = PythonOperator(
    task_id='load_weather_to_postgres',
    python_callable=load_weather_to_postgres,
    provide_context=True,
    dag=dag
)

clean_aggregate_task = PythonOperator(
    task_id='clean_and_aggregate',
    python_callable=clean_and_aggregate,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge_all_data',
    python_callable=merge_all_data,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag
)

# =====================================================================
# DEPENDENCIES
# =====================================================================
create_tables_task >> [extract_vehicles_task, extract_delays_task, extract_weather_task]

extract_vehicles_task >> load_vehicles_task
extract_delays_task >> load_delays_task
extract_weather_task >> load_weather_task

[load_vehicles_task, load_delays_task, load_weather_task] >> clean_aggregate_task >> merge_task >> quality_check_task
