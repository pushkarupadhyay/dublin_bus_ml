import asyncio
import json
from datetime import datetime, timezone
from typing import Set, Dict
import logging
from contextlib import asynccontextmanager

import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


#National Transport API
GTFS_REALTIME_API = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles?format=json"

# Your API Keys
API_PRIMARY_KEY = ""  #primary key
API_SECONDARY_KEY = ""  #backup key

#Request settings
FETCH_INTERVAL = 30  #Fetch from API every 10 seconds
BROADCAST_INTERVAL = 5  #Broadcast to clients every 2 seconds
REQUEST_TIMEOUT = 15  #API request timeout

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


#Vehicle Cache
class VehicleCache:
    
    def __init__(self):
        self.vehicles: Dict[str, dict] = {}
        self.last_update = datetime.now(timezone.utc)
        self.last_api_fetch = None
        self.total_fetches = 0
        self.fetch_errors = 0
    
    def update_vehicles(self, vehicles_list: list):
        #Update all vehicles at once
        self.vehicles.clear()
        for vehicle in vehicles_list:
            vehicle_id = vehicle.get('vehicle_id')
            if vehicle_id:
                self.vehicles[vehicle_id] = vehicle
        
        self.last_update = datetime.now(timezone.utc)
        self.last_api_fetch = datetime.now(timezone.utc)
        self.total_fetches += 1
    
    def get_all_vehicles(self) -> list:
        #Get all cached vehicles
        return list(self.vehicles.values())
    
    def get_count(self) -> int:
        #Get vehicle count
        return len(self.vehicles)
    
    def get_stats(self) -> dict:
        #Get cache statistics
        return {
            "count": len(self.vehicles),
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "last_api_fetch": self.last_api_fetch.isoformat() if self.last_api_fetch else None,
            "total_fetches": self.total_fetches,
            "fetch_errors": self.fetch_errors
        }

vehicle_cache = VehicleCache()

#connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"Client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        #Broadcast message to all connected clients
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.debug(f"Error sending to client: {e}")
                disconnected.add(connection)
        
        self.active_connections -= disconnected

manager = ConnectionManager()


#Fetch Vehicles from GTFS API
async def fetch_vehicles_from_api() -> list:
    #Fetch vehicle positions from National Transport API 
    try:
        #Headers with API key authentication
        headers = {
            "Cache-Control": "no-cache",
            "x-api-key": API_PRIMARY_KEY,
            "User-Agent": "DublinBusMonitor/3.0",
            "Accept": "application/json"
        }
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(GTFS_REALTIME_API, headers=headers) as response:
                if response.status == 401:
                    logger.error("API authentication failed: check your API keys")
                    vehicle_cache.fetch_errors += 1
                    return []
                
                if response.status == 403:
                    logger.error("API access forbidden:check API key permissions")
                    vehicle_cache.fetch_errors += 1
                    return []
                
                if response.status != 200:
                    logger.error(f"API returned status {response.status}")
                    vehicle_cache.fetch_errors += 1
                    return []
                
                data = await response.json()
                
                #Parse vehicle positions
                vehicles = []
                
                if 'entity' in data:
                    for entity in data['entity']:
                        if 'vehicle' in entity:
                            vehicle = entity['vehicle']
                            
                            position = vehicle.get('position', {})
                            trip = vehicle.get('trip', {})
                            vehicle_desc = vehicle.get('vehicle', {})
                            
                            lat = position.get('latitude')
                            lng = position.get('longitude')
                            
                            if lat and lng:
                                vehicle_data = {
                                    "entity_id": entity.get('id'),
                                    "trip_id": trip.get('trip_id'),
                                    "route_id": trip.get('route_id'),
                                    "start_date": trip.get('start_date'),
                                    "schedule_relationship": trip.get('schedule_relationship'),
                                    "vehicle_id": vehicle_desc.get('id'),
                                    "vehicle_label": vehicle_desc.get('label'),
                                    "latitude": float(lat),
                                    "longitude": float(lng),
                                    "bearing": position.get('bearing'),
                                    "speed": position.get('speed'),
                                    "timestamp": vehicle.get('timestamp', int(datetime.now(timezone.utc).timestamp())),
                                    "current_status": vehicle.get('current_status'),
                                    "current_stop_sequence": vehicle.get('current_stop_sequence'),
                                    "stop_id": vehicle.get('stop_id'),
                                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                                    "status": "ON_TIME"
                                }
                                vehicles.append(vehicle_data)
                
                logger.info(f"Fetched {len(vehicles)} vehicles from API")
                return vehicles
    
    except asyncio.TimeoutError:
        logger.error(f"API request timeout after {REQUEST_TIMEOUT}s")
        vehicle_cache.fetch_errors += 1
        return []
    except aiohttp.ClientConnectorError as e:
        logger.error(f"Connection error: {e}")
        vehicle_cache.fetch_errors += 1
        return []
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error: {e}")
        vehicle_cache.fetch_errors += 1
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        vehicle_cache.fetch_errors += 1
        return []


async def fetch_vehicles_task():
    #Background task which continuously fetch vehicles from API
    logger.info(f"Starting API fetch task (every {FETCH_INTERVAL}s)")
    logger.info(f"API URL: {GTFS_REALTIME_API}")
    
    iteration = 0
    
    while True:
        try:
            iteration += 1
            logger.info(f"Fetch iteration #{iteration}")
            
            vehicles = await fetch_vehicles_from_api()
            
            if vehicles:
                vehicle_cache.update_vehicles(vehicles)
                logger.info(f" Cached {len(vehicles)} vehicles")
            else:
                logger.warning(" No vehicles fetched this iteration")
            
            await asyncio.sleep(FETCH_INTERVAL)
        
        except Exception as e:
            logger.error(f" Error in fetch task: {e}")
            await asyncio.sleep(FETCH_INTERVAL)

#broadcasting to client
async def broadcast_vehicles_task():
    #Background task to broadcast cached vehicles to WebSocket clients
    logger.info(f"Starting broadcast task (every {BROADCAST_INTERVAL}s)")
    
    while True:
        try:
            if len(manager.active_connections) > 0:
                vehicles = vehicle_cache.get_all_vehicles()
                count = vehicle_cache.get_count()
                
                message = {
                    "type": "VEHICLES_UPDATE",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "count": count,
                    "vehicles": vehicles
                }
                
                await manager.broadcast(message)
                logger.info(f"Broadcasted {count} vehicles to {len(manager.active_connections)} clients")
            
            await asyncio.sleep(BROADCAST_INTERVAL)
        
        except Exception as e:
            logger.error(f"Error in broadcast task: {e}")
            await asyncio.sleep(BROADCAST_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting WebSocket server (Direct API mode)...")
    
    fetch_task = asyncio.create_task(fetch_vehicles_task())
    broadcast_task = asyncio.create_task(broadcast_vehicles_task())
    
    logger.info("WebSocket server started successfully")
    
    yield
    
    logger.info("Shutting down")
    fetch_task.cancel()
    broadcast_task.cancel()
    
    try:
        await fetch_task
    except asyncio.CancelledError:
        pass
    
    try:
        await broadcast_task
    except asyncio.CancelledError:
        pass

#FastAPI
app = FastAPI(
    title="Dublin Bus WebSocket Server by Direct API",
    version="3.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#WebSocket Endpoint

@app.websocket("/ws/vehicles")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    
    try:
        vehicles = vehicle_cache.get_all_vehicles()
        
        await websocket.send_json({
            "type": "INITIAL_DATA",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(vehicles),
            "vehicles": vehicles
        })
        
        logger.info(f"📨 Sent {len(vehicles)} vehicles to new client")
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "HEARTBEAT"})
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


#REST API Endpoints

@app.get("/")
async def root():
    return {
        "service": "Dublin Bus WebSocket Server (Direct API)",
        "version": "3.0.0",
        "mode": "direct_api_fetch",
        "api_url": GTFS_REALTIME_API,
        "fetch_interval": f"{FETCH_INTERVAL}s",
        "endpoints": {
            "websocket": "/ws/vehicles",
            "rest_api": "/api/vehicles/all",
            "health": "/health",
            "stats": "/stats"
        }
    }

@app.get("/api/vehicles/all")
async def get_all_vehicles():
    vehicles = vehicle_cache.get_all_vehicles()
    return {
        "success": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "count": len(vehicles),
        "vehicles": vehicles
    }

@app.get("/api/vehicles/count")
async def get_vehicle_count():
    return {
        "success": True,
        "count": vehicle_cache.get_count(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health")
async def health_check():
    stats = vehicle_cache.get_stats()
    return {
        "status": "healthy",
        "service": "websocket_direct_api",
        "active_connections": len(manager.active_connections),
        "cached_vehicles": stats["count"],
        "last_api_fetch": stats["last_api_fetch"],
        "total_fetches": stats["total_fetches"],
        "fetch_errors": stats["fetch_errors"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/stats")
async def get_stats():
    stats = vehicle_cache.get_stats()
    return {
        "success": True,
        "cache": stats,
        "connections": {"active": len(manager.active_connections)},
        "config": {
            "api_url": GTFS_REALTIME_API,
            "fetch_interval": FETCH_INTERVAL,
            "broadcast_interval": BROADCAST_INTERVAL
        }
    }

@app.post("/api/refresh")
async def force_refresh():
    logger.info("Manual refresh triggered")
    vehicles = await fetch_vehicles_from_api()
    
    if vehicles:
        vehicle_cache.update_vehicles(vehicles)
        return {"success": True, "count": len(vehicles)}
    else:
        return {"success": False, "count": 0}


#main
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
