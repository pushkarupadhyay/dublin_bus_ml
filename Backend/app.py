"""
Dublin Bus Delay Prediction API - FastAPI
Complete production-ready API with all endpoints
"""

import os
import glob
from datetime import datetime, timezone
from typing import List, Optional
import numpy as np
import pandas as pd
import joblib
from catboost import CatBoostRegressor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text, func, distinct
from sqlalchemy.orm import Session, sessionmaker
import logging

# =====================================================
# CONFIGURATION
# =====================================================
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", r"C:\Users\pushk\airflow\artifacts")
DB_URI = os.getenv("DB_URI", "postgresql+psycopg2://dap:dap@localhost:5432/dublin_bus_db")
VISUALIZATIONS_DIR = os.path.join(os.getcwd(), "visualizations")
VIDEOS_DIR = os.path.join(os.getcwd(), "videos")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# DATABASE SESSION
# =====================================================
engine = create_engine(DB_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Dependency for database sessions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================
# PYDANTIC MODELS (Response Schemas)
# =====================================================
class RouteInfo(BaseModel):
    route_id: str
    route_short_name: str
    route_long_name: str

class RouteSearchResult(BaseModel):
    route_id: str
    route_short_name: str
    route_long_name: str
    active_vehicles: int

class PredictionRow(BaseModel):
    id: int
    route_id: str
    vehicle_id: Optional[str] = None
    stop_sequence: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    pred_arrival_delay: float
    pred_std: float
    actual_arrival_delay: Optional[float] = None

class PredictResponse(BaseModel):
    model_version: str
    scored_at_utc: str
    route_info: Optional[RouteInfo] = None
    rows: List[PredictionRow]

class RoutesListResponse(BaseModel):
    total_routes: int
    routes: List[RouteInfo]

class Stop(BaseModel):
    stop_id: str
    stop_name: str
    latitude: float
    longitude: float
    distance: Optional[int] = None

class Arrival(BaseModel):
    route_id: str
    route_short_name: str
    destination: str
    scheduled_time: str
    predicted_time: str
    delay: int
    status: str

class VisualizationFile(BaseModel):
    filename: str
    path: str
    type: str

class ModelMetrics(BaseModel):
    model_version: str
    scored_at: str
    samples: int
    mae: float
    rmse: float
    r2: float
    y_mean: float
    y_std: float
    pred_mean: float
    pred_std: float

# =====================================================
# MODEL SERVICE
# =====================================================
class ModelService:
    def __init__(self, artifacts_dir: str):
        self.artifacts_dir = artifacts_dir
        self.meta = None
        self.model_version = None
        self.models: List[CatBoostRegressor] = []

    def load_latest(self):
        """Load latest model bundle"""
        files = glob.glob(os.path.join(self.artifacts_dir, "catboost_bagged_*.joblib"))
        if not files:
            raise FileNotFoundError(f"No catboost_bagged_*.joblib found in {self.artifacts_dir}")
        
        latest = max(files, key=os.path.getmtime)
        self.meta = joblib.load(latest)
        self.model_version = os.path.splitext(os.path.basename(latest))[0]
        
        # Load all CatBoost models
        self.models = []
        for p in self.meta["model_paths"]:
            # Handle path conversion (Linux -> Windows)
            if not os.path.exists(p):
                p = os.path.join(self.artifacts_dir, os.path.basename(p))
            
            m = CatBoostRegressor()
            m.load_model(p)
            self.models.append(m)
        
        logger.info(f"✅ Loaded model: {self.model_version}")
        logger.info(f"✅ Loaded {len(self.models)} CatBoost models")

    def canonicalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix column name typos"""
        df = df.copy()
        for bad, good in self.meta["rename_map"].items():
            if bad in df.columns and good not in df.columns:
                df = df.rename(columns={bad: good})
        return df

    def add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract time features from timestamps"""
        df = df.copy()
        df['vehicle_timestamp'] = pd.to_datetime(df['vehicle_timestamp'], errors='coerce', utc=True)
        df['weather_timestamp'] = pd.to_datetime(df['weather_timestamp'], errors='coerce', utc=True)
        
        df['vehicle_hour'] = df['vehicle_timestamp'].dt.hour.fillna(0).astype(int)
        df['vehicle_dow'] = df['vehicle_timestamp'].dt.dayofweek.fillna(0).astype(int)
        df['weather_hour'] = df['weather_timestamp'].dt.hour.fillna(0).astype(int)
        df['weather_dow'] = df['weather_timestamp'].dt.dayofweek.fillna(0).astype(int)
        
        return df.drop(columns=['vehicle_timestamp', 'weather_timestamp'], errors='ignore')

    def prepare_X(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for prediction"""
        feature_cols = self.meta["feature_cols"]
        cat_cols = self.meta["cat_cols"]
        num_cols = self.meta["num_cols"]
        train_medians = self.meta.get("train_medians", {})
        
        df = self.canonicalize_columns(df_raw)
        df = self.add_time_features(df)
        
        # Ensure all features exist
        for c in feature_cols:
            if c not in df.columns:
                df[c] = np.nan
        
        X = df[feature_cols].copy()
        
        # Handle categorical columns
        for c in cat_cols:
            X[c] = X[c].astype(object).where(X[c].notna(), "missing").astype(str)
        
        # Handle numerical columns
        for c in num_cols:
            X[c] = pd.to_numeric(X[c], errors='coerce')
            fill = train_medians.get(c, float(np.nanmedian(X[c].to_numpy())))
            if np.isnan(fill):
                fill = 0.0
            X[c] = X[c].fillna(fill)
        
        return X

    def predict_mean_std(self, X: pd.DataFrame) -> tuple:
        """Ensemble prediction with uncertainty"""
        all_preds = np.vstack([m.predict(X) for m in self.models])
        return np.mean(all_preds, axis=0), np.std(all_preds, axis=0)

# =====================================================
# GLOBAL INSTANCES
# =====================================================
svc = ModelService(ARTIFACTS_DIR)

# =====================================================
# LIFESPAN (Startup/Shutdown)
# =====================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup, cleanup on shutdown"""
    svc.load_latest()
    yield
    try:
        engine.dispose()
    except Exception:
        pass

# =====================================================
# FASTAPI APP
# =====================================================
app = FastAPI(
    title="Dublin Bus Delay Prediction API",
    version="2.0.0",
    lifespan=lifespan
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# ENDPOINTS - HEALTH & INFO
# =====================================================
@app.get("/health")
def health():
    """Health check"""
    return {
        "status": "ok",
        "service": "fastapi_predictions",
        "model_version": svc.model_version,
        "n_models": len(svc.models),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/")
def root():
    """API root"""
    return {
        "service": "Dublin Bus Delay Prediction API",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "routes": "/routes",
            "routes_search": "/routes/search",
            "predict_latest": "/predict/latest",
            "predict_route": "/predict/route/{route_short_name}",
            "stops_nearby": "/stops/nearby",
            "stop_arrivals": "/stops/{stop_id}/arrivals",
            "visualizations": "/visualizations/list",
            "model_performance": "/model/performance"
        }
    }

# =====================================================
# ENDPOINTS - ROUTES
# =====================================================
@app.get("/routes", response_model=RoutesListResponse)
def get_all_routes():
    """Get all available bus routes"""
    try:
        query = """
            SELECT route_id, route_short_name, route_long_name
            FROM routes
            ORDER BY route_short_name
        """
        df = pd.read_sql(query, engine)
        
        routes = [
            RouteInfo(
                route_id=r["route_id"],
                route_short_name=r["route_short_name"],
                route_long_name=r["route_long_name"]
            )
            for _, r in df.iterrows()
        ]
        
        return RoutesListResponse(total_routes=len(routes), routes=routes)
    
    except Exception as e:
        logger.error(f"Error fetching routes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/routes/search")
def search_routes(query: str = Query(..., min_length=1)):
    """Search for routes by route_short_name"""
    try:
        # Use SQL query with ILIKE for case-insensitive search
        sql_query = text("""
            SELECT 
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                COUNT(DISTINCT bw.vehicle_id) as active_vehicles
            FROM routes r
            LEFT JOIN bus_weather_merged bw ON r.route_id = bw.route_id
            WHERE r.route_short_name ILIKE :query
                AND bw.vehicle_timestamp >= NOW() - INTERVAL '30 minutes'
            GROUP BY r.route_id, r.route_short_name, r.route_long_name
            ORDER BY r.route_short_name
            LIMIT 10
        """)
        
        with engine.connect() as conn:
            result = conn.execute(sql_query, {"query": f"%{query}%"})
            rows = result.fetchall()
        
        results = [
            {
                "route_id": row[0],
                "route_short_name": row[1],
                "route_long_name": row[2],
                "active_vehicles": int(row[3]) if row[3] else 0
            }
            for row in rows
        ]
        
        logger.info(f"✅ Found {len(results)} routes for query: {query}")
        
        return {
            "success": True,
            "routes": results,
            "count": len(results)
        }
    
    except Exception as e:
        logger.error(f"❌ Error searching routes: {e}")
        return {
            "success": False,
            "routes": [],
            "error": str(e)
        }

# =====================================================
# ENDPOINTS - PREDICTIONS
# =====================================================
@app.get("/predict/latest", response_model=PredictResponse)
def predict_latest(
    limit: int = Query(10, ge=1, le=500),
    write_db: bool = Query(False)
):
    """Predict delays for latest rows (all routes)"""
    try:
        query = f"""
            SELECT *
            FROM bus_weather_merged
            WHERE stop_id IS NOT NULL
              AND stop_sequence IS NOT NULL
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND vehicle_timestamp IS NOT NULL
              AND weather_timestamp IS NOT NULL
            ORDER BY id DESC
            LIMIT {limit}
        """
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return PredictResponse(
                model_version=svc.model_version,
                scored_at_utc=datetime.now(timezone.utc).isoformat(),
                route_info=None,
                rows=[]
            )
        
        X = svc.prepare_X(df)
        pred_mean, pred_std = svc.predict_mean_std(X)
        
        rows = [
            PredictionRow(
                id=int(df['id'].iloc[i]),
                route_id=str(df['route_id'].iloc[i]),
                vehicle_id=str(df.get('vehicle_id', pd.Series([None]*len(df))).iloc[i]) if 'vehicle_id' in df.columns else None,
                stop_sequence=int(df['stop_sequence'].iloc[i]) if 'stop_sequence' in df.columns else None,
                latitude=float(df['latitude'].iloc[i]) if 'latitude' in df.columns else None,
                longitude=float(df['longitude'].iloc[i]) if 'longitude' in df.columns else None,
                pred_arrival_delay=float(pred_mean[i]),
                pred_std=float(pred_std[i]),
                actual_arrival_delay=float(df['arrival_delay'].iloc[i]) if 'arrival_delay' in df.columns and pd.notna(df['arrival_delay'].iloc[i]) else None
            )
            for i in range(len(df))
        ]
        
        # Write to DB if requested
        if write_db:
            stmt = text("UPDATE bus_weather_merged SET arrival_delay_pred = :pred, arrival_delay_pred_std = :pred_std WHERE id = :id")
            payload = [{"id": r.id, "pred": r.pred_arrival_delay, "pred_std": r.pred_std} for r in rows]
            with engine.begin() as conn:
                conn.execute(stmt, payload)
        
        return PredictResponse(
            model_version=svc.model_version,
            scored_at_utc=datetime.now(timezone.utc).isoformat(),
            route_info=None,
            rows=rows
        )
    
    except Exception as e:
        logger.error(f"Error in predict_latest: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/predict/route/{route_short_name}", response_model=PredictResponse)
def predict_by_route(
    route_short_name: str,
    limit: int = Query(10, ge=1, le=500),
    write_db: bool = Query(False)
):
    """Predict delays for a specific route"""
    try:
        # Get route info
        route_query = text("""
            SELECT route_id, route_short_name, route_long_name
            FROM routes
            WHERE route_short_name = :route_short_name
            LIMIT 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(route_query, {"route_short_name": route_short_name})
            route_row = result.fetchone()
        
        if not route_row:
            return PredictResponse(
                model_version=svc.model_version,
                scored_at_utc=datetime.now(timezone.utc).isoformat(),
                route_info=None,
                rows=[]
            )
        
        route_info = RouteInfo(
            route_id=route_row[0],
            route_short_name=route_row[1],
            route_long_name=route_row[2]
        )
        
        # Fetch data for this route
        query = text(f"""
            SELECT *
            FROM bus_weather_merged
            WHERE route_id = :route_id
              AND stop_id IS NOT NULL
              AND stop_sequence IS NOT NULL
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND vehicle_timestamp IS NOT NULL
              AND weather_timestamp IS NOT NULL
            ORDER BY id DESC
            LIMIT {limit}
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"route_id": route_info.route_id})
        
        if df.empty:
            return PredictResponse(
                model_version=svc.model_version,
                scored_at_utc=datetime.now(timezone.utc).isoformat(),
                route_info=route_info,
                rows=[]
            )
        
        X = svc.prepare_X(df)
        pred_mean, pred_std = svc.predict_mean_std(X)
        
        rows = [
            PredictionRow(
                id=int(df['id'].iloc[i]),
                route_id=str(df['route_id'].iloc[i]),
                vehicle_id=str(df.get('vehicle_id', pd.Series([None]*len(df))).iloc[i]) if 'vehicle_id' in df.columns else None,
                stop_sequence=int(df['stop_sequence'].iloc[i]) if 'stop_sequence' in df.columns else None,
                latitude=float(df['latitude'].iloc[i]) if 'latitude' in df.columns else None,
                longitude=float(df['longitude'].iloc[i]) if 'longitude' in df.columns else None,
                pred_arrival_delay=float(pred_mean[i]),
                pred_std=float(pred_std[i]),
                actual_arrival_delay=float(df['arrival_delay'].iloc[i]) if 'arrival_delay' in df.columns and pd.notna(df['arrival_delay'].iloc[i]) else None
            )
            for i in range(len(df))
        ]
        
        # Write to DB if requested
        if write_db:
            stmt = text("UPDATE bus_weather_merged SET arrival_delay_pred = :pred, arrival_delay_pred_std = :pred_std WHERE id = :id")
            payload = [{"id": r.id, "pred": r.pred_arrival_delay, "pred_std": r.pred_std} for r in rows]
            with engine.begin() as conn:
                conn.execute(stmt, payload)
        
        return PredictResponse(
            model_version=svc.model_version,
            scored_at_utc=datetime.now(timezone.utc).isoformat(),
            route_info=route_info,
            rows=rows
        )
    
    except Exception as e:
        logger.error(f"Error in predict_by_route: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================
# ENDPOINTS - STOPS
# =====================================================
@app.get("/stops/nearby")
def get_nearby_stops(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius: int = Query(500, ge=100, le=5000, description="Radius in meters")
):
    """Get stops near a location"""
    try:
        query = text(f"""
            SELECT 
                stop_id,
                stop_name,
                latitude,
                longitude,
                (
                    6371000 * acos(
                        cos(radians(:lat)) * 
                        cos(radians(latitude)) * 
                        cos(radians(longitude) - radians(:lng)) + 
                        sin(radians(:lat)) * 
                        sin(radians(latitude))
                    )
                ) AS distance
            FROM stops
            WHERE (
                6371000 * acos(
                    cos(radians(:lat)) * 
                    cos(radians(latitude)) * 
                    cos(radians(longitude) - radians(:lng)) + 
                    sin(radians(:lat)) * 
                    sin(radians(latitude))
                )
            ) <= :radius
            ORDER BY distance
            LIMIT 10
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"lat": lat, "lng": lng, "radius": radius})
        
        stops = [
            {
                "stop_id": row["stop_id"],
                "stop_name": row["stop_name"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "distance": int(row["distance"])
            }
            for _, row in df.iterrows()
        ]
        
        logger.info(f"✅ Found {len(stops)} stops near ({lat}, {lng})")
        
        return {"success": True, "count": len(stops), "stops": stops}
    
    except Exception as e:
        logger.error(f"Error in get_nearby_stops: {e}")
        return {"success": False, "error": str(e), "stops": []}

@app.get("/stops/{stop_id}/arrivals")
def get_stop_arrivals(stop_id: str):
    """Get upcoming arrivals for a stop"""
    try:
        query = text("""
            SELECT 
                t.trip_id,
                t.route_id,
                t.stop_id,
                t.arrival_delay,
                t.departure_delay,
                t.trip_timestamp,
                r.route_short_name,
                r.route_long_name
            FROM trip_delays_raw t
            LEFT JOIN routes r ON t.route_id = r.route_id
            WHERE t.stop_id = :stop_id
            AND t.trip_timestamp >= NOW() - INTERVAL '10 minutes'
            ORDER BY t.trip_timestamp DESC
            LIMIT 20
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"stop_id": stop_id})
        
        arrivals = []
        for _, row in df.iterrows():
            arrivals.append({
                "route_id": row["route_id"],
                "route_short_name": row.get("route_short_name", row["route_id"]),
                "destination": row.get("route_long_name", "Unknown"),
                "scheduled_time": row["trip_timestamp"].strftime("%H:%M"),
                "predicted_time": row["trip_timestamp"].strftime("%H:%M"),
                "delay": int(row["arrival_delay"]) if row["arrival_delay"] else 0,
                "status": "LATE" if row["arrival_delay"] and row["arrival_delay"] > 60 else "DUE"
            })
        
        return {"success": True, "stop_id": stop_id, "count": len(arrivals), "arrivals": arrivals}
    
    except Exception as e:
        logger.error(f"Error in get_stop_arrivals: {e}")
        return {"success": False, "error": str(e), "arrivals": []}

# =====================================================
# ENDPOINTS - VISUALIZATIONS
# =====================================================
@app.get("/visualizations/list")
def list_visualizations():
    """List all available visualization files"""
    try:
        viz_files = []
        if os.path.exists(VISUALIZATIONS_DIR):
            for file in os.listdir(VISUALIZATIONS_DIR):
                if file.endswith(('.html', '.png', '.jpg')):
                    viz_files.append({
                        "filename": file,
                        "path": f"/visualizations/{file}",
                        "type": file.split('.')[-1]
                    })
        
        video_files = []
        if os.path.exists(VIDEOS_DIR):
            for file in os.listdir(VIDEOS_DIR):
                if file.endswith(('.gif', '.mp4')):
                    video_files.append({
                        "filename": file,
                        "path": f"/videos/{file}",
                        "type": file.split('.')[-1]
                    })
        
        return {
            "success": True,
            "visualizations": viz_files,
            "videos": video_files
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/visualizations/{filename}")
def get_visualization(filename: str):
    """Serve visualization file"""
    try:
        file_path = os.path.join(VISUALIZATIONS_DIR, filename)
        if os.path.exists(file_path):
            return FileResponse(file_path)
        else:
            raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/videos/{filename}")
def get_video(filename: str):
    """Serve video/gif file"""
    try:
        file_path = os.path.join(VIDEOS_DIR, filename)
        if os.path.exists(file_path):
            return FileResponse(file_path)
        else:
            raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================
# ENDPOINTS - MODEL PERFORMANCE
# =====================================================
@app.get("/model/performance")
def get_model_performance():
    """Get latest model performance metrics"""
    try:
        query = """
            SELECT 
                model_version,
                scored_at,
                metric_rows,
                mae,
                rmse,
                r2,
                y_mean,
                y_std,
                pred_mean,
                pred_std
            FROM model_scoring_runs
            ORDER BY scored_at DESC
            LIMIT 10
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {"success": True, "latest": None, "history": []}
        
        metrics = [
            {
                "model_version": row["model_version"],
                "scored_at": row["scored_at"].isoformat(),
                "samples": int(row["metric_rows"]),
                "mae": float(row["mae"]),
                "rmse": float(row["rmse"]),
                "r2": float(row["r2"]),
                "y_mean": float(row["y_mean"]),
                "y_std": float(row["y_std"]),
                "pred_mean": float(row["pred_mean"]),
                "pred_std": float(row["pred_std"])
            }
            for _, row in df.iterrows()
        ]
        
        return {
            "success": True,
            "latest": metrics[0] if metrics else None,
            "history": metrics
        }
    
    except Exception as e:
        logger.error(f"Error in get_model_performance: {e}")
        return {"success": False, "error": str(e)}

# =====================================================
# RUN SERVER
# =====================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8888,
        reload=True
    )
