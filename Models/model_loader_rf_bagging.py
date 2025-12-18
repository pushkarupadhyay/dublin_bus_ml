import os
import glob
import pandas as pd
import numpy as np
import joblib
import psycopg2
from datetime import datetime, timezone
import warnings

warnings.filterwarnings('ignore')


class RandomForestModelService:
    def __init__(self, db_config, artifacts_dir="../artifacts"):
        self.db_config = db_config
        self.artifacts_dir = artifacts_dir
        self.conn = self.get_db_connection()
        self.models = []
        self.meta = None
        self.load_latest_models()

    def get_db_connection(self):
        """Establish PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"]
            )
            print("✅ Database connection established")
            return conn
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def load_latest_models(self):
        """Load the latest Random Forest bagging models from artifacts"""
        artifacts_path = os.path.abspath(self.artifacts_dir)
        joblib_files = glob.glob(os.path.join(artifacts_path, "rf_bagged_*.joblib"))

        if not joblib_files:
            raise FileNotFoundError(f"No rf_bagged_*.joblib found in {artifacts_path}")

        latest_meta_file = max(joblib_files, key=os.path.getmtime)
        
        try:
            # Load metadata
            self.meta = joblib.load(latest_meta_file)
            print(f"✅ Loaded metadata: {os.path.basename(latest_meta_file)}")
            
            # Load individual models
            self.models = []
            for model_path in self.meta["model_paths"]:
                # Handle path: if relative, make it absolute
                if not os.path.exists(model_path):
                    model_path = os.path.join(artifacts_path, os.path.basename(model_path))
                
                model = joblib.load(model_path)
                self.models.append(model)
                print(f"  ✓ Loaded: {os.path.basename(model_path)}")
            
            print(f"✅ Total models loaded: {len(self.models)}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to load models: {e}")

    def fetch_input_data(self, limit=5):
        """Fetch data from database"""
        try:
            query = f"""
                SELECT * FROM bus_weather_merged 
                WHERE stop_id IS NOT NULL
                  AND stop_sequence IS NOT NULL
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND vehicle_timestamp IS NOT NULL
                  AND weather_timestamp IS NOT NULL
                ORDER BY id DESC 
                LIMIT {limit}
            """
            
            df = pd.read_sql(query, self.conn)
            print(f"✅ Fetched {len(df)} rows from database")
            
        except Exception as e:
            raise RuntimeError(f"Failed to fetch data: {e}")

        # Create copy
        df = df.copy()
        
        # Drop unnecessary columns
        exclude_cols = [
            "speed", "bearing", "id", "trip_id", "vehicle_timestamp",
            "weather_timestamp", "processing_timestamp", "arrival_delay",
            "arrival_delay_pred", "arrival_delay_pred_std"
        ]
        df = df.drop(columns=[col for col in exclude_cols if col in df.columns])
        
        print(f"ℹ️  Columns in dataframe: {list(df.columns)}")
        
        # Convert timestamps
        for col in ["vehicle_timestamp", "weather_timestamp"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        
        # Extract time features
        if "vehicle_timestamp" in df.columns:
            df["vehicle_hour"] = df["vehicle_timestamp"].dt.hour.fillna(0).astype(int)
            df["vehicle_dow"] = df["vehicle_timestamp"].dt.dayofweek.fillna(0).astype(int)
            df = df.drop("vehicle_timestamp", axis=1, errors="ignore")
        
        if "weather_timestamp" in df.columns:
            df["weather_hour"] = df["weather_timestamp"].dt.hour.fillna(0).astype(int)
            df["weather_dow"] = df["weather_timestamp"].dt.dayofweek.fillna(0).astype(int)
            df = df.drop("weather_timestamp", axis=1, errors="ignore")
        
        # Handle categorical columns
        cat_cols = self.meta["cat_cols"]
        for col in cat_cols:
            if col in df.columns:
                le = self.meta["label_encoders"][col]
                df[col] = df[col].fillna("missing").astype(str)
                df[col] = le.transform(df[col])
        
        # Handle numeric columns
        num_cols = self.meta["num_cols"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df[col] = df[col].fillna(df[col].median())
        
        # Fill any remaining NaN
        df = df.fillna(0)
        
        print(f"✅ Data prepared: shape = {df.shape}")
        print(f"✅ No NaN values: {not df.isna().any().any()}")
        
        return df

    def prepare_X(self, df):
        """Prepare feature matrix for prediction"""
        # Get feature columns from metadata
        feature_cols = self.meta["num_cols"] + self.meta["cat_cols"]
        
        # Ensure all features present
        for col in feature_cols:
            if col not in df.columns:
                df[col] = 0
        
        # Select only training features in correct order
        X = df[feature_cols].copy()
        
        return X

    def predict_with_uncertainty(self, X):
        """
        Make predictions across ensemble and calculate uncertainty
        Returns: (mean predictions, std predictions)
        """
        if len(X) == 0:
            raise ValueError("Input dataframe is empty")
        
        # Get predictions from all models
        all_predictions = []
        for i, model in enumerate(self.models):
            try:
                preds = model.predict(X)
                all_predictions.append(preds)
                print(f"  ✓ Model {i+1} predictions: shape={preds.shape}")
            except Exception as e:
                print(f"  ⚠️  Model {i+1} prediction failed: {e}")
        
        if not all_predictions:
            raise RuntimeError("No models produced valid predictions")
        
        # Stack predictions (n_models x n_samples)
        all_predictions = np.array(all_predictions)
        
        # Calculate mean and std
        mean_predictions = np.mean(all_predictions, axis=0)
        std_predictions = np.std(all_predictions, axis=0)
        
        print(f"✅ Ensemble predictions: mean shape={mean_predictions.shape}")
        print(f"✅ Uncertainty estimates: std shape={std_predictions.shape}")
        
        return mean_predictions, std_predictions

    def score_batch(self, limit=5, write_db=True):
        """Score a batch of data and optionally write to database"""
        print("\n" + "="*70)
        print("RANDOM FOREST BATCH SCORING")
        print("="*70)
        
        # Fetch data
        df_raw = self.fetch_input_data(limit=limit)
        
        if len(df_raw) == 0:
            print("❌ No data to score")
            return []
        
        # Prepare features
        X = self.prepare_X(df_raw)
        
        # Make predictions
        mean_preds, std_preds = self.predict_with_uncertainty(X)
        
        # Prepare results
        results = []
        for i in range(len(df_raw)):
            result = {
                "id": int(df_raw["id"].iloc[i]),
                "route_id": str(df_raw.get("route_id", [""] * len(df_raw)).iloc[i]),
                "vehicle_id": str(df_raw.get("vehicle_id", [""] * len(df_raw)).iloc[i]),
                "stop_sequence": int(df_raw.get("stop_sequence", [0] * len(df_raw)).iloc[i]),
                "latitude": float(df_raw.get("latitude", [0.0] * len(df_raw)).iloc[i]),
                "longitude": float(df_raw.get("longitude", [0.0] * len(df_raw)).iloc[i]),
                "pred_arrival_delay": float(mean_preds[i]),
                "pred_std": float(std_preds[i]),
                "actual_arrival_delay": float(df_raw.get("arrival_delay", [None] * len(df_raw)).iloc[i]) 
                    if pd.notna(df_raw.get("arrival_delay", [None] * len(df_raw)).iloc[i]) else None,
            }
            results.append(result)
        
        # Write to database
        if write_db:
            self._write_predictions_to_db(results)
        
        # Print summary
        print("\n" + "-"*70)
        print("Predictions (first 5):")
        for r in results[:5]:
            print(f"  ID {r['id']}: {r['pred_arrival_delay']:.2f}s ± {r['pred_std']:.2f}s")
        print("-"*70)
        
        return results

    def _write_predictions_to_db(self, results):
        """Write predictions to database"""
        try:
            cursor = self.conn.cursor()
            for r in results:
                cursor.execute("""
                    UPDATE bus_weather_merged
                    SET arrival_delay_pred = %s,
                        arrival_delay_pred_std = %s
                    WHERE id = %s
                """, (r["pred_arrival_delay"], r["pred_std"], r["id"]))
            
            self.conn.commit()
            print(f"✅ Wrote {len(results)} predictions to database")
            cursor.close()
        except Exception as e:
            print(f"⚠️  Failed to write to database: {e}")

    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✅ Database connection closed")


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "dublin_bus_db",
        "user": "dap",
        "password": "dap"
    }

    try:
        # Initialize service
        service = RandomForestModelService(db_config=db_config)
        
        # Score batch
        results = service.score_batch(limit=10, write_db=True)
        
        # Display results
        print("\n" + "="*70)
        print("Scoring Results Summary")
        print("="*70)
        print(f"Total predictions: {len(results)}")
        if results:
            mean_delay = np.mean([r["pred_arrival_delay"] for r in results])
            mean_std = np.mean([r["pred_std"] for r in results])
            print(f"Average predicted delay: {mean_delay:.2f}s")
            print(f"Average uncertainty: ±{mean_std:.2f}s")
        print("="*70)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        service.close_connection()