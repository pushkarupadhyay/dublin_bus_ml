from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "dublin_bus",
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}


def train_and_register_random_forest_bagged():
    """
    Train 7 Random Forest models using bootstrap bagging
    Save models and metadata to artifacts directory
    Register in PostgreSQL model_registry
    """
    import pandas as pd
    import psycopg2
    import numpy as np
    import joblib
    from pathlib import Path
    from datetime import datetime, timezone
    from sklearn.model_selection import GroupShuffleSplit
    from sklearn.preprocessing import LabelEncoder
    from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
    from sklearn.ensemble import RandomForestRegressor

    ARTIFACTS_DIR = "/opt/airflow/artifacts"
    Path(ARTIFACTS_DIR).mkdir(parents=True, exist_ok=True)

    N_MODELS = 7
    BOOTSTRAP_FRAC = 1.0  # 1.0 = same size sample with replacement

    # Database connection
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="dublin_bus_db",
        user="dap",
        password="dap",
        port=5432,
    )

    try:
        # ============================================================
        # 1. LOAD DATA
        # ============================================================
        query = """
            SELECT *
            FROM bus_weather_merged
            WHERE arrival_delay IS NOT NULL
              AND trip_id IS NOT NULL
              AND route_id IS NOT NULL
              AND vehicle_id IS NOT NULL
              AND stop_id IS NOT NULL
              AND stop_sequence IS NOT NULL
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
              AND vehicle_timestamp IS NOT NULL
              AND weather_timestamp IS NOT NULL
        """
        df = pd.read_sql(query, conn)
        if df.empty:
            raise ValueError("No training data found.")

        print(f"✅ Loaded {len(df)} rows from database")

        # ============================================================
        # 2. HANDLE COLUMN NAME TYPOS
        # ============================================================
        rename_map = {
            "ttemperature": "temperature",
            "temperrature": "temperature",
            "temperaturee": "temperature",
            "wwind_speed": "wind_speed",
            "wind__speed": "wind_speed",
            "stop_sequuence": "stop_sequence",
            "weather__severity": "weather_severity",
            "is_latee": "is_late",
        }
        for bad, good in rename_map.items():
            if bad in df.columns and good not in df.columns:
                df = df.rename(columns={bad: good})

        # ============================================================
        # 3. EXTRACT TARGET
        # ============================================================
        y = pd.to_numeric(df["arrival_delay"], errors="coerce")
        df = df.loc[y.notna()].copy()
        y = y.loc[df.index].astype(float)

        print(f"✅ Target (arrival_delay): {len(y)} non-null values")

        # ============================================================
        # 4. DROP LEAKAGE COLUMN
        # ============================================================
        if "is_late" in df.columns:
            df = df.drop(columns=["is_late"])

        # ============================================================
        # 5. CREATE TIME FEATURES
        # ============================================================
        df["vehicle_timestamp"] = pd.to_datetime(df["vehicle_timestamp"], errors="coerce", utc=True)
        df["weather_timestamp"] = pd.to_datetime(df["weather_timestamp"], errors="coerce", utc=True)

        df["vehicle_hour"] = df["vehicle_timestamp"].dt.hour.fillna(0).astype(int)
        df["vehicle_dow"] = df["vehicle_timestamp"].dt.dayofweek.fillna(0).astype(int)
        df["weather_hour"] = df["weather_timestamp"].dt.hour.fillna(0).astype(int)
        df["weather_dow"] = df["weather_timestamp"].dt.dayofweek.fillna(0).astype(int)

        df = df.drop(columns=["vehicle_timestamp", "weather_timestamp"], errors="ignore")

        print("✅ Time features created")

        # ============================================================
        # 6. DEFINE FEATURE COLUMNS
        # ============================================================
        GROUP_COL = "trip_id"
        CAT_COLS = [
            "route_id", "vehicle_id", "stop_id",
            "temperature_category", "weather_severity"
        ]
        NUM_COLS = [
            "stop_sequence", "latitude", "longitude",
            "temperature", "humidity", "wind_speed", "precipitation",
            "vehicle_hour", "vehicle_dow", "weather_hour", "weather_dow",
        ]
        FEATURE_COLS = CAT_COLS + NUM_COLS

        missing = [c for c in FEATURE_COLS + [GROUP_COL] if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        X = df[FEATURE_COLS].copy()
        groups = df[GROUP_COL].astype(str)

        print(f"✅ Features: {len(FEATURE_COLS)} total ({len(CAT_COLS)} cat, {len(NUM_COLS)} num)")

        # ============================================================
        # 7. PREPROCESS FEATURES
        # ============================================================
        # Categorical: fill missing with "missing" string
        for c in CAT_COLS:
            X[c] = X[c].astype("object").where(X[c].notna(), "missing").astype(str)

        # Numeric: convert to float
        for c in NUM_COLS:
            X[c] = pd.to_numeric(X[c], errors="coerce")

        # ============================================================
        # 8. TRAIN-TEST SPLIT (by trip_id for leak prevention)
        # ============================================================
        gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        train_idx, val_idx = next(gss.split(X, y, groups=groups))

        X_train_full, X_val = X.iloc[train_idx].copy(), X.iloc[val_idx].copy()
        y_train_full, y_val = y.iloc[train_idx].copy(), y.iloc[val_idx].copy()

        print(f"✅ Train: {len(X_train_full)} | Val: {len(X_val)}")

        # ============================================================
        # 9. COMPUTE TRAIN MEDIANS (for numeric imputation)
        # ============================================================
        train_medians = {
            c: float(np.nanmedian(X_train_full[c].to_numpy()))
            for c in NUM_COLS
        }
        for c in NUM_COLS:
            if np.isnan(train_medians[c]):
                train_medians[c] = 0.0
            X_train_full[c] = X_train_full[c].fillna(train_medians[c])
            X_val[c] = X_val[c].fillna(train_medians[c])

        # ============================================================
        # 10. ENCODE CATEGORICAL COLUMNS (Handle Unseen Values)
        # ============================================================
        label_encoders = {}
        for c in CAT_COLS:
            le = LabelEncoder()
            # Fit on training data only
            X_train_full[c] = le.fit_transform(X_train_full[c])
            
            # For validation: handle unseen categories (map to -1)
            X_val_encoded = []
            for val in X_val[c]:
                try:
                    X_val_encoded.append(le.transform([val])[0])
                except ValueError:
                    # Unseen category in validation set → map to -1
                    X_val_encoded.append(-1)
            X_val[c] = X_val_encoded
            
            label_encoders[c] = le

        print("✅ Categorical features encoded (unseen values → -1)")

        # ============================================================
        # 11. TRAIN BAGGED RANDOM FOREST MODELS
        # ============================================================
        pkl_paths = []
        val_preds_all = []

        n_train = len(X_train_full)
        boot_n = int(n_train * BOOTSTRAP_FRAC)

        print(f"\n📊 Training {N_MODELS} Random Forest models...")

        for m in range(N_MODELS):
            print(f"\n  Model {m+1}/{N_MODELS}:")

            # Bootstrap sample with replacement
            boot_idx = np.random.default_rng(42 + m).integers(0, n_train, size=boot_n)
            X_boot = X_train_full.iloc[boot_idx]
            y_boot = y_train_full.iloc[boot_idx]

            # Train Random Forest
            model = RandomForestRegressor(
                n_estimators=200,
                max_depth=20,
                min_samples_split=5,
                min_samples_leaf=2,
                max_features='sqrt',
                random_state=42 + m,
                n_jobs=-1,  # Parallel
                verbose=0,
            )

            model.fit(X_boot, y_boot)

            # Evaluate on validation set
            train_pred = model.predict(X_boot)
            val_pred = model.predict(X_val)

            train_mae = mean_absolute_error(y_boot, train_pred)
            train_rmse = float(np.sqrt(mean_squared_error(y_boot, train_pred)))
            val_mae = mean_absolute_error(y_val, val_pred)
            val_rmse = float(np.sqrt(mean_squared_error(y_val, val_pred)))

            print(f"    Train MAE: {train_mae:.2f}s, RMSE: {train_rmse:.2f}s")
            print(f"    Val MAE:   {val_mae:.2f}s, RMSE: {val_rmse:.2f}s")

            # Save model
            model_version = f"rf_bag_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_m{m}"
            pkl_path = f"{ARTIFACTS_DIR}/{model_version}.pkl"
            joblib.dump(model, pkl_path)
            pkl_paths.append(pkl_path)

            # Store validation predictions
            val_preds_all.append(val_pred)

        # ============================================================
        # 12. COMPUTE ENSEMBLE METRICS
        # ============================================================
        print(f"\n🎯 Ensemble Metrics:")

        val_pred_mean = np.mean(np.vstack(val_preds_all), axis=0)
        val_pred_std = np.std(np.vstack(val_preds_all), axis=0)

        r2 = r2_score(y_val, val_pred_mean)
        mae = mean_absolute_error(y_val, val_pred_mean)
        rmse = float(np.sqrt(mean_squared_error(y_val, val_pred_mean)))

        print(f"  Val R²:   {r2:.4f}")
        print(f"  Val MAE:  {mae:.2f}s")
        print(f"  Val RMSE: {rmse:.2f}s")

        # Baseline comparison (always predict mean)
        baseline_mae = mean_absolute_error(y_val, [y_val.mean()] * len(y_val))
        improvement = ((baseline_mae - mae) / baseline_mae) * 100
        print(f"  Improvement vs baseline: {improvement:.1f}%")

        # ============================================================
        # 13. SAVE METADATA
        # ============================================================
        bundle_version = f"rf_bagged_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        meta_path = f"{ARTIFACTS_DIR}/{bundle_version}.joblib"

        joblib.dump(
            {
                "model_type": "random_forest_bagged",
                "model_paths": pkl_paths,
                "feature_cols": FEATURE_COLS,
                "cat_cols": CAT_COLS,
                "num_cols": NUM_COLS,
                "label_encoders": label_encoders,
                "rename_map": rename_map,
                "train_medians": train_medians,
                "n_models": N_MODELS,
            },
            meta_path,
        )

        print(f"\n✅ Meta saved: {meta_path}")
        print(f"✅ Models saved: {len(pkl_paths)} pkl files")

        # ============================================================
        # 14. REGISTER IN MODEL REGISTRY
        # ============================================================
        cur = conn.cursor()

        cur.execute("""
            UPDATE model_registry
            SET is_active = FALSE
            WHERE model_name = 'delay_prediction'
        """)

        cur.execute("""
            INSERT INTO model_registry
            (model_name, model_version, r2, mae, rmse, artifact_path, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE)
        """, (
            "delay_prediction",
            bundle_version,
            float(r2),
            float(mae),
            float(rmse),
            meta_path,
        ))

        conn.commit()
        cur.close()

        print(f"\n✅ Registered: {bundle_version}")
        print(f"   R²={r2:.4f}, MAE={mae:.2f}s, RMSE={rmse:.2f}s")

    finally:
        conn.close()


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="dublin_bus_ml_training_rf_bagged_v1",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "random_forest", "bagging"],
) as dag:

    PythonVirtualenvOperator(
        task_id="train_and_register_rf_bagged",
        python_callable=train_and_register_random_forest_bagged,
        requirements=[
            "pandas==2.3.3",
            "numpy==2.3.5",
            "scikit-learn==1.8.0",
            "joblib==1.5.3",
            "psycopg2-binary==2.9.11",
        ],
        system_site_packages=False,
    )