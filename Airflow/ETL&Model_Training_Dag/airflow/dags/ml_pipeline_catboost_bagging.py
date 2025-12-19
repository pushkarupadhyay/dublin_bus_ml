from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {"owner": "dublin_bus", "retries": 1, "retry_delay": timedelta(minutes=10)}


def train_and_register_catboost_bagged():
    import pandas as pd
    import psycopg2
    import numpy as np
    import joblib
    from pathlib import Path
    from datetime import datetime, timezone

    from sklearn.model_selection import GroupShuffleSplit
    from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
    from catboost import CatBoostRegressor

    ARTIFACTS_DIR = "/opt/airflow/artifacts"
    Path(ARTIFACTS_DIR).mkdir(parents=True, exist_ok=True)

    N_MODELS = 7
    BOOTSTRAP_FRAC = 1.0  # 1.0 =same size sample with replacement
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="dublin_bus_db",
        user="dap",
        password="dap",
        port=5432,
    )

    try:
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

        # target
        y = pd.to_numeric(df["arrival_delay"], errors="coerce")
        df = df.loc[y.notna()].copy()
        y = y.loc[df.index].astype(float)

        # drop leakage
        if "is_late" in df.columns:
            df = df.drop(columns=["is_late"])

        # time features in pandas
        df["vehicle_timestamp"] = pd.to_datetime(df["vehicle_timestamp"], errors="coerce", utc=True)
        df["weather_timestamp"] = pd.to_datetime(df["weather_timestamp"], errors="coerce", utc=True)

        df["vehicle_hour"] = df["vehicle_timestamp"].dt.hour.fillna(0).astype(int)
        df["vehicle_dow"] = df["vehicle_timestamp"].dt.dayofweek.fillna(0).astype(int)
        df["weather_hour"] = df["weather_timestamp"].dt.hour.fillna(0).astype(int)
        df["weather_dow"] = df["weather_timestamp"].dt.dayofweek.fillna(0).astype(int)

        df = df.drop(columns=["vehicle_timestamp", "weather_timestamp"], errors="ignore")

        GROUP_COL = "trip_id"
        CAT_COLS = ["route_id", "vehicle_id", "stop_id", "temperature_category", "weather_severity"]
        NUM_COLS = [
            "stop_sequence", "latitude", "longitude", "temperature", "humidity",
            "wind_speed", "precipitation",
            "vehicle_hour", "vehicle_dow", "weather_hour", "weather_dow",
        ]
        FEATURE_COLS = CAT_COLS + NUM_COLS

        missing = [c for c in FEATURE_COLS + [GROUP_COL] if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        X = df[FEATURE_COLS].copy()
        groups = df[GROUP_COL].astype(str)

        # stable casting/imputation
        for c in CAT_COLS:
            X[c] = X[c].astype("object").where(X[c].notna(), "missing").astype(str)

        for c in NUM_COLS:
            X[c] = pd.to_numeric(X[c], errors="coerce")

        # leakage free split by trip_id
        gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        train_idx, val_idx = next(gss.split(X, y, groups=groups))

        X_train_full, X_val = X.iloc[train_idx].copy(), X.iloc[val_idx].copy()
        y_train_full, y_val = y.iloc[train_idx].copy(), y.iloc[val_idx].copy()

        # numeric medians from training set
        train_medians = {c: float(np.nanmedian(X_train_full[c].to_numpy())) for c in NUM_COLS}
        for c in NUM_COLS:
            if np.isnan(train_medians[c]):
                train_medians[c] = 0.0
            X_train_full[c] = X_train_full[c].fillna(train_medians[c])
            X_val[c] = X_val[c].fillna(train_medians[c])

        #train bagged models on bootstrap samples [web:270]
        cbm_paths = []
        val_preds_all = []

        n_train = len(X_train_full)
        boot_n = int(n_train * BOOTSTRAP_FRAC)

        for m in range(N_MODELS):
            boot_idx = np.random.default_rng(42 + m).integers(0, n_train, size=boot_n)
            X_boot = X_train_full.iloc[boot_idx]
            y_boot = y_train_full.iloc[boot_idx]

            model = CatBoostRegressor(
                loss_function="RMSE",
                iterations=5000,
                learning_rate=0.02,
                depth=8,
                random_seed=42 + m,
                verbose=200,
            )

            model.fit(
                X_boot, y_boot,
                cat_features=CAT_COLS,
                eval_set=(X_val, y_val),
                use_best_model=True,
                early_stopping_rounds=400,  #CatBoost early stopping control [web:280]
            )

            model_version = f"catboost_bag_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_m{m}"
            cbm_path = f"{ARTIFACTS_DIR}/{model_version}.cbm"
            model.save_model(cbm_path)
            cbm_paths.append(cbm_path)

            val_preds_all.append(model.predict(X_val))

        #ensemble metrics on validation 
        val_pred_mean = np.mean(np.vstack(val_preds_all), axis=0)
        r2 = r2_score(y_val, val_pred_mean)
        mae = mean_absolute_error(y_val, val_pred_mean)
        rmse = float(np.sqrt(mean_squared_error(y_val, val_pred_mean)))

        #save meta bundle
        bundle_version = f"catboost_bagged_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        meta_path = f"{ARTIFACTS_DIR}/{bundle_version}.joblib"
        joblib.dump(
            {
                "model_type": "catboost_bagged",
                "model_paths": cbm_paths,
                "feature_cols": FEATURE_COLS,
                "cat_cols": CAT_COLS,
                "num_cols": NUM_COLS,
                "rename_map": rename_map,
                "train_medians": train_medians,
                "n_models": N_MODELS,
            },
            meta_path,
        )

        # register
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

        print(f"Bagged CatBoost registered: {bundle_version} | R2={r2:.3f} MAE={mae:.2f} RMSE={rmse:.2f}")
        print(f"Meta saved: {meta_path}")
        print(f"Models saved: {len(cbm_paths)} cbm files")

    finally:
        conn.close()


with DAG(
    dag_id="dublin_bus_ml_training_catboost_bagged_v1",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "catboost", "bagging"],
) as dag:

    PythonVirtualenvOperator(
        task_id="train_and_register_catboost_bagged",
        python_callable=train_and_register_catboost_bagged,
        requirements=[
            "numpy==1.26.4",
            "pandas==2.3.3",
            "scikit-learn==1.8.0",
            "joblib==1.5.3",
            "psycopg2-binary==2.9.11",
            "catboost==1.2.5",
        ],
        system_site_packages=False,
    )
