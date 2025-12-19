import os
import glob
import numpy as np
import pandas as pd
import joblib

from sqlalchemy import create_engine, text
from catboost import CatBoostRegressor

ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Airflow", "ETL&Model_Training_Dag", "airflow", "artifacts"))
DB_URI = "postgresql+psycopg2://dap:dap@localhost:5432/dublin_bus_db"
PRED_LIMIT = 5
METRIC_LIMIT = 2000


def load_latest_meta(artifacts_dir: str) -> tuple[dict, str]:
    files = glob.glob(os.path.join(artifacts_dir, "catboost_bagged_*.joblib"))
    if not files:
        raise FileNotFoundError(f"No catboost_bagged_*.joblib found in: {artifacts_dir}")
    latest = max(files, key=os.path.getmtime)
    meta = joblib.load(latest)
    model_version = os.path.splitext(os.path.basename(latest))[0]
    print(f"Loaded from meta: {os.path.basename(latest)}")
    return meta, model_version


def canonicalize_columns(df: pd.DataFrame, rename_map: dict) -> pd.DataFrame:
    df = df.copy()
    for bad, good in rename_map.items():
        if bad in df.columns and good not in df.columns:
            df = df.rename(columns={bad: good})
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["vehicle_timestamp"] = pd.to_datetime(df["vehicle_timestamp"], errors="coerce", utc=True)
    df["weather_timestamp"] = pd.to_datetime(df["weather_timestamp"], errors="coerce", utc=True)

    df["vehicle_hour"] = df["vehicle_timestamp"].dt.hour.fillna(0).astype(int)
    df["vehicle_dow"] = df["vehicle_timestamp"].dt.dayofweek.fillna(0).astype(int)
    df["weather_hour"] = df["weather_timestamp"].dt.hour.fillna(0).astype(int)
    df["weather_dow"] = df["weather_timestamp"].dt.dayofweek.fillna(0).astype(int)

    return df.drop(columns=["vehicle_timestamp", "weather_timestamp"], errors="ignore")


def fetch_latest_rows(engine, limit: int) -> pd.DataFrame:
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
        LIMIT {limit};
    """
    return pd.read_sql(query, engine)


def prepare_X(df_raw: pd.DataFrame, meta: dict) -> pd.DataFrame:
    df = canonicalize_columns(df_raw, meta["rename_map"])
    df = add_time_features(df)

    feature_cols = meta["feature_cols"]
    cat_cols = meta["cat_cols"]
    num_cols = meta["num_cols"]
    train_medians = meta.get("train_medians", {})

    for c in feature_cols:
        if c not in df.columns:
            df[c] = np.nan

    X = df[feature_cols].copy()

    for c in cat_cols:
        X[c] = X[c].astype("object").where(X[c].notna(), "missing").astype(str)

    for c in num_cols:
        X[c] = pd.to_numeric(X[c], errors="coerce")
        fill = train_medians.get(c, float(np.nanmedian(X[c].to_numpy())))
        if np.isnan(fill):
            fill = 0.0
        X[c] = X[c].fillna(fill)

    return X


def load_models(meta: dict, artifacts_dir: str):
    paths = meta["model_paths"]
    models = []
    for p in paths:
        #linux path to windows folder by basename
        if not os.path.exists(p):
            p = os.path.join(artifacts_dir, os.path.basename(p))
        m = CatBoostRegressor()
        m.load_model(p)
        models.append(m)
    print(f"Loaded {len(models)} CatBoost models")
    return models


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = float(1.0 - (ss_res / ss_tot)) if ss_tot > 0 else None
    return {"mae": mae, "rmse": rmse, "r2": r2}


def write_predictions(engine, ids, preds_mean, preds_std):
    stmt = text("""
        UPDATE bus_weather_merged
        SET arrival_delay_pred = :pred,
            arrival_delay_pred_std = :pred_std
        WHERE id = :id
    """)
    payload = [
        {"id": int(i), "pred": float(pm), "pred_std": float(ps)}
        for i, pm, ps in zip(ids, preds_mean, preds_std)
    ]
    with engine.begin() as conn:
        conn.execute(stmt, payload)


def write_scoring_run(engine, model_version: str, pred_rows: int, metric_rows: int, metrics: dict, stats: dict):
    stmt = text("""
        INSERT INTO model_scoring_runs
        (model_version, pred_rows, metric_rows, mae, rmse, r2, y_mean, y_std, pred_mean, pred_std)
        VALUES
        (:model_version, :pred_rows, :metric_rows, :mae, :rmse, :r2, :y_mean, :y_std, :pred_mean, :pred_std)
    """)
    with engine.begin() as conn:
        conn.execute(stmt, {
            "model_version": model_version,
            "pred_rows": int(pred_rows),
            "metric_rows": int(metric_rows),
            "mae": float(metrics["mae"]),
            "rmse": float(metrics["rmse"]),
            "r2": metrics["r2"],
            "y_mean": float(stats["y_mean"]),
            "y_std": float(stats["y_std"]),
            "pred_mean": float(stats["pred_mean"]),
            "pred_std": float(stats["pred_std"]),
        })


def main():
    meta, model_version = load_latest_meta(ARTIFACTS_DIR)
    engine = create_engine(DB_URI)

    models = load_models(meta, ARTIFACTS_DIR)

    #Predict latest rows
    df_pred = fetch_latest_rows(engine, PRED_LIMIT)
    if df_pred.empty:
        print("No rows found for prediction")
        return
    X_pred = prepare_X(df_pred, meta)

    all_preds = np.vstack([m.predict(X_pred) for m in models])
    preds_mean = np.mean(all_preds, axis=0)
    preds_std = np.std(all_preds, axis=0)

    write_predictions(engine, df_pred["id"].tolist(), preds_mean, preds_std)
    print("Wrote arrival_delay_pred + arrival_delay_pred_std.")

    print("\nPredictions (arrival_delay):")
    for row_id, pm, ps in zip(df_pred["id"].tolist(), preds_mean, preds_std):
        print(f"id={row_id}  pred={float(pm):.3f}  std={float(ps):.3f}")

    #Metrics window
    df_m = fetch_latest_rows(engine, METRIC_LIMIT)
    X_m = prepare_X(df_m, meta)
    all_m = np.vstack([m.predict(X_m) for m in models])
    pred_m = np.mean(all_m, axis=0)

    y = pd.to_numeric(df_m["arrival_delay"], errors="coerce").to_numpy(dtype=float)
    mask = ~np.isnan(y)
    y_eval = y[mask]
    p_eval = pred_m[mask]

    stats = {
        "y_mean": float(np.mean(y_eval)),
        "y_std": float(np.std(y_eval)),
        "pred_mean": float(np.mean(p_eval)),
        "pred_std": float(np.std(p_eval)),
    }
    metrics = compute_metrics(y_eval, p_eval)

    print("\nMetric window stats:", stats)
    print("Metrics:", metrics)

    baseline = np.full_like(y_eval, float(np.mean(y_eval)))
    print("Baseline metrics:", compute_metrics(y_eval, baseline))

    write_scoring_run(engine, model_version, pred_rows=len(df_pred), metric_rows=int(mask.sum()), metrics=metrics, stats=stats)
    print("Saved scoring run to model_scoring_runs")


if __name__ == "__main__":
    main()
