"""
SMARD Project Extension — Day 1-2: Renewable Generation Forecasting
=====================================================================
Pulls daily renewable generation PER TSO REGION from
SMARD_PROD.GOLD.AGG_REGIONAL_COMPARISON (the pre-built dbt aggregation
that already carries true region grain — FCT_ENERGY_READINGS only has
the national 'DE' rollup). Drops any day whose RENEWABLE_MWH falls below
30% of a reference-period median (catches genuine ingestion gaps, such
as the 2026-06-12/06-13 Silver-layer gap, wherever they sit in the
timeline). Trains one Facebook Prophet model per region, validates on a
7-day holdout (MAPE/MAE) of the cleaned data, then refits on full
cleaned history and writes a 14-day forward forecast to
GOLD.RENEWABLE_FORECAST.

Run on the smard-airflow-vm (or anywhere with Snowflake network access).

Usage:
    pip install prophet "snowflake-connector-python[pandas]" pandas numpy
    export SNOWFLAKE_ACCOUNT=qg17675.europe-west3.gcp
    export SNOWFLAKE_USER=OJASINDULKAR
    export SNOWFLAKE_PASSWORD=...          # don't hardcode this in the script
    export SNOWFLAKE_ROLE=TRANSFORMER       # optional, defaults shown
    export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
    export SNOWFLAKE_DATABASE=SMARD_PROD

    python3 forecast_renewable.py
"""

import os

import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from prophet import Prophet

# ---------------------------------------------------------------------------
# Config — credentials come from environment variables, never hardcoded
# ---------------------------------------------------------------------------
SF_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SF_ROLE = os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "SMARD_PROD")
SF_SCHEMA = "GOLD"

HOLDOUT_DAYS = 7
FORECAST_HORIZON_DAYS = 14


def get_connection():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )


def pull_renewable_data(conn):
    """
    Pull daily renewable generation PER TSO REGION from the pre-built
    Gold aggregation table (already has true per-region grain, unlike
    FCT_ENERGY_READINGS which only carries the national 'DE' rollup).
    ACTIVE_SOURCES is pulled alongside so we can detect incomplete-
    ingestion days (e.g. the SMARD_DEV-vs-Silver cutover around 2026-06-12)
    by source-count, not by guessing at date ranges.
    """
    query = """
        SELECT
            "region" AS region,
            "region_full" AS region_full,
            AGG_DATE AS ds,
            RENEWABLE_MWH AS y,
            ACTIVE_SOURCES AS active_sources
        FROM AGG_REGIONAL_COMPARISON
        ORDER BY "region", ds
    """
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def filter_incomplete_days(df_region, frac_threshold=0.3, recency_buffer_days=21):
    """
    Drop any day where RENEWABLE_MWH falls below frac_threshold of the
    'normal' (reference-period) daily median — catches genuine ingestion
    gaps or missing generation data, regardless of where they sit in the
    timeline. ACTIVE_SOURCES turned out NOT to be a reliable completeness
    signal here — a single source category can legitimately report zero
    for a day without the day actually being incomplete (e.g. 2026-06-15
    had ACTIVE_SOURCES=10 but a perfectly normal MWh total).

    The reference median is computed from history EXCLUDING the most
    recent `recency_buffer_days`, so a real recent incident can't drag
    the threshold down and hide itself.

    Returns (filtered_df, dropped_df, reference_median).
    """
    df_region = df_region.sort_values("ds").reset_index(drop=True)
    if len(df_region) < 30:
        return df_region, df_region.iloc[0:0], None

    reference = df_region.iloc[: -recency_buffer_days] if len(df_region) > recency_buffer_days else df_region
    reference_median = reference["y"].median()
    if not reference_median or reference_median <= 0:
        return df_region, df_region.iloc[0:0], reference_median

    threshold = frac_threshold * reference_median
    keep_mask = df_region["y"] >= threshold

    filtered = df_region[keep_mask].reset_index(drop=True)
    dropped = df_region[~keep_mask]
    return filtered, dropped, reference_median


def train_and_validate(df_region):
    """
    Train Prophet on all-but-holdout, score on the last HOLDOUT_DAYS rows,
    then refit on the FULL (trimmed) history and forecast forward.
    Returns (forecast_df, mae, mape) or (None, None, None) if not enough data.
    """
    df_region = df_region.sort_values("ds").reset_index(drop=True)

    if len(df_region) < 30 + HOLDOUT_DAYS:
        return None, None, None

    # Holdout = last HOLDOUT_DAYS *rows* (robust to occasional missing dates)
    train = df_region.iloc[:-HOLDOUT_DAYS]
    test = df_region.iloc[-HOLDOUT_DAYS:]

    val_model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=len(train) > 365,
    )
    val_model.fit(train[["ds", "y"]])

    forecast_test = val_model.predict(test[["ds"]])
    merged = test.merge(forecast_test[["ds", "yhat"]], on="ds")

    mae = float(np.mean(np.abs(merged["y"] - merged["yhat"])))
    mape = float(
        np.mean(np.abs((merged["y"] - merged["yhat"]) / merged["y"].replace(0, np.nan))) * 100
    )

    # Refit on the full trimmed history for the production forecast
    full_model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=len(df_region) > 365,
    )
    full_model.fit(df_region[["ds", "y"]])

    future = full_model.make_future_dataframe(
        periods=FORECAST_HORIZON_DAYS, freq="D", include_history=False
    )
    forecast = full_model.predict(future)

    return forecast, mae, mape


def main():
    conn = get_connection()

    print("Pulling daily renewable generation per TSO region from GOLD.AGG_REGIONAL_COMPARISON ...")
    df = pull_renewable_data(conn)
    regions = sorted(df["region"].unique())
    print(f"Regions found: {regions}")
    print(f"Date range in raw data: {df['ds'].min().date()} to {df['ds'].max().date()}")

    all_forecasts = []
    results = []

    for region in regions:
        df_region = df[df["region"] == region][["ds", "y", "active_sources"]].copy()
        df_region, dropped, reference_median = filter_incomplete_days(df_region)

        print(f"\nRegion: {region} — reference median = {reference_median:,.0f} MWh/day "
              f"(threshold = 30% of that), {len(df_region)} usable rows after filtering "
              f"({len(dropped)} dropped)")
        if len(dropped):
            print("  Dropped (incomplete-ingestion) dates:")
            print(dropped[["ds", "y", "active_sources"]].to_string(index=False))

        forecast, mae, mape = train_and_validate(df_region[["ds", "y"]])
        if forecast is None:
            print(f"  Skipping {region} — not enough clean history for a {HOLDOUT_DAYS}-day holdout yet.")
            continue

        print(f"  Holdout MAE:  {mae:,.2f} MWh/day")
        print(f"  Holdout MAPE: {mape:.2f}%")
        results.append({"region": region, "mae_mwh": round(mae, 2), "mape_pct": round(mape, 2),
                         "incomplete_days_excluded": len(dropped)})

        out = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        out["REGION"] = region
        out = out.rename(
            columns={
                "ds": "TIMESTAMP",
                "yhat": "PREDICTED_VALUE",
                "yhat_lower": "LOWER_BOUND",
                "yhat_upper": "UPPER_BOUND",
            }
        )
        all_forecasts.append(out[["REGION", "TIMESTAMP", "PREDICTED_VALUE", "LOWER_BOUND", "UPPER_BOUND"]])

    if not all_forecasts:
        print("No regions had enough history to forecast. Exiting.")
        return

    forecast_df = pd.concat(all_forecasts, ignore_index=True)

    print(f"\nWriting {len(forecast_df)} forecast rows to GOLD.RENEWABLE_FORECAST ...")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS GOLD.RENEWABLE_FORECAST (
            REGION VARCHAR,
            TIMESTAMP TIMESTAMP_NTZ,
            PREDICTED_VALUE FLOAT,
            LOWER_BOUND FLOAT,
            UPPER_BOUND FLOAT,
            MODEL_TRAINED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )
    # Idempotent re-run while iterating on Day 1-2 — replace the whole table each run
    cursor.execute("TRUNCATE TABLE GOLD.RENEWABLE_FORECAST")

    write_pandas(conn, forecast_df, "RENEWABLE_FORECAST", schema="GOLD", database=SF_DATABASE)

    results_df = pd.DataFrame(results)
    results_df.to_csv("forecast_validation_results.csv", index=False)

    print("\n=== Validation Summary (MAPE / MAE per region) ===")
    print(results_df.to_string(index=False))
    print("\nSaved to forecast_validation_results.csv — drop this straight into the README results table.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
