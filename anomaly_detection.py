"""
SMARD Project Extension — Day 3: Anomaly Detection (Prophet confidence intervals)
=====================================================================
Replaces the z-score approach with Prophet-based anomaly detection.
Prophet already understands weekly + yearly seasonality, so day-of-week
effects (e.g. Sunday morning demand patterns) don't generate false positives
the way a naive rolling z-score does.

Approach:
  1. Pull daily renewable + demand series from Gold
  2. Apply magnitude-based completeness filter (same as forecast_renewable.py)
  3. Train one Prophet model per metric (renewable_total, demand)
  4. Generate in-sample predictions to get yhat/yhat_lower/yhat_upper
  5. Flag any day where actual falls OUTSIDE the confidence interval
  6. Write to GOLD.ANOMALY_FLAGS

Why this beats z-score for energy data:
  - Prophet absorbs weekly + yearly seasonality natively
  - A normal Sunday reading won't be flagged because Prophet knows Sundays
  - Genuine anomalies (sudden renewable drop, real demand spike) still get caught
  - Reuses Day 1 forecasting work — same data, same model, extended purpose
"""

import os

import numpy as np
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from prophet import Prophet

SF_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER      = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "SMARD_PROD")
SF_SCHEMA    = "GOLD"

FRAC_THRESHOLD         = 0.30
RECENCY_BUFFER_DAYS    = 21
OUTPUT_WINDOW_DAYS     = 90
PROPHET_INTERVAL_WIDTH = 0.95


def get_connection():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        role=SF_ROLE, warehouse=SF_WAREHOUSE,
        database=SF_DATABASE, schema=SF_SCHEMA,
    )


def pull_data(conn):
    renewable_df = pd.read_sql("""
        SELECT "region" AS region, AGG_DATE AS ds,
               RENEWABLE_MWH AS y, ACTIVE_SOURCES AS active_sources
        FROM AGG_REGIONAL_COMPARISON ORDER BY "region", ds
    """, conn)
    demand_df = pd.read_sql("""
        SELECT "region" AS region, "reading_date" AS ds, SUM("value_mw") AS y
        FROM FCT_ENERGY_READINGS
        WHERE "energy_source" = 'consumption'
        GROUP BY "region", "reading_date" ORDER BY "region", ds
    """, conn)
    for df in (renewable_df, demand_df):
        df.columns = [c.lower() for c in df.columns]
        df["ds"] = pd.to_datetime(df["ds"])
    renewable_df["metric"] = "renewable_total"
    demand_df["metric"]    = "demand"
    demand_df["active_sources"] = None
    return pd.concat([renewable_df, demand_df], ignore_index=True)


def filter_incomplete_days(df_series):
    df_series = df_series.sort_values("ds").reset_index(drop=True)
    if len(df_series) < 30:
        return df_series, pd.DataFrame(), None
    reference = (df_series.iloc[:-RECENCY_BUFFER_DAYS]
                 if len(df_series) > RECENCY_BUFFER_DAYS else df_series)
    ref_median = reference["y"].median()
    if not ref_median or ref_median <= 0:
        return df_series, pd.DataFrame(), ref_median
    keep = df_series["y"] >= FRAC_THRESHOLD * ref_median
    return df_series[keep].reset_index(drop=True), df_series[~keep], ref_median


def fit_and_predict(df_clean):
    train = df_clean[["ds", "y"]].copy()
    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=len(train) > 365,
        interval_width=PROPHET_INTERVAL_WIDTH,
    )
    model.fit(train)
    return model.predict(train[["ds"]])[["ds", "yhat", "yhat_lower", "yhat_upper"]]


def classify_anomaly(row):
    if pd.isna(row["yhat_lower"]):
        return False, "insufficient_history"
    if row["y"] < row["yhat_lower"]:
        return True, ("renewable_drop" if row["metric"] == "renewable_total" else "demand_drop")
    if row["y"] > row["yhat_upper"]:
        return True, ("renewable_spike" if row["metric"] == "renewable_total" else "demand_spike")
    return False, "normal"


def main():
    conn = get_connection()
    print("Pulling daily renewable + demand series from Gold layer ...")
    df = pull_data(conn)
    print(f"Regions: {sorted(df['region'].unique())}")
    print(f"Date range: {df['ds'].min().date()} to {df['ds'].max().date()}")

    all_scored = []
    for (region, metric), group in df.groupby(["region", "metric"]):
        df_clean, dropped, ref_median = filter_incomplete_days(group.copy())
        print(f"\n{region}/{metric}: {len(df_clean)} clean days "
              f"({len(dropped)} dropped" +
              (f", ref median={ref_median:,.0f})" if ref_median else ")"))
        if len(df_clean) < 60:
            print("  Skipping — not enough history.")
            continue
        print("  Training Prophet ...")
        forecast = fit_and_predict(df_clean)
        scored = df_clean[["ds", "y", "metric"]].merge(forecast, on="ds", how="left")
        scored["region"] = region
        scored["anomaly_score"] = scored.apply(
            lambda r: round(
                (r["y"] - r["yhat_upper"]) / r["yhat"] if r["y"] > r["yhat_upper"]
                else (r["y"] - r["yhat_lower"]) / r["yhat"] if r["y"] < r["yhat_lower"]
                else 0.0, 4), axis=1)
        res = scored.apply(classify_anomaly, axis=1, result_type="expand")
        scored["is_anomaly"]   = res[0]
        scored["anomaly_type"] = res[1]
        all_scored.append(scored)

    if not all_scored:
        print("No data to score.")
        return

    full   = pd.concat(all_scored, ignore_index=True)
    cutoff = full["ds"].max() - pd.Timedelta(days=OUTPUT_WINDOW_DAYS)
    output = full[full["ds"] >= cutoff].copy()
    n_flagged = output["is_anomaly"].sum()

    print(f"\n=== Results (last {OUTPUT_WINDOW_DAYS} days) ===")
    print(f"Days scored:       {len(output)}")
    print(f"Anomalies flagged: {n_flagged}")
    if n_flagged:
        print("\nCounts by type:")
        print(output[output["is_anomaly"]]["anomaly_type"].value_counts().to_string())
        print("\n=== Top 10 by severity ===")
        top10 = (output[output["is_anomaly"]]
                 .assign(abs_score=lambda x: x["anomaly_score"].abs())
                 .sort_values("abs_score", ascending=False).head(10))
        print(top10[["region","metric","ds","y","yhat","yhat_lower",
                      "yhat_upper","anomaly_score","anomaly_type"]].to_string(index=False))

    # Convert date to string to avoid PyArrow/write_pandas timestamp serialization bug
    output["ds"] = output["ds"].dt.strftime("%Y-%m-%d")

    out = output.rename(columns={
        "region":"REGION","ds":"TIMESTAMP","anomaly_score":"ANOMALY_SCORE",
        "is_anomaly":"IS_ANOMALY","anomaly_type":"ANOMALY_TYPE","metric":"ENERGY_METRIC",
        "y":"VALUE_MW","yhat":"ROLLING_MEAN_MW","yhat_lower":"ROLLING_STD_MW",
    })[["REGION","TIMESTAMP","ANOMALY_SCORE","IS_ANOMALY","ANOMALY_TYPE",
        "ENERGY_METRIC","VALUE_MW","ROLLING_MEAN_MW","ROLLING_STD_MW"]]
    out = out.reset_index(drop=True)

    print(f"\nWriting {len(out)} rows to GOLD.ANOMALY_FLAGS ...")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS GOLD.ANOMALY_FLAGS (
            REGION VARCHAR, TIMESTAMP VARCHAR, ANOMALY_SCORE FLOAT,
            IS_ANOMALY BOOLEAN, ANOMALY_TYPE VARCHAR, ENERGY_METRIC VARCHAR,
            VALUE_MW FLOAT, ROLLING_MEAN_MW FLOAT, ROLLING_STD_MW FLOAT,
            MODEL_RUN_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.execute("TRUNCATE TABLE GOLD.ANOMALY_FLAGS")
    write_pandas(conn, out, "ANOMALY_FLAGS", schema="GOLD", database=SF_DATABASE)
    print("Done.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
