"""
query_templates.py
==================
Six Snowflake query functions, one per template.
Each returns (pd.DataFrame, chart_type, chart_config) so the
response layer knows how to render the result.
"""

import os
import pandas as pd
import snowflake.connector

def _get_sf_config():
    return {
        "account":   os.environ.get("SNOWFLAKE_ACCOUNT", "qg17675.europe-west3.gcp"),
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "role":      "LLM_AGENT_READONLY",
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  os.environ.get("SNOWFLAKE_DATABASE", "SMARD_PROD"),
        "schema":    "GOLD",
    }


def _run_query(sql: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(**_get_sf_config())
    try:
        # Disable Arrow iterator — use JSON format to avoid PyArrow
        # timestamp conversion bug on Python 3.12 / Snowflake connector
        conn.cursor().execute(
            "ALTER SESSION SET PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = 'JSON'"
        )
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Template 1 — FORECAST
# ---------------------------------------------------------------------------
def query_forecast() -> tuple:
    sql = """
        SELECT
            TO_VARCHAR(TIMESTAMP, 'YYYY-MM-DD') AS date,
            REGION AS region,
            PREDICTED_VALUE AS predicted_mwh,
            LOWER_BOUND AS lower_mwh,
            UPPER_BOUND AS upper_mwh
        FROM RENEWABLE_FORECAST
        ORDER BY date
    """
    df = _run_query(sql)
    chart_config = {
        "x": "date",
        "y": "predicted_mwh",
        "lower": "lower_mwh",
        "upper": "upper_mwh",
        "title": "14-Day Renewable Generation Forecast (MWh/day)",
        "y_label": "MWh",
    }
    return df, "line", chart_config


# ---------------------------------------------------------------------------
# Template 2 — ANOMALIES
# ---------------------------------------------------------------------------
def query_anomalies(days: int = 90) -> tuple:
    sql = f"""
        SELECT
            TO_VARCHAR(TIMESTAMP, 'YYYY-MM-DD') AS date,
            ENERGY_METRIC AS metric,
            ANOMALY_TYPE AS type,
            ROUND(ABS(ANOMALY_SCORE), 4) AS severity,
            ROUND(VALUE_MW, 2) AS actual_mwh,
            ROUND(ROLLING_MEAN_MW, 2) AS expected_mwh
        FROM ANOMALY_FLAGS
        WHERE IS_ANOMALY = TRUE
          AND TIMESTAMP >= DATEADD(day, -{days}, CURRENT_DATE())
        ORDER BY severity DESC
    """
    df = _run_query(sql)
    chart_config = {
        "title": f"Anomalies Detected (Last {days} Days)",
        "columns": ["date", "metric", "type", "severity", "actual_mwh", "expected_mwh"],
    }
    return df, "table", chart_config


# ---------------------------------------------------------------------------
# Template 3 — RENEWABLE SHARE
# ---------------------------------------------------------------------------
def query_renewable_share(days: int = 30) -> tuple:
    sql = f"""
        SELECT
            AGG_DATE AS date,
            ROUND(RENEWABLE_MWH, 2) AS renewable_mwh,
            ROUND(TOTAL_GENERATION_MWH, 2) AS total_mwh,
            ROUND(RENEWABLE_PCT, 2) AS renewable_pct
        FROM AGG_REGIONAL_COMPARISON
        WHERE "region" = 'DE'
          AND AGG_DATE >= DATEADD(day, -{days}, CURRENT_DATE())
          AND AGG_DATE < CURRENT_DATE()
        ORDER BY date
    """
    df = _run_query(sql)
    chart_config = {
        "x": "date",
        "y": "renewable_pct",
        "title": f"Daily Renewable Share % (Last {days} Days)",
        "y_label": "Renewable %",
    }
    return df, "bar", chart_config


# ---------------------------------------------------------------------------
# Template 4 — DEMAND
# ---------------------------------------------------------------------------
def query_demand(days: int = 30) -> tuple:
    sql = f"""
        SELECT
            "reading_date" AS date,
            ROUND(SUM("value_mw"), 2) AS daily_demand_mwh
        FROM FCT_ENERGY_READINGS
        WHERE "energy_source" = 'consumption'
          AND "reading_date" >= DATEADD(day, -{days}, CURRENT_DATE())
          AND "reading_date" < CURRENT_DATE()
        GROUP BY "reading_date"
        ORDER BY date
    """
    df = _run_query(sql)
    chart_config = {
        "x": "date",
        "y": "daily_demand_mwh",
        "title": f"Daily Electricity Demand (Last {days} Days)",
        "y_label": "MWh",
    }
    return df, "line", chart_config


# ---------------------------------------------------------------------------
# Template 5 — GENERATION BY SOURCE
# ---------------------------------------------------------------------------
def query_generation(days: int = 30) -> tuple:
    sql = f"""
        SELECT
            "energy_source" AS source,
            ROUND(SUM("value_mw"), 2) AS total_mwh,
            ROUND(AVG("value_mw"), 2) AS avg_mwh_per_period,
            "is_renewable" AS is_renewable
        FROM FCT_ENERGY_READINGS
        WHERE "energy_source" NOT IN ('consumption', 'price_de_lu')
          AND "reading_date" >= DATEADD(day, -{days}, CURRENT_DATE())
          AND "reading_date" < CURRENT_DATE()
        GROUP BY "energy_source", "is_renewable"
        ORDER BY total_mwh DESC
    """
    df = _run_query(sql)
    chart_config = {
        "x": "total_mwh",
        "y": "source",
        "title": f"Generation by Energy Source (Last {days} Days)",
        "x_label": "Total MWh",
        "color": "is_renewable",
    }
    return df, "bar_horizontal", chart_config


# ---------------------------------------------------------------------------
# Template 6 — YEAR-OVER-YEAR COMPARISON
# ---------------------------------------------------------------------------
def query_comparison() -> tuple:
    sql = """
        SELECT
            MONTH(TRY_TO_DATE(AGG_DATE)) AS month,
            YEAR(TRY_TO_DATE(AGG_DATE)) AS year,
            ROUND(AVG(RENEWABLE_PCT), 2) AS avg_renewable_pct,
            ROUND(AVG(RENEWABLE_MWH), 2) AS avg_renewable_mwh
        FROM AGG_REGIONAL_COMPARISON
        WHERE "region" = 'DE'
          AND YEAR(TRY_TO_DATE(AGG_DATE)) IN (YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) - 1)
        GROUP BY YEAR(TRY_TO_DATE(AGG_DATE)), MONTH(TRY_TO_DATE(AGG_DATE))
        ORDER BY year, month
    """
    df = _run_query(sql)
    chart_config = {
        "x": "month",
        "y": "avg_renewable_pct",
        "color": "year",
        "title": "Renewable Share % — This Year vs Last Year",
        "y_label": "Avg Renewable %",
    }
    return df, "line", chart_config


# ---------------------------------------------------------------------------
# Template router
# ---------------------------------------------------------------------------
TEMPLATE_MAP = {
    "FORECAST":        query_forecast,
    "ANOMALIES":       query_anomalies,
    "RENEWABLE_SHARE": query_renewable_share,
    "DEMAND":          query_demand,
    "GENERATION":      query_generation,
    "COMPARISON":      query_comparison,
}


def run_template(template_name: str) -> tuple:
    """Run the named template and return (df, chart_type, chart_config)."""
    fn = TEMPLATE_MAP.get(template_name)
    if not fn:
        return pd.DataFrame(), None, {}
    return fn()
