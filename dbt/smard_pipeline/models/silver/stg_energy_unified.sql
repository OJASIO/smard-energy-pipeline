
{{
    config(
        materialized = "incremental",
        unique_key   = "reading_id",
        incremental_strategy = "merge",
        on_schema_change = "append_new_columns"
    )
}}

/*
stg_energy_unified
Silver layer - dbt transformation only
PySpark already cleaned data in Snowflake Silver
dbt only does: incremental merge + deduplication
Columns quoted because created with lowercase by pandas
*/

select
    "reading_id",
    "timestamp_ms",
    "reading_ts",
    "reading_date",
    "reading_hour",
    "reading_minute",
    "reading_ts_15min",
    "energy_source",
    "filter_id",
    "region",
    "region_full",
    "value_mw",
    "is_renewable",
    "is_fossil",
    "rolling_avg_1h_mw",
    "prev_value_mw",
    "change_mw",
    "change_pct",
    "is_anomaly",
    "time_of_day",
    "data_source",
    "_silver_processed_at",
    "ingestion_date"

from {{ source("silver_raw", "stg_energy_stream_clean") }}

{% if is_incremental() %}
where "_silver_processed_at" > (
    select max("_silver_processed_at") from {{ this }}
)
{% endif %}
