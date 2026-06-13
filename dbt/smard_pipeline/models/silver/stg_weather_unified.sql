
{{
    config(
        materialized = "incremental",
        unique_key   = "\"weather_id\"",
        incremental_strategy = "merge",
        on_schema_change = "append_new_columns"
    )
}}

/*
stg_weather_unified
Silver layer - dbt transformation only
PySpark already cleaned data in Snowflake Silver
dbt only does: incremental merge + deduplication
Columns quoted because created with lowercase by pandas
*/

select
    "weather_id",
    "region",
    "region_full",
    "latitude",
    "longitude",
    "reading_ts",
    "reading_ts_15min",
    "observation_time",
    "wind_speed_ms",
    "wind_direction",
    "wind_gusts_ms",
    "solar_direct_wm2",
    "solar_diffuse_wm2",
    "temperature_c",
    "cloud_cover_pct",
    "precipitation_mm",
    "wind_category",
    "solar_category",
    "temp_category",
    "data_source",
    "_silver_processed_at",
    "ingestion_date"

from {{ source("silver_raw", "stg_weather_stream_clean") }}

{% if is_incremental() %}
where "_silver_processed_at" > (
    select max("_silver_processed_at") from {{ this }}
)
{% endif %}
