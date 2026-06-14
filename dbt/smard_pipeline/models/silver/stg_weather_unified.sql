{{
    config(
        materialized = "incremental",
        unique_key   = '"weather_id"',
        incremental_strategy = "merge",
        on_schema_change = "append_new_columns"
    )
}}
/*
stg_weather_unified
Silver layer - UNION ALL stream + historical weather
dbt does: UNION ALL + dedup + incremental merge
*/
with stream as (
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
        select max("_silver_processed_at")
        from {{ this }}
        where "data_source" = 'stream'
    )
    {% endif %}
),
historical as (
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
    from {{ source("silver_raw", "stg_weather_historical_clean") }}
    {% if is_incremental() %}
    where "_silver_processed_at" > (
        select max("_silver_processed_at")
        from {{ this }}
        where "data_source" = 'historical'
    )
    {% endif %}
),
combined as (
    select * from stream
    union all
    select * from historical
),
deduped as (
    select *
    from combined
    qualify row_number() over (
        partition by "weather_id"
        order by "_silver_processed_at" desc
    ) = 1
)
select * from deduped
