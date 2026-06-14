{{
    config(
        materialized = "incremental",
        unique_key   = '"reading_id"',
        incremental_strategy = "merge",
        on_schema_change = "append_new_columns"
    )
}}
/*
stg_energy_unified
Silver layer - dbt transformation only
PySpark already cleaned both stream and historical data
dbt does: UNION ALL stream + historical + dedup + incremental merge
*/
with stream as (
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
        select max("_silver_processed_at")
        from {{ this }}
        where "data_source" = 'stream'
    )
    {% endif %}
),
historical as (
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
    from {{ source("silver_raw", "stg_energy_historical_clean") }}
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
        partition by "reading_id"
        order by "_silver_processed_at" desc
    ) = 1
)
select * from deduped
