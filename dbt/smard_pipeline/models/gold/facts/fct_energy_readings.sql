
{{
    config(
        materialized         = "incremental",
        unique_key           = "\"reading_id\"",
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
fct_energy_readings - Central fact table
Joins energy readings with weather data
Grain: one row per 15-min per energy source per region
Join key: reading_ts_15min + region
*/

with energy as (
    select * from {{ ref("stg_energy_unified") }}
    {% if is_incremental() %}
    where "_silver_processed_at" > (
        select max("_silver_processed_at") from {{ this }}
    )
    {% endif %}
),

weather as (
    select * from {{ ref("stg_weather_unified") }}
)

select
    e."reading_id",
    e."reading_ts_15min",
    e."reading_date",
    e."reading_hour",
    e."reading_minute",
    e."region",
    e."region_full",
    e."energy_source",
    e."value_mw",
    e."is_renewable",
    e."is_fossil",
    e."rolling_avg_1h_mw",
    e."prev_value_mw",
    e."change_mw",
    e."change_pct",
    e."is_anomaly",
    e."time_of_day",
    e."data_source",
    w."temperature_c",
    w."wind_speed_ms",
    w."wind_direction",
    w."solar_direct_wm2",
    w."solar_diffuse_wm2",
    w."cloud_cover_pct",
    w."precipitation_mm",
    w."wind_category",
    w."solar_category",
    w."temp_category",
    e."_silver_processed_at",
    current_timestamp() as dbt_processed_at

from energy e
left join weather w
    on  e."reading_ts_15min" = w."reading_ts_15min"
    and e."region"           = w."region"
