
{{
    config(
        materialized         = "incremental",
        unique_key           = ["agg_date", "region"],
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
agg_weather_correlation - Weather vs generation correlation
Shows relationship between weather and energy output
Key insight for renewable energy planning
*/

select
    "reading_date"                             as agg_date,
    "region",
    "region_full",
    round(avg("temperature_c"), 2)             as avg_temperature_c,
    round(avg("wind_speed_ms"), 2)             as avg_wind_speed_ms,
    round(avg("solar_direct_wm2"), 2)          as avg_solar_wm2,
    round(avg("cloud_cover_pct"), 2)           as avg_cloud_cover_pct,
    round(avg("precipitation_mm"), 2)          as avg_precipitation_mm,
    round(avg(case when "energy_source" = 'wind_onshore'
        then "value_mw" end), 2)               as avg_wind_onshore_mw,
    round(avg(case when "energy_source" = 'wind_offshore'
        then "value_mw" end), 2)               as avg_wind_offshore_mw,
    round(avg(case when "energy_source" = 'solar'
        then "value_mw" end), 2)               as avg_solar_mw,
    current_timestamp()                        as dbt_updated_at
from {{ ref("fct_energy_readings") }}
where "temperature_c" is not null
{% if is_incremental() %}
and "reading_date" >= (
    select dateadd(day, -3, max(agg_date)) from {{ this }}
)
{% endif %}
group by "reading_date", "region", "region_full"
