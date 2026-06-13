
{{
    config(
        materialized         = "incremental",
        unique_key           = ["region", "energy_source", "reading_hour"],
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
agg_hourly_patterns - Hourly generation patterns
Shows typical generation profile by hour
Useful for demand forecasting dashboard
*/

select
    "region",
    "region_full",
    "energy_source",
    "is_renewable",
    "reading_hour",
    "time_of_day",
    count(*)                                   as reading_count,
    round(avg("value_mw"), 2)                  as avg_generation_mw,
    round(max("value_mw"), 2)                  as peak_generation_mw,
    round(min("value_mw"), 2)                  as min_generation_mw,
    round(stddev("value_mw"), 2)               as stddev_generation_mw,
    current_timestamp()                        as dbt_updated_at
from {{ ref("fct_energy_readings") }}
where "energy_source" not in ('consumption', 'price_de_lu')
{% if is_incremental() %}
and "_silver_processed_at" > (
    select max("_silver_processed_at") from {{ this }}
)
{% endif %}
group by
    "region", "region_full", "energy_source",
    "is_renewable", "reading_hour", "time_of_day"
