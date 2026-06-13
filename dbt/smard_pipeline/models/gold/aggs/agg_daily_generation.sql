
{{
    config(
        materialized         = "incremental",
        unique_key           = ["agg_date", "region", "energy_source"],
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
agg_daily_generation - Daily generation summary
GROUP BY date + region + energy source
*/

select
    "reading_date"                             as agg_date,
    "region",
    "region_full",
    "energy_source",
    "is_renewable",
    "is_fossil",
    count(*)                                   as reading_count,
    round(sum("value_mw" * 0.25), 2)          as total_generation_mwh,
    round(avg("value_mw"), 2)                  as avg_generation_mw,
    round(max("value_mw"), 2)                  as peak_generation_mw,
    round(min("value_mw"), 2)                  as min_generation_mw,
    round(avg("rolling_avg_1h_mw"), 2)         as avg_rolling_1h_mw,
    sum(case when "is_anomaly" = 'true'
        then 1 else 0 end)                     as anomaly_count,
    current_timestamp()                        as dbt_updated_at
from {{ ref("fct_energy_readings") }}
where "energy_source" not in ('consumption', 'price_de_lu')
{% if is_incremental() %}
and "reading_date" >= (
    select dateadd(day, -3, max(agg_date)) from {{ this }}
)
{% endif %}
group by
    "reading_date", "region", "region_full",
    "energy_source", "is_renewable", "is_fossil"
