
{{
    config(
        materialized         = "incremental",
        unique_key           = "agg_id",
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

select
    md5(cast("reading_date" as varchar)
        || cast("region" as varchar))          as agg_id,
    "reading_date"                             as agg_date,
    "region",
    "region_full",
    round(sum("value_mw" * 0.25), 2)          as total_generation_mwh,
    round(avg("value_mw"), 2)                  as avg_generation_mw,
    round(max("value_mw"), 2)                  as peak_generation_mw,
    round(sum(case when "is_renewable" = 'true'
        then "value_mw" * 0.25 else 0 end), 2) as renewable_mwh,
    round(
        sum(case when "is_renewable" = 'true'
            then "value_mw" else 0 end)
        / nullif(sum("value_mw"), 0) * 100
    , 2)                                       as renewable_pct,
    count(distinct "energy_source")            as active_sources,
    current_timestamp()                        as dbt_updated_at
from {{ ref("fct_energy_readings") }}
where "energy_source" not in ('consumption', 'price_de_lu')
{% if is_incremental() %}
and "reading_date" >= (
    select dateadd(day, -7, max(agg_date)) from {{ this }}
)
{% endif %}
group by "reading_date", "region", "region_full"
