
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
    count(*)                                   as price_readings,
    round(avg("value_mw"), 2)                  as avg_price_eur_mwh,
    round(min("value_mw"), 2)                  as min_price_eur_mwh,
    round(max("value_mw"), 2)                  as max_price_eur_mwh,
    round(stddev("value_mw"), 2)               as stddev_price,
    sum(case when "value_mw" < 0
        then 1 else 0 end)                     as negative_price_hours,
    sum(case when "value_mw" > 100
        then 1 else 0 end)                     as high_price_hours,
    current_timestamp()                        as dbt_updated_at
from {{ ref("fct_energy_readings") }}
where "energy_source" = 'price_de_lu'
{% if is_incremental() %}
and "reading_date" >= (
    select dateadd(day, -3, max(agg_date)) from {{ this }}
)
{% endif %}
group by "reading_date", "region"
