
{{
    config(
        materialized         = "incremental",
        unique_key           = "agg_id",
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
agg_cross_border_daily
Gold layer - Daily cross-border flow aggregation
Shows daily totals per flow type for Germany
Key for energy independence analysis on dashboard
*/

select
    md5(cast("reading_date" as varchar)
        || cast("flow_name" as varchar))      as agg_id,
    "reading_date"                             as agg_date,
    "flow_name",
    "flow_type",
    "is_total",
    count(*)                                   as reading_count,
    round(sum(cast("value_mw" as float)), 2)   as total_flow_mwh,
    round(avg(cast("value_mw" as float)), 2)   as avg_flow_mw,
    round(max(cast("value_mw" as float)), 2)   as peak_flow_mw,
    round(min(cast("value_mw" as float)), 2)   as min_flow_mw,
    current_timestamp()                        as dbt_updated_at

from {{ ref("fct_cross_border_flows") }}

{% if is_incremental() %}
where "reading_date" >= (
    select dateadd(day, -3, max(agg_date)) from {{ this }}
)
{% endif %}

group by
    "reading_date",
    "flow_name",
    "flow_type",
    "is_total"
