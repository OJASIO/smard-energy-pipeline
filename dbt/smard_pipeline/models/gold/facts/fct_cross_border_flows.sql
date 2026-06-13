
{{
    config(
        materialized         = "incremental",
        unique_key           = "flow_id",
        incremental_strategy = "merge",
        on_schema_change     = "append_new_columns"
    )
}}

/*
fct_cross_border_flows
Gold layer - Cross-border electricity flow fact table
Source: SMARD API cross-border flow filters
Grain: one row per hour per flow type
*/

select
    "flow_id",
    "timestamp_ms",
    "reading_ts",
    "reading_ts_15min",
    "reading_date",
    "reading_hour",
    "flow_name",
    "flow_type",
    "is_total",
    "filter_id",
    "region",
    "value_mw",
    "data_source",
    "_silver_processed_at",
    current_timestamp() as dbt_processed_at

from {{ source("silver_raw", "stg_cross_border_clean") }}

{% if is_incremental() %}
where "_silver_processed_at" > (
    select max("_silver_processed_at") from {{ this }}
)
{% endif %}
