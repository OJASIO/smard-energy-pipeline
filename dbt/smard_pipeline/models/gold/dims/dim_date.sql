
{{
    config(materialized = "table")
}}

/*
dim_date - Date dimension
Built from energy readings date spine
*/

with date_spine as (
    select distinct
        cast("reading_date" as date) as calendar_date
    from {{ ref("stg_energy_unified") }}
    where "reading_date" is not null
)

select
    to_char(calendar_date, 'YYYYMMDD')        as date_sk,
    calendar_date,
    year(calendar_date)                        as year,
    month(calendar_date)                       as month,
    quarter(calendar_date)                     as quarter,
    day(calendar_date)                         as day_of_month,
    dayofweek(calendar_date)                   as day_of_week,
    dayname(calendar_date)                     as day_name,
    monthname(calendar_date)                   as month_name,
    case
        when dayofweek(calendar_date) in (0, 6)
        then true else false
    end                                        as is_weekend,
    case
        when month(calendar_date) in (12, 1, 2) then 'winter'
        when month(calendar_date) in (3, 4, 5)  then 'spring'
        when month(calendar_date) in (6, 7, 8)  then 'summer'
        else 'autumn'
    end                                        as season,
    current_timestamp()                        as dbt_updated_at
from date_spine
order by calendar_date
