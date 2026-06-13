
{{
    config(materialized = "table")
}}

/*
dim_regions - German TSO control zone dimension
5 regions: DE national + 4 TSO control zones
*/

select distinct
    "region"                                   as region_code,
    "region_full"                              as region_name,
    cast("latitude" as float)                  as latitude,
    cast("longitude" as float)                 as longitude,
    case "region"
        when 'DE'         then 'National'
        when '50Hertz'    then 'TSO Zone'
        when 'Amprion'    then 'TSO Zone'
        when 'TenneT'     then 'TSO Zone'
        when 'TransnetBW' then 'TSO Zone'
        else 'Other'
    end                                        as region_type,
    case "region"
        when 'DE'         then 'Germany national grid'
        when '50Hertz'    then 'North and East Germany'
        when 'Amprion'    then 'West Germany Rhine-Ruhr'
        when 'TenneT'     then 'South and Central Germany'
        when 'TransnetBW' then 'Baden-Wuerttemberg'
        else 'Unknown'
    end                                        as region_description,
    current_timestamp()                        as dbt_updated_at

from {{ ref("stg_weather_unified") }}
