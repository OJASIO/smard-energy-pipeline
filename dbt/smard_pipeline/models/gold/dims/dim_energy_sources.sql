
{{
    config(materialized = "table")
}}

/*
dim_energy_sources - Energy source dimension
Static reference data for all 14 SMARD energy sources
*/

select
    energy_source                              as source_code,
    case energy_source
        when 'wind_onshore'       then 'Wind Onshore'
        when 'wind_offshore'      then 'Wind Offshore'
        when 'solar'              then 'Solar PV'
        when 'biomass'            then 'Biomass'
        when 'hydro'              then 'Hydro'
        when 'pumped_storage'     then 'Pumped Storage'
        when 'other_renewables'   then 'Other Renewables'
        when 'other_conventional' then 'Other Conventional'
        when 'lignite'            then 'Lignite Brown Coal'
        when 'nuclear'            then 'Nuclear'
        when 'hard_coal'          then 'Hard Coal'
        when 'natural_gas'        then 'Natural Gas'
        when 'consumption'        then 'Total Consumption'
        when 'price_de_lu'        then 'Day-Ahead Price DE/LU'
        else energy_source
    end                                        as source_name,
    case energy_source
        when 'wind_onshore'       then 'Renewable'
        when 'wind_offshore'      then 'Renewable'
        when 'solar'              then 'Renewable'
        when 'biomass'            then 'Renewable'
        when 'hydro'              then 'Renewable'
        when 'pumped_storage'     then 'Renewable'
        when 'other_renewables'   then 'Renewable'
        when 'lignite'            then 'Fossil'
        when 'hard_coal'          then 'Fossil'
        when 'natural_gas'        then 'Fossil'
        when 'nuclear'            then 'Nuclear'
        when 'other_conventional' then 'Conventional'
        when 'consumption'        then 'Demand'
        when 'price_de_lu'        then 'Price'
        else 'Other'
    end                                        as energy_category,
    case energy_source
        when 'wind_onshore'       then true
        when 'wind_offshore'      then true
        when 'solar'              then true
        when 'biomass'            then true
        when 'hydro'              then true
        when 'pumped_storage'     then true
        when 'other_renewables'   then true
        else false
    end                                        as is_renewable,
    case energy_source
        when 'lignite'            then true
        when 'hard_coal'          then true
        when 'natural_gas'        then true
        when 'other_conventional' then true
        else false
    end                                        as is_fossil,
    case energy_source
        when 'lignite'            then 820
        when 'hard_coal'          then 740
        when 'natural_gas'        then 490
        when 'other_conventional' then 600
        when 'nuclear'            then 12
        when 'biomass'            then 230
        when 'hydro'              then 4
        when 'wind_onshore'       then 11
        when 'wind_offshore'      then 12
        when 'solar'              then 41
        when 'pumped_storage'     then 0
        when 'other_renewables'   then 50
        else 0
    end                                        as co2_intensity_gkwh,
    current_timestamp()                        as dbt_updated_at
from (
    select distinct "energy_source" as energy_source
    from {{ ref("stg_energy_unified") }}
)
