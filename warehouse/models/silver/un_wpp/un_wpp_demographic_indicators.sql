{# UN World Population Prospects 2024, demographic indicators, medium variant (entity
   demographic_indicators_medium of bronze.un_wpp_rows): one row per location × year of the newest Landing copy,
   typed. Fields are taken by name from field_names, so a revision that reorders columns still maps. Years
   1950-2023 are estimates, 2024-2100 projections (is_projection); 2101 carries only TPopulation1Jan. Units as
   published: populations, births, deaths and migrants in thousands; rates per 1,000; TFR children per woman;
   life expectancy in years. loc_type is empty for the ad-hoc groups (e.g. European Union (EU: 27), LocID 1551). #}
{%- set indicators = [
    'TPopulation1Jan', 'TPopulation1July', 'TPopulationMale1July', 'TPopulationFemale1July', 'PopDensity',
    'PopSexRatio', 'MedianAgePop', 'NatChange', 'NatChangeRT', 'PopChange', 'PopGrowthRate', 'DoublingTime',
    'Births', 'Births1519', 'CBR', 'TFR', 'NRR', 'MAC', 'SRB', 'Deaths', 'DeathsMale', 'DeathsFemale', 'CDR',
    'LEx', 'LExMale', 'LExFemale', 'LE15', 'LE15Male', 'LE15Female', 'LE65', 'LE65Male', 'LE65Female', 'LE80',
    'LE80Male', 'LE80Female', 'InfantDeaths', 'IMR', 'LBsurvivingAge1', 'Under5Deaths', 'Q5', 'Q0040',
    'Q0040Male', 'Q0040Female', 'Q0060', 'Q0060Male', 'Q0060Female', 'Q1550', 'Q1550Male', 'Q1550Female',
    'Q1560', 'Q1560Male', 'Q1560Female', 'NetMigrations', 'CNMR'
] -%}
with newest as (
    select
        *,
        string_split(field_names, ',') as f,
        string_split(field_values, '|') as v
    from {{ source('un_wpp', 'un_wpp_rows') }}
    where entity = 'demographic_indicators_medium'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    cast(v[list_position(f, 'LocID')] as integer) as loc_id,
    v[list_position(f, 'Location')] as location,
    nullif(v[list_position(f, 'ISO2_code')], '') as iso2,
    nullif(v[list_position(f, 'ISO3_code')], '') as iso3,
    nullif(v[list_position(f, 'LocTypeName')], '') as loc_type,
    nullif(v[list_position(f, 'Variant')], '') as variant,
    cast(v[list_position(f, 'Time')] as integer) as year,
    cast(v[list_position(f, 'Time')] as integer) >= 2024 as is_projection,
    {%- for c in indicators %}
    try_cast(nullif(v[list_position(f, '{{ c }}')], '') as double) as {{ c | lower }},
    {%- endfor %}
    raw_sha256,
    source_version,
    ingested_at
from newest
