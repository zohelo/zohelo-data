{# Indicator catalogue (warehouse/dimensions/indicator.yaml), v1: World Bank WDI indicators.
   indicator_key = source_system:code. Eurostat datasets, BDL variables and NBP series join
   as their facts are modelled. #}
select
    'WDI:' || indicator_code as indicator_key,
    'world_bank_wdi' as source_system,
    indicator_code,
    indicator_name as name_en,
    topic,
    unit_of_measure,
    periodicity,
    short_definition,
    long_definition,
    source_note,
    license_type
from {{ ref('wdi_indicators') }}
