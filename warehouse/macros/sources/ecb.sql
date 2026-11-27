{# ECB geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__ecb() %}
-- ECB: ISO two-letter countries (GR, GB) and the aggregates of poc_ecb_areas (U2, I8, I9, B6, B0, D0); static
-- codes, so Gold does not read ECB Silver here. Countries come from dim_geography's country keys.
ecb_areas as ({{ poc_ecb_areas() }}),

ecb as (
    select
        'ecb' as source_system,
        area_code as source_code,
        candidate_key as geography_key
    from ecb_areas
    union all
    select
        'ecb' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
)
{% endmacro %}

{# ECB Data Portal blocks: one per landed selection (ingestion/sources/ecb.yaml entity). currencies: the EXR
   currencies built (EU members outside the euro area and the main world currencies); empty for other flows. #}
{% macro poc_ecb_blocks() %}
    select * from (
        values
        ('exr_daily', 'ECB euro reference exchange rates, daily', 'PLN,CZK,HUF,RON,SEK,DKK,BGN,USD,GBP,CHF,JPY,CNY,NOK'),
        ('exr_monthly', 'ECB euro reference exchange rates, monthly average', 'PLN,CZK,HUF,RON,SEK,DKK,BGN,USD,GBP,CHF,JPY,CNY,NOK'),
        ('fm_key_rates', 'Key ECB interest rates (date of changes)', ''),
        ('est_estr', 'Euro short-term rate (EUR STR)', ''),
        ('icp_annual_rate', 'HICP annual rate of change (ICP, discontinued after 2025-12)', ''),
        ('hicp_annual_rate', 'HICP annual rate of change (HICP, COICOP 2018)', ''),
        ('irs_long_term', 'Long-term interest rates for convergence purposes (10-year)', ''),
        ('bsi_monetary_aggregates', 'Euro area monetary aggregates M1, M2, M3', '')
    ) as t (entity, block_name, currencies)
{% endmacro %}

{# ECB area codes → Gold area (poc_map_geography section ecb; static, so Gold does not depend on ECB Silver
   existing). Countries are ISO two-letter codes and map by themselves; these are the aggregates (TITLE_COMPL in
   silver.ecb_observations, Dev trial 43088143150). I8/I9 are Eurostat's EA19/EA20 aggregates (#2775).
   I10 (euro area 21) and V5 have no Gold area yet. #}
{% macro poc_ecb_areas() %}
    select * from (
        values
        ('U2', 'EUROSTAT:EA'), ('I8', 'EUROSTAT:EA19'), ('I9', 'EUROSTAT:EA20'), ('B6', 'EUROSTAT:EU27_2020'),
        ('B0', 'EUROSTAT:EU'), ('D0', 'EUROSTAT:EU')
    ) as t (area_code, candidate_key)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__ecb() %}
    {{ return({
        'block_model': 'poc_build_ecb',
        'activated': "select 'ecb', entity, block_name from (" ~ poc_ecb_blocks() ~ ")",
    }) }}
{% endmacro %}
