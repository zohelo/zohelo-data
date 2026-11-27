{# UNCTAD geography codes (D34 registration, docs/source-registration.md). Reads the source's own Silver
   data through poc_optional_ref, so models/gold/poc_map_geography.sql lists those models in its config meta
   `optional_parents` (ops/repo_check.py pairs the two). #}
{% macro poc_geography__unctad() %}
-- UNCTAD: economies by UNCTAD code (M49; France 250 in the FDI, merchandise and services tables, 251 only in port
-- calls) from poc_unctad_areas: the EU27 members, 3412 European Union (2020 …) = EU27_2020, 0000 World. Other
-- economies have no ISO code in the tables (_X); the build stage builds only the listed areas, and
-- tests/unctad_areas_present.sql fails when a listed code is missing from one of the three built tables.
unctad_areas as ({{ poc_unctad_areas() }}),

unctad as (
    select
        'unctad' as source_system,
        c.economy as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct economy from {{ poc_optional_ref('unctad_fdi_flows_stock', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_merchandise_trade', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_services_trade', 'economy:varchar') }}
    ) as c
    left join unctad_areas as a on c.economy = a.economy
    left join geo as g on a.candidate_key = g.geography_key
    where c.economy is not null
)
{% endmacro %}

{# UNCTAD Data Hub blocks (gold.build_unctad): table × dimension codes (dimension_names, in that order) × measure.
   measure_field is the table's measure column; measure_suffix its short code in the measure key. #}
{% macro poc_unctad_blocks() %}
    select * from (
        values
        ('US_FdiFlowsStock', 'FDI inward flows (UNCTAD), US$ million', 'Flow,Direction', '08:1', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI outward flows (UNCTAD), US$ million', 'Flow,Direction', '08:2', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI inward stock (UNCTAD), US$ million', 'Flow,Direction', '09:1', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI outward stock (UNCTAD), US$ million', 'Flow,Direction', '09:2', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI inward stock (UNCTAD), % of GDP', 'Flow,Direction', '09:1', 'Percentage of Gross Domestic Product', 'PCT_GDP', 'PC_GDP', 'percent of GDP'),
        ('US_TradeMerchTotal', 'Merchandise exports (UNCTAD), US$ million', 'Flow', '02', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeMerchTotal', 'Merchandise imports (UNCTAD), US$ million', 'Flow', '01', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeMerchTotal', 'Merchandise exports (UNCTAD), growth year on year', 'Flow', '02', 'Growth rate, year-on-year', 'GROWTH', 'PCH', 'percent change on previous year'),
        ('US_TradeServCatTotal', 'Services exports (UNCTAD), US$ million', 'Flow,Category', '02:S', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeServCatTotal', 'Services imports (UNCTAD), US$ million', 'Flow,Category', '01:S', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices')
    ) as t (entity, block_name, dimension_names, dimension_codes, measure_field, measure_suffix, unit_code, unit_name)
{% endmacro %}

{# UNCTAD economies built (gold.build_unctad): EU27 members by UNCTAD code, the European Union (2020 …) group and World. #}
{% macro poc_unctad_areas() %}
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('191', 'HR'), ('196', 'CY'), ('203', 'CZ'), ('208', 'DK'),
        ('233', 'EE'), ('246', 'FI'), ('250', 'FR'), ('276', 'DE'), ('300', 'GR'), ('348', 'HU'), ('372', 'IE'),
        ('380', 'IT'), ('428', 'LV'), ('440', 'LT'), ('442', 'LU'), ('470', 'MT'), ('528', 'NL'), ('616', 'PL'),
        ('620', 'PT'), ('642', 'RO'), ('703', 'SK'), ('705', 'SI'), ('724', 'ES'), ('752', 'SE'),
        ('3412', 'EUROSTAT:EU27_2020'), ('0000', 'WB:WLD')
    ) as t (economy, candidate_key)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__unctad() %}
    {{ return({
        'block_model': 'poc_build_unctad',
        'activated': "select distinct 'unctad', entity, entity from (" ~ poc_unctad_blocks() ~ ")",
    }) }}
{% endmacro %}
