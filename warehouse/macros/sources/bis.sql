{# BIS Data Portal (gold.build_bis, #2861): D34 registration (docs/source-registration.md). #}
{# BIS Data Portal measures (gold.build_bis, #2861): one row per built series of a landed selection
   (ingestion/sources/bis.yaml entity). series_code = the series key without FREQ and the area, as the BIS
   publishes it; measure_codes = the codes that stay in the measure key next to the unit; unit_code / unit_name
   as published (BIS unit codes: 368 per cent per year, 882 index 2020 = 100, 628 index 2010 = 100, 771
   year-on-year changes in per cent, 770 percentage of GDP, 367 per cent); source_unit = UNIT_MEASURE of the
   series in silver.bis_observations (tests/bis_gold_units.sql fails when the BIS changes it, e.g. a rebased
   index). Total credit (tc): percentage of GDP, adjusted for breaks only; amounts in national currency and US
   dollars, the unadjusted series, credit from banks and to the whole non-financial sector, and the narrow-basket (27 economies) effective exchange
   rates are not built. #}
{% macro poc_bis_measures() %}
    select * from (
        values
        ('cbpol', '', '', '368', 'Per cent per year', '368', 'Central bank policy rate, end of month'),
        ('eer', 'R.B', 'R.B', '882', 'Index, 2020 = 100', '882', 'Real effective exchange rate, CPI-based, broad basket (64 economies)'),
        ('eer', 'N.B', 'N.B', '882', 'Index, 2020 = 100', '882', 'Nominal effective exchange rate, broad basket (64 economies)'),
        ('spp', 'N.628', 'N', '628', 'Index, 2010 = 100', '628', 'Residential property prices, nominal'),
        ('spp', 'R.628', 'R', '628', 'Index, 2010 = 100', '628', 'Residential property prices, real (CPI-deflated)'),
        ('spp', 'N.771', 'N', '771', 'Year-on-year changes, in per cent', '771', 'Residential property prices, nominal, year-on-year change'),
        ('spp', 'R.771', 'R', '771', 'Year-on-year changes, in per cent', '771', 'Residential property prices, real (CPI-deflated), year-on-year change'),
        ('tc', 'P.A.M.770.A', 'P.A.M', '770', 'Percentage of GDP', '367', 'Credit to the private non-financial sector from all sectors, market value, adjusted for breaks'),
        ('tc', 'H.A.M.770.A', 'H.A.M', '770', 'Percentage of GDP', '367', 'Credit to households and NPISHs from all sectors, market value, adjusted for breaks'),
        ('tc', 'N.A.M.770.A', 'N.A.M', '770', 'Percentage of GDP', '367', 'Credit to non-financial corporations from all sectors, market value, adjusted for breaks'),
        ('tc', 'G.A.N.770.A', 'G.A.N', '770', 'Percentage of GDP', '367', 'Credit to general government from all sectors, nominal value, adjusted for breaks'),
        ('credit_gap', 'P.A.A', 'P.A.A', '770', 'Percentage of GDP', '770', 'Credit-to-GDP ratio, private non-financial sector, actual data'),
        ('credit_gap', 'P.A.B', 'P.A.B', '770', 'Percentage of GDP', '770', 'Credit-to-GDP ratio, private non-financial sector, trend (one-sided HP filter)'),
        ('credit_gap', 'P.A.C', 'P.A.C', '770', 'Percentage of GDP', '770', 'Credit-to-GDP gap, private non-financial sector (actual minus trend)'),
        ('dsr', 'P', 'P', '367', 'Per cent', '367', 'Debt service ratio, private non-financial sector'),
        ('dsr', 'H', 'H', '367', 'Per cent', '367', 'Debt service ratio, households and NPISHs'),
        ('dsr', 'N', 'N', '367', 'Per cent', '367', 'Debt service ratio, non-financial corporations')
    ) as t (entity, series_code, measure_codes, unit_code, unit_name, source_unit, measure_name)
{% endmacro %}

{# BIS selections built (block = landed entity). #}
{% macro poc_bis_blocks() %}
    select * from (
        values
        ('cbpol', 'BIS central bank policy rates, monthly'),
        ('eer', 'BIS effective exchange rates, monthly'),
        ('spp', 'BIS selected residential property prices, quarterly'),
        ('tc', 'BIS total credit to the non-financial sector, quarterly'),
        ('credit_gap', 'BIS credit-to-GDP gaps, quarterly'),
        ('dsr', 'BIS debt service ratios, quarterly')
    ) as t (entity, block_name)
{% endmacro %}

{# BIS area codes that are not ISO 3166 alpha-2 countries (silver.bis_observations, Dev probe 43281574547):
   XM euro area → the Eurostat euro-area member (as ECB U2); the BIS aggregates have no Gold area and are not
   built (candidate_key ''): 4T emerging market economies, 5R advanced economies, 5A all reporting economies,
   G2 G20, XW world. Static, so Gold does not read BIS Silver in poc_map_geography. #}
{% macro poc_bis_areas() %}
    select * from (
        values
        ('XM', 'EUROSTAT:EA'), ('4T', ''), ('5R', ''), ('5A', ''), ('G2', ''), ('XW', '')
    ) as t (area_code, candidate_key)
{% endmacro %}

{# The series code of a row of silver.bis_observations (alias o): the series key without FREQ and the area, as
   poc_bis_measures lists it. One expression for the build stage and the tests that read Silver
   (tests/bis_gold_units.sql, tests/bis_series_not_empty.sql). #}
{% macro poc_bis_series_code() %}
        case o.entity
            when 'cbpol' then ''
            when 'eer'
                then
                    json_extract_string(o.dimensions_json, '$.EER_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.EER_BASKET')
            when 'spp'
                then
                    json_extract_string(o.dimensions_json, '$.VALUE')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_MEASURE')
            when 'tc'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.VALUATION')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_ADJUST')
            when 'credit_gap'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.CG_DTYPE')
            when 'dsr' then json_extract_string(o.dimensions_json, '$.DSR_BORROWERS')
        end
{%- endmacro %}

{% macro poc_source__bis() %}
    {{ return({
        'block_model': 'poc_build_bis',
        'activated': "select 'bis', entity, block_name from (" ~ poc_bis_blocks() ~ ")",
    }) }}
{% endmacro %}

{# ISO 3166 alpha-2 countries (dim_geography's country keys) and XM euro area → EUROSTAT:EA (poc_bis_areas; the
   BIS aggregates 4T, 5R, 5A, G2, XW have no Gold area and are not listed). Static: Gold does not read BIS
   Silver here. #}
{% macro poc_geography__bis() %}
bis_areas as ({{ poc_bis_areas() }}),

bis as (
    select
        'bis' as source_system,
        area_code as source_code,
        candidate_key as geography_key
    from bis_areas
    where candidate_key <> ''
    union all
    select
        'bis' as source_system,
        geography_key as source_code,
        geography_key
    from geo
    where level_name = 'country' and length(geography_key) = 2
)
{% endmacro %}
