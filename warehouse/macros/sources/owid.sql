{# Our World in Data (register CD-031, #2982): D34 registration (docs/source-registration.md). #}
{# The selected charts (one Gold block each): chart = Landing entity, indicator = the value column's short name in
   the chart CSV, published_unit = the column's unit in metadata.json as published (tests/owid_charts_declared.sql
   fails when the published unit changes), unit_code / unit_name for the measure. One value column per chart; the
   attribute columns (owid_region, gdp_per_capita__annotations) are not measures. #}
{% macro poc_owid_charts() %}
    select * from (
        values
        (
            'consumption_co2_per_capita', 'consumption_emissions_per_capita', 'tonnes per person',
            't_co2_per_person', 'tonnes of CO2 per person',
            'Consumption-based CO2 emissions per capita (Global Carbon Budget, via Our World in Data)'
        ),
        (
            'electoral_democracy_index', 'electdem_vdem__estimate_best', '',
            'index_0_1', 'index (0 to 1)',
            'Electoral democracy index, best estimate (V-Dem, via Our World in Data)'
        ),
        (
            'gdp_per_capita_maddison', 'gdp_per_capita', 'international-$ in 2011 prices',
            'intl_usd_2011_per_person', 'international-$ in 2011 prices per person',
            'GDP per capita (Maddison Project Database, via Our World in Data)'
        )
    ) as t (chart, indicator, published_unit, unit_code, unit_name, measure_name)
{% endmacro %}

{% macro poc_source__owid() %}
    {{ return({
        'block_model': 'poc_build_owid',
        'activated': "select 'owid', chart, measure_name from (" ~ poc_owid_charts() ~ ")",
    }) }}
{% endmacro %}

{# Countries by ISO 3166-1 alpha-3 (dim_geography.iso3, static, so the map never waits for this source), OWID_WRL
   (World) as WB:WLD and OWID_KOS (Kosovo) as the World Bank's XKX. Not mapped (Silver only): OWID's continents,
   income groups and other regions (no code), its other OWID_… codes (historical states such as the USSR,
   Czechoslovakia, Yugoslavia) and ISO codes that dim_geography does not hold. #}
{% macro poc_geography__owid() %}
owid as (
    select
        'owid' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'owid' as source_system,
        'OWID_WRL' as source_code,
        'WB:WLD' as geography_key
    union all
    select
        'owid' as source_system,
        'OWID_KOS' as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 = 'XKX'
)
{% endmacro %}
