{# World Bank GEM series built (gold.build_world_bank_gem): one block per landed series (entity series_<id>), the
   frequency built and the unit as GEM names it; areas: EU27 members (ISO alpha-3 through dim_geography.iso3). #}
{% macro poc_world_bank_gem_series() %}
    select * from (
        values
        ('series_CPTOTSAXNZGY', 'CPTOTSAXNZGY', 'M', 'pct', '% y-o-y', 'CPI price, % y-o-y (World Bank GEM)'),
        ('series_CPTOTSAXN', 'CPTOTSAXN', 'M', 'index', 'index', 'CPI price index, seasonally adjusted (World Bank GEM)'),
        ('series_CORESA', 'CORESA', 'M', 'index', 'index', 'Core CPI index, seasonally adjusted (World Bank GEM)'),
        ('series_IPTOTSAKD', 'IPTOTSAKD', 'M', 'constant_usd', 'constant US$', 'Industrial production, constant US$, seasonally adjusted (World Bank GEM)'),
        ('series_UNEMPSA_', 'UNEMPSA_', 'M', 'pct', '%', 'Unemployment rate, seasonally adjusted (World Bank GEM)'),
        ('series_RETSALESSA', 'RETSALESSA', 'M', 'index', 'index', 'Retail sales volume index, seasonally adjusted (World Bank GEM)'),
        ('series_DPANUSSPB', 'DPANUSSPB', 'M', 'lcu_per_usd', 'LCU per US$', 'Exchange rate, LCU per US$, period average (World Bank GEM)'),
        ('series_NYGDPMKTPSAKD', 'NYGDPMKTPSAKD', 'Q', 'mln_constant_2010_usd', 'constant 2010 US$, millions', 'GDP, constant 2010 US$, millions, seasonally adjusted (World Bank GEM)')
    ) as t (entity, series_code, frequency, unit_code, unit_name, block_name)
{% endmacro %}

{# D34 registration (docs/source-registration.md): the Gold block and the geography of World Bank GEM (#2373). #}
{% macro poc_source__world_bank_gem() %}
    {{ return({
        'block_model': 'poc_build_world_bank_gem',
        'activated': "select 'world_bank_gem', entity, block_name from (" ~ poc_world_bank_gem_series() ~ ")",
    }) }}
{% endmacro %}

{% macro poc_geography__world_bank_gem() %}
-- World Bank GEM (API source 15): countries by ISO 3166-1 alpha-3 (dim_geography iso3), built from Gold's own list
-- like oecd, so the dimensions never wait for the GEM source.
world_bank_gem as (
    select
        'world_bank_gem' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
)
{% endmacro %}
