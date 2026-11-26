{# World Bank WDI geography codes (D34 registration, docs/source-registration.md). #}

{# The World Bank economy codes of wdi_countries under `source_system`: a country by its ISO alpha-2 code, an
   aggregate as WB:<code>; a code dim_geography does not know is _X. WDI's own map, and the map of the World Bank
   sources that publish the same codes (IDS, WGI, Findex, ICP), built from wdi_countries only so that the shared
   map never waits for their Silver tables. Reads the CTE `geo` of poc_map_geography. #}
{% macro poc_world_bank_economies(source_system) %}
    select
        '{{ source_system }}' as source_system,
        w.economy_code as source_code,
        coalesce(g.geography_key, g_agg.geography_key, '_X') as geography_key
    from {{ ref('wdi_countries') }} as w
    left join geo as g on g.level_name = 'country' and w.iso2_code = g.iso2
    left join geo as g_agg on g_agg.geography_key = 'WB:' || w.economy_code
{% endmacro %}

{% macro poc_geography__world_bank_wdi() %}
world_bank_wdi as ({{ poc_world_bank_economies('world_bank_wdi') }})
{% endmacro %}
