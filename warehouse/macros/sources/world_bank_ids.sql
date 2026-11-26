{# World Bank IDS geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_ids() %}
-- World Bank IDS (#2819): the same World Bank economy codes as WDI (ISO3 countries, WB: groups such as LMY,
-- ECA), so the mapping is the WDI one under its own source_system; no IDS table is read here.
world_bank_ids as ({{ poc_world_bank_economies('world_bank_ids') }})
{% endmacro %}
