{# World Bank IDS geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__world_bank_ids() %}
-- World Bank IDS (#2819): the same World Bank economy codes as WDI (ISO3 countries, WB: groups such as LMY,
-- ECA), so the mapping is the WDI one under its own source_system; no IDS table is read here.
world_bank_ids as ({{ poc_world_bank_economies('world_bank_ids') }})
{% endmacro %}

{# World Bank IDS series built (gold.build_ids, #2819): external debt stocks and debt service, in US$ and as
   ratios. Debtor economy × counterpart WLD (the only counterpart in the archive); annual. #}
{% macro poc_ids_series() %}
    select * from (
        values
        ('DT.DOD.DECT.CD', 'usd'),
        ('DT.DOD.DECT.GN.ZS', 'pct_gni'),
        ('DT.TDS.DECT.CD', 'usd'),
        ('DT.TDS.DECT.GN.ZS', 'pct_gni'),
        ('DT.TDS.DECT.EX.ZS', 'pct_exports')
    ) as t (series_code, unit_code)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__world_bank_ids() %}
    {{ return({
        'block_model': 'poc_build_ids',
        'blocks': [
            ['IDS_CSV', 'World Bank International Debt Statistics'],
        ],
    }) }}
{% endmacro %}
