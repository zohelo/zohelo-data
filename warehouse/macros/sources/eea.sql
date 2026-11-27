{# EEA geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__eea() %}
-- EEA greenhouse gas inventory: countries by ISO 3166-1 alpha-2 as published (Greece GR), built from Gold's own
-- list like oecd, so the dimensions never wait for the EEA source; the EEA aggregate EUA has no Gold key (_X).
eea as (
    select
        'eea' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
)
{% endmacro %}

{# EEA greenhouse gas inventory (gold.build_eea, block govreg_ghg_inventory): the sectors built, CRF codes as
   published; is_total marks the national totals (without and with LULUCF). #}
{% macro poc_eea_sectors() %}
    select * from (
        values
        ('Sectors/Totals_excl', true),
        ('Sectors/Totals_incl', true),
        ('1', false),
        ('2', false),
        ('3', false),
        ('4', false),
        ('5', false)
    ) as t (sector_code, is_total)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__eea() %}
    {{ return({
        'block_model': 'poc_build_eea',
        'blocks': [
            ['govreg_ghg_inventory', 'Greenhouse gas emissions by sector, GovReg inventory (EEA)'],
        ],
    }) }}
{% endmacro %}
