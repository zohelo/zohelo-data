{# GLEIF geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__gleif() %}
-- GLEIF (#2740): countries of the legal jurisdiction by ISO 3166-1 alpha-2; Polish legal-address regions by
-- ISO 3166-2, PL-<TERC> since 2018 and the legacy letter codes still found in the golden copy.
gleif as (
    select
        'gleif' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
    union all
    select
        'gleif' as source_system,
        'PL-' || terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
    union all
    select
        'gleif' as source_system,
        'PL-' || letters as source_code,
        'TERC:' || terc_code as geography_key
    from (
        values
        ('DS', '02'), ('KP', '04'), ('LU', '06'), ('LB', '08'), ('LD', '10'), ('MA', '12'), ('MZ', '14'),
        ('OP', '16'), ('PK', '18'), ('PD', '20'), ('PM', '22'), ('SL', '24'), ('SK', '26'), ('WN', '28'),
        ('WP', '30'), ('ZP', '32')
    ) as t (letters, terc_code)
)
{% endmacro %}

{# Gold block (D34; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__gleif() %}
    {{ return({
        'block_model': 'poc_build_gleif',
        'blocks': [
            ['lei2', 'GLEIF LEI records (golden copy)'],
        ],
    }) }}
{% endmacro %}
