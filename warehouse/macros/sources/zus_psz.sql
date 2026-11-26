{# ZUS statistical portal geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__zus_psz() %}
-- ZUS statistical portal (PLS-009): '00' = Poland; voivodeships by their 2-digit TERYT code (the Silver model matched
-- ZUS's voivodeship names to these codes). Built from TERYT, so the dimensions never wait for the ZUS source.
zus_psz as (
    select
        'zus_psz' as source_system,
        terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
    where unit_level = 'wojewodztwo'
    union all
    select
        'zus_psz' as source_system,
        '00' as source_code,
        'PL' as geography_key
)
{% endmacro %}
