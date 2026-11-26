{# GUS TERYT geography codes (D34 registration, docs/source-registration.md). #}
{% macro poc_geography__gus_teryt() %}
-- TERYT TERC codes are TERC keys (#2739).
gus_teryt as (
    select
        'gus_teryt' as source_system,
        terc_code as source_code,
        'TERC:' || terc_code as geography_key
    from {{ ref('teryt_terc') }}
)
{% endmacro %}
