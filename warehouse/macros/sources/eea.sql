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
