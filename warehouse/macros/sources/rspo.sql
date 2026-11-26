{# rspo (register PLS-021): Silver helpers, Gold registration (D34, docs/source-registration.md) and geography.
   The yearly lists store an empty field as an empty cell (NULL, 2020-2024) or an empty string (2025); both are
   missing values, never zero. #}
{% macro poc_rspo_text(column) -%}
    nullif(trim({{ column }}), '')
{%- endmacro %}

{# A code as published (digits only, e.g. '02', '1465011'); any other text fails the build instead of becoming null. #}
{% macro poc_rspo_code(column, label) -%}
    case
        when {{ poc_rspo_text(column) }} is null then null
        when regexp_full_match({{ poc_rspo_text(column) }}, '[0-9]+') then {{ poc_rspo_text(column) }}
        else error('{{ label }}: not a code: ' || {{ column }})
    end
{%- endmacro %}

{# A count as published (a whole number >= 0, stored as a number); a missing count stays null, never 0; any other
   text fails the build. #}
{% macro poc_rspo_count(column, label) -%}
    case
        when {{ poc_rspo_text(column) }} is null then null
        when try_cast({{ poc_rspo_text(column) }} as double) >= 0
            and try_cast({{ poc_rspo_text(column) }} as double) = floor(try_cast({{ poc_rspo_text(column) }} as double))
            then cast(try_cast({{ poc_rspo_text(column) }} as double) as integer)
        else error('{{ label }}: not a count: ' || {{ column }})
    end
{%- endmacro %}

{# Full-time equivalents as published (Excel stores doubles, e.g. 12.299999999999999): rounded to 4 decimals. #}
{% macro poc_rspo_fte(column, label) -%}
    case
        when {{ poc_rspo_text(column) }} is null then null
        when try_cast({{ poc_rspo_text(column) }} as double) >= 0
            then cast(round(try_cast({{ poc_rspo_text(column) }} as double), 4) as decimal(12, 4))
        else error('{{ label }}: not a number: ' || {{ column }})
    end
{%- endmacro %}

{# Newest Landing copy of every yearly list (a Bronze table may briefly hold two before the latest-only prune). #}
{% macro poc_rspo_newest(table) -%}
    select *
    from {{ source('rspo', table) }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
{%- endmacro %}

{% macro poc_source__rspo() %}
    {{ return({'block_model': 'poc_build_rspo', 'blocks': [['institutions', 'RSPO schools and educational institutions (counts)']]}) }}
{% endmacro %}

{# Areas: the TERYT codes as published (voivodeship WW, powiat WWPP, gmina WWPPGGT, the codes of dim_geography) and
   PL for Poland. Static (dim_geography), no Silver read: the map never waits for this source. A code of an earlier
   list that TERYT no longer has maps to _X and is counted by the build check. #}
{% macro poc_geography__rspo() %}
rspo as (
    select
        'rspo' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat', 'gmina') and g.terc_code is not null
    union all
    select
        'rspo' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
