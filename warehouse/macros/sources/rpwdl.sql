{# rpwdl (register PLS-020): Silver helpers, Gold registration (D34, docs/source-registration.md) and geography.
   The register files write the text NULL for an empty field and 'brak wpisu' ("no entry") in some code columns;
   both are missing values, never zero. #}
{% macro poc_rpwdl_text(column) -%}
    case when lower(trim({{ column }})) in ('null', '', 'brak wpisu') then null else trim({{ column }}) end
{%- endmacro %}

{# A date as published (YYYY-MM-DD); any other text fails the build instead of becoming null. #}
{% macro poc_rpwdl_date(column, label) -%}
    case
        when {{ poc_rpwdl_text(column) }} is null then null
        when try_cast({{ poc_rpwdl_text(column) }} as date) is not null then cast({{ poc_rpwdl_text(column) }} as date)
        else error('{{ label }}: not a date: ' || {{ column }})
    end
{%- endmacro %}

{# A count as published; any other text fails the build (a missing count stays null, never 0). #}
{% macro poc_rpwdl_count(column, label) -%}
    case
        when {{ poc_rpwdl_text(column) }} is null then null
        when try_cast({{ poc_rpwdl_text(column) }} as integer) is not null then cast({{ poc_rpwdl_text(column) }} as integer)
        else error('{{ label }}: not a count: ' || {{ column }})
    end
{%- endmacro %}

{# The voivodeship of a TERYT code (2, 4 or 7 digits as published): its first two digits when they are one of the
   16 voivodeship codes; 0000000 and other text give null. #}
{% macro poc_rpwdl_voivodeship(column) -%}
    case
        when left({{ poc_rpwdl_text(column) }}, 2) in (
            '02', '04', '06', '08', '10', '12', '14', '16', '18', '20', '22', '24', '26', '28', '30', '32'
        ) then left({{ poc_rpwdl_text(column) }}, 2)
    end
{%- endmacro %}

{# The register snapshot of a row: the download time of its file (version = ingested, no server validator). #}
{% macro poc_rpwdl_snapshot() -%}
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date)
{%- endmacro %}

{# Newest Landing copy of every entity (a Bronze table may briefly hold two before the latest-only prune). #}
{% macro poc_rpwdl_newest(table) -%}
    select *
    from {{ source('rpwdl', table) }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
{%- endmacro %}

{% macro poc_source__rpwdl() %}
    {{ return({'block_model': 'poc_build_rpwdl', 'blocks': [['register', 'RPWDL register of healthcare providers and practices (counts)']]}) }}
{% endmacro %}

{# Areas: the two-digit voivodeship code (TERC WW) and PL for Poland. Static (dim_geography), no Silver read. #}
{% macro poc_geography__rpwdl() %}
rpwdl as (
    select
        'rpwdl' as source_system,
        left(g.terc_code, 2) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'rpwdl' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
