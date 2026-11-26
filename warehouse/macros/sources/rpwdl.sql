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

{# The voivodeship of a TERYT code (2, 4 or 7 digits as published; Bronze keeps only 2 for providers and
   practices): its first two digits when they are a voivodeship of silver.teryt_terc (newest catalogue state);
   0000000, the text NULL and other text give null. #}
{% macro poc_rpwdl_voivodeship(column) -%}
    case
        when left({{ poc_rpwdl_text(column) }}, 2) in (
            select v.terc_code from {{ ref('teryt_terc') }} as v where v.unit_level = 'wojewodztwo'
        ) then left({{ poc_rpwdl_text(column) }}, 2)
    end
{%- endmacro %}

{# The register snapshot day of a row (#3139): the download day (UTC) of its file, version = ingested (no server
   validator). A copy is written only when the bytes change, so an unchanged file keeps the day it was first read:
   the endpoint caches a generated ZIP, so the same bytes on a later day show the same register state, not a newer
   one, and end dates are compared with the day that state was read. #}
{% macro poc_rpwdl_snapshot() -%}
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date)
{%- endmacro %}

{# The snapshot year of a row: the year of its entity (<register>_<active|inactive>_<year>, the run's year in
   Europe/Warsaw; ingestion/to_landing/rpwdl/). Each year keeps its newest copy, so a past year holds its last state. #}
{% macro poc_rpwdl_year() -%}
    cast(regexp_extract(entity, '_([0-9]{4})$', 1) as integer)
{%- endmacro %}

{# active or inactive: the ledger file the row came from. #}
{% macro poc_rpwdl_ledger_state() -%}
    regexp_extract(entity, '_(active|inactive)_[0-9]{4}$', 1)
{%- endmacro %}

{# Newest Landing copy of every entity (a Bronze table may hold two copies of one year: before the latest-only prune,
   or when an older copy's bytes are also another year's newest). Entities without a year are the copies landed
   before #3139 (one rolling state, superseded by the year entities) and are left out. #}
{% macro poc_rpwdl_newest(table) -%}
    select *
    from {{ source('rpwdl', table) }}
    where regexp_matches(entity, '_(active|inactive)_[0-9]{4}$')
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
