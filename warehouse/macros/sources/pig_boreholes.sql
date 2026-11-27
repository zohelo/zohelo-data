{# pig_boreholes (register PL-ENV-014): Gold registration, decision D34 (docs/source-registration.md). Block 'boreholes'
   (models/gold/poc_build_pig_boreholes.sql): snapshot counts of the CBDG borehole register for Poland, the
   voivodeships and the powiats. The build places each borehole's point in a PRG gmina boundary (silver.prg_units), as
   build_pig_midas does, and takes the powiat and voivodeship from the gmina's TERYT code. Source codes: 'PL', the
   voivodeship's 2-digit and the powiat's 4-digit TERYT code (static: read from geo only). #}
{% macro poc_source__pig_boreholes() %}
    {{ return({'block_model': 'poc_build_pig_boreholes', 'blocks': [
        ['boreholes', 'PIG-PIB CBDG boreholes'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__pig_boreholes() %}
pig_boreholes as (
    select
        'pig_boreholes' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name in ('wojewodztwo', 'powiat') and g.terc_code is not null
    union all
    select
        'pig_boreholes' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# Text of a CBDG property: trimmed, empty text and the producer's literal 'null' as NULL. #}
{% macro poc_pig_boreholes_text(column) -%}
    nullif(nullif(trim({{ column }}), ''), 'null')
{%- endmacro %}

{# A number as DuckDB wrote the JSON number ('14.0', '-0.5'); text that is not a number fails the build. #}
{% macro poc_pig_boreholes_number(column, model) -%}
    case
        when {{ poc_pig_boreholes_text(column) }} is null then null
        when try_cast(trim({{ column }}) as double) is not null then cast(trim({{ column }}) as double)
        else error('{{ model }}: not a number in {{ column }}: ' || {{ column }})
    end
{%- endmacro %}

{# A whole number written as an integer or with a zero fraction ('3489666.0'); anything else fails the build. #}
{% macro poc_pig_boreholes_integer(column, model) -%}
    case
        when {{ poc_pig_boreholes_text(column) }} is null then null
        when regexp_full_match(trim({{ column }}), '[0-9]+(\.0+)?')
            then cast(cast(trim({{ column }}) as decimal(18, 3)) as bigint)
        else error('{{ model }}: not a whole number in {{ column }}: ' || {{ column }})
    end
{%- endmacro %}
