{# GIOŚ groundwater Silver helpers (models/silver/gios_groundwater/). Bronze bronze.gios_groundwater_cells holds one
   row per workbook cell with its column_role (ingestion/to_bronze/gios_groundwater/load.py); these macros pick the
   newest version of each workbook and type the values. #}

{# The year of an entity (<kind>_<year>). #}
{% macro gios_groundwater_year(entity) %}cast(regexp_extract({{ entity }}, '([0-9]{4})$', 1) as integer){% endmacro %}

{# Newest version of every workbook (entity), by its Landing version; cells of the given kinds. #}
{% macro gios_groundwater_newest(kinds) %}
    select *
    from {{ source('gios_groundwater', 'gios_groundwater_cells') }}
    where workbook in ('{{ kinds | join("', '") }}')
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
{% endmacro %}

{# Monitoring network named by a link text: diagnostic, operational (also the source's typo "opercyjny"),
   diagnostic_operational (both, 2007 inorganic results), nitrate_study (the 2025 nitrate study), not_stated. #}
{% macro gios_groundwater_network(text) %}
    case
        when {{ text }} ilike '%azotanów%' then 'nitrate_study'
        when {{ text }} ilike '%diagnostyczny i operacyjny%' then 'diagnostic_operational'
        when {{ text }} ilike '%diagnostyczn%' then 'diagnostic'
        when {{ text }} ilike '%monitoring oper%' or {{ text }} ilike '%monitoring operc%' then 'operational'
        else 'not_stated'
    end
{% endmacro %}

{# A number as stored in a cell: numeric cells as stored (e.g. 7.0000000000000007E-2), text cells only when the
   whole text is a number with a decimal comma or point ("0,01"); anything else (markers) is NULL, never 0. #}
{% macro gios_groundwater_number(cell_type, text) %}
    case
        when {{ cell_type }} = 'n' then try_cast({{ text }} as double)
        when regexp_full_match(trim({{ text }}), '-?[0-9]+([.,][0-9]+)?')
            then try_cast(replace(trim({{ text }}), ',', '.') as double)
    end
{% endmacro %}

{# One row per workbook row (entity × row_number, row 1 = header excluded) with the descriptor columns. Excel serial
   dates of "Data poboru próbki" become dates; a text date (dd.mm.yyyy or yyyy-mm-dd) is parsed, other text kept in
   sample_date_text. #}
{% macro gios_groundwater_rows(kinds) %}
    {% set descriptors = ['lp', 'gwb_number_161', 'gwb_code_161', 'eu_point_id_161', 'gwb_number_172',
        'gwb_code_172', 'eu_point_id_172', 'gwb_number_174', 'gwb_code_174', 'eu_point_id_174', 'point_id_monitoring',
        'point_no_monbada', 'point_no_soh', 'point_no_cbdh', 'x_puwg1992', 'y_puwg1992', 'voivodeship', 'powiat',
        'gmina', 'locality', 'river_basin', 'rzgw', 'stratigraphy', 'aquifer_top_depth', 'screen_interval',
        'water_table', 'aquifer_type', 'point_type', 'land_use', 'study_year', 'sample_date'] %}
    select
        entity,
        any_value(workbook) as workbook,
        {{ gios_groundwater_year('entity') }} as year,
        any_value(link_text) as link_text,
        row_number,
        {% for d in descriptors %}
            {% if d in ('x_puwg1992', 'y_puwg1992', 'aquifer_top_depth') %}
                max(case when column_role = '{{ d }}' then {{ gios_groundwater_number('cell_type', 'value_text') }} end)
                    as {{ d }},
            {% elif d == 'study_year' %}
                max(case when column_role = 'study_year' then try_cast(trim(value_text) as integer) end) as study_year,
            {% elif d == 'sample_date' %}
                max(case when column_role = 'sample_date' then
                    case
                        when cell_type = 'n' then date '1899-12-30' + cast(floor(try_cast(value_text as double)) as integer)
                        when regexp_full_match(trim(value_text), '[0-9]{2}\.[0-9]{2}\.[0-9]{4}') then cast(strptime(trim(value_text), '%d.%m.%Y') as date)
                        when regexp_full_match(trim(value_text), '[0-9]{4}-[0-9]{2}-[0-9]{2}') then cast(trim(value_text) as date)
                    end
                end) as sample_date,
                max(case when column_role = 'sample_date' and cell_type <> 'n' then trim(value_text) end)
                    as sample_date_text,
            {% else %}
                max(case when column_role = '{{ d }}' then nullif(trim(value_text), '') end) as {{ d }},
            {% endif %}
        {% endfor %}
        max(case when column_role = 'monitoring_type' then nullif(trim(value_text), '') end) as monitoring_type,
        max(case when column_role = 'lab_analysis_no' then nullif(trim(value_text), '') end) as lab_analysis_no,
        max(case when column_role = 'final_class' then nullif(trim(value_text), '') end) as final_class,
        any_value(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256
    from ({{ gios_groundwater_newest(kinds) }}) as c
    where row_number > 1
    group by entity, row_number
{% endmacro %}

{# One row per indicator cell (entity × row_number × column_number) of the given kinds and years, with the header of
   its column: parameter as published, its name and the unit in square brackets. value only where the cell is a
   number (gios_groundwater_number); value_flag names what the cell holds: number, number_text (a number stored as
   text), below_limit ("<0,05": below the limit of quantification, limit_value = the number after "<"),
   not_determined ("n.o."), unreadable ("#######": the cell holds Excel's overflow mark as text); other text keeps
   the flag 'other' and fails the accepted_values test. Cells holding only spaces are no value and are left out. #}
{% macro gios_groundwater_results(kinds, year_from, year_to) %}
with cells as (
    select *
    from ({{ gios_groundwater_newest(kinds) }}) as c
    where {{ gios_groundwater_year('entity') }} between {{ year_from }} and {{ year_to }}
),

headers as (
    select
        entity,
        column_number,
        regexp_replace(trim(value_text), '\s+', ' ', 'g') as parameter
    from cells
    where row_number = 1
)

select
    c.entity,
    c.workbook,
    {{ gios_groundwater_year('c.entity') }} as year,
    c.row_number,
    c.column_number,
    h.parameter,
    trim(regexp_replace(h.parameter, '\s*\[[^\]]*\]\s*$', '')) as parameter_name,
    nullif(regexp_extract(h.parameter, '\[([^\]]*)\]\s*$', 1), '') as unit,
    c.value_text,
    {{ gios_groundwater_number('c.cell_type', 'c.value_text') }} as value,
    case
        when c.cell_type = 'n' and try_cast(c.value_text as double) is not null then 'number'
        when regexp_full_match(trim(c.value_text), '-?[0-9]+([.,][0-9]+)?') then 'number_text'
        when regexp_full_match(trim(c.value_text), '<\s*[0-9]+([.,][0-9]+)?') then 'below_limit'
        when lower(trim(c.value_text)) = 'n.o.' then 'not_determined'
        when regexp_full_match(trim(c.value_text), '#+') then 'unreadable'
        else 'other'
    end as value_flag,
    case
        when regexp_full_match(trim(c.value_text), '<\s*[0-9]+([.,][0-9]+)?')
            then try_cast(replace(regexp_replace(trim(c.value_text), '^<\s*', ''), ',', '.') as double)
    end as limit_value,
    c.source_version,
    c.raw_sha256
from cells as c
inner join headers as h on c.entity = h.entity and c.column_number = h.column_number
where c.row_number > 1 and c.column_role = 'parameter' and trim(c.value_text) <> ''
{% endmacro %}
