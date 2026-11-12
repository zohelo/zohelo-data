{# Silver model of one UNCTAD Data Hub table (entity of bronze.unctad_rows): one row per row of the newest Landing
   copy. Every Data Hub CSV has the same shape: a time field first (Year, Period or Quarter, with an optional
   "<time> Label"), dimension code/label pairs ("Economy" and "Economy Label", "Flow" and "Flow Label"…) and
   measures, each a value column with "<measure> Footnote" and "<measure> Missing value" next to it. The macro
   finds them by name, so a table that adds a measure or a dimension keeps loading: economy and economy_label are
   columns (null for tables without economies), the other dimensions are JSON maps of code and label by dimension
   name, the measures JSON maps of value (double), footnote and missing-value reason by measure name. #}
{% macro unctad_table(entity) %}
with newest as (
    select
        string_split(field_names, '|') as f,
        string_split(field_values, '|') as v,
        source_row_number,
        raw_sha256,
        source_version,
        ingested_at
    from {{ source('unctad', 'unctad_rows') }}
    where entity = '{{ entity }}'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

shaped as (
    select
        *,
        list_filter(f, lambda x: list_contains(f, x || ' Label') and x <> f[1] and x <> 'Economy') as dims,  -- noqa: RF02
        list_filter(f, lambda x: list_contains(f, x || ' Footnote')) as measures  -- noqa: RF02
    from newest
)

select
    '{{ entity }}' as entity,
    f[1] as period_field,
    v[1] as period,
    nullif(v[list_position(f, f[1] || ' Label')], '') as period_label,
    nullif(v[list_position(f, 'Economy')], '') as economy,
    nullif(v[list_position(f, 'Economy Label')], '') as economy_label,
    cast(to_json(map(dims, list_transform(dims, lambda x: v[list_position(f, x)]))) as varchar) as dimensions,  -- noqa: RF02
    cast(
        to_json(map(dims, list_transform(dims, lambda x: v[list_position(f, x || ' Label')])))  -- noqa: RF02
        as varchar
    ) as dimension_labels,
    cast(
        to_json(
            map(
                measures,
                list_transform(measures, lambda x: try_cast(nullif(v[list_position(f, x)], '') as double))  -- noqa: RF02
            )
        ) as varchar
    ) as measure_values,
    cast(
        to_json(map(measures, list_transform(measures, lambda x: nullif(v[list_position(f, x || ' Footnote')], ''))))  -- noqa: RF02
        as varchar
    ) as measure_footnotes,
    cast(
        to_json(
            map(
                measures,
                list_transform(measures, lambda x: nullif(v[list_position(f, x || ' Missing value')], ''))  -- noqa: RF02
            )
        ) as varchar
    ) as measure_missing,
    source_row_number,
    raw_sha256,
    source_version,
    ingested_at
from shaped
{% endmacro %}
