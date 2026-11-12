{# Gold PoC metrics view of one concept: every measure of the concept at its native grain,
   total series only, one row per source × area × period, with the geography and calendar
   attributes a chart needs. Rows of an unmatched area (`_X`) are left out: two unmatched units
   would share one row key (#2307); build_check and the metrics_geography_unmatched warning count
   them. Materialised as a table (the catalog holds no views).

   Units (decision D33): the facts keep the value and unit as the source publishes them; this view is
   the one place that normalises a KPI. `value` is in the series' target unit `unit` (the measure's
   kpi_measure.csv unit_key, else the KPI's unit, the same rule as gold.publish), converted by pure
   scale: published value × factor(source unit) / factor(target unit), unit.csv, rounded to the
   published decimals shifted by the factor (poc_rescale). The source unit is the unit_map.csv row of
   the measure's unit code. `value_published` and `unit_published` keep what the source published.
   No conversion when the code has no unit_map row (reported by test metrics_unit_unmapped) or the
   two units have different bases (test metrics_unit_base fails): currencies, index bases and price
   bases are never converted. #}
{% macro poc_concept_metrics(concept_key) %}
with members as (
    select cm.measure_key, cm.definition, cm.source_system
    from {{ ref('poc_concept_measures') }} as cm
    where cm.concept_key = '{{ concept_key }}'
),
facts as (
    select * from {{ ref('poc_fact_annual') }}
    union all select * from {{ ref('poc_fact_quarterly') }}
    union all select * from {{ ref('poc_fact_monthly') }}
    union all select * from {{ ref('poc_fact_daily') }}
),
units as ({{ poc_measure_units(concept_key) }})

select
    '{{ concept_key }}' as concept_key,
    f.source_system,
    f.measure_key,
    m.measure_name,
    mb.definition,
    f.series_key,
    s.breakdown_json,
    f.geography_key,
    g.name as geography_name,
    g.level_name as geography_level,
    g.country_key,
    g.wojewodztwo_key,
    f.date_key,
    c.year,
    c.month_label,
    f.frequency,
    case
        when un.ratio is null or un.ratio = 1 then f.value
        else {{ poc_rescale('f.value', 'un.ratio') }}
    end as value,
    un.target_unit_key as unit,
    f.value as value_published,
    m.unit_key as unit_published,
    f.observation_status_key
from facts as f
inner join members as mb on mb.measure_key = f.measure_key
inner join {{ ref('poc_dim_series') }} as s on s.series_key = f.series_key
inner join {{ ref('poc_dim_measure') }} as m on m.measure_key = f.measure_key
left join {{ ref('poc_dim_geography') }} as g on g.geography_key = f.geography_key
left join {{ ref('poc_dim_calendar') }} as c on c.date_key = f.date_key
left join units as un on un.measure_key = f.measure_key
where s.is_total and f.geography_key <> '_X'
{% endmacro %}

{# Source and target unit of every measure of one concept (or of every concept when concept_key is
   none): the target is the kpi_measure.csv unit_key of the best-priority row whose pattern matches,
   else the KPI's unit (as gold.publish); the source is the unit_map.csv row of the measure's unit code
   with the longest matching measure_key_pattern (empty = every measure). ratio is set only when both
   units are known and share a base. #}
{% macro poc_measure_units(concept_key=none) %}
    select distinct
        cm.concept_key,
        cm.measure_key,
        cm.source_system,
        dm.unit_key as unit_code,
        tgt.unit_key as target_unit_key,
        src.unit_key as source_unit_key,
        su.base_unit_key as source_base_unit_key,
        tu.base_unit_key as target_base_unit_key,
        case
            when su.base_unit_key = tu.base_unit_key then cast(su.factor as double) / cast(tu.factor as double)
        end as ratio
    from {{ ref('poc_concept_measures') }} as cm
    inner join {{ ref('poc_dim_measure') }} as dm on dm.measure_key = cm.measure_key
    left join lateral (
        select arg_min(coalesce(km.unit_key, k.unit_key), km.priority) as unit_key
        from {{ ref('kpi_measure') }} as km
        inner join {{ ref('kpi') }} as k on k.kpi_key = km.kpi_key
        where
            km.kpi_key = cm.concept_key
            and km.source_system = cm.source_system
            and cm.measure_key like km.measure_key_pattern
    ) as tgt on true
    left join lateral (
        select arg_max(um.unit_key, length(coalesce(um.measure_key_pattern, ''))) as unit_key
        from {{ ref('unit_map') }} as um
        where
            um.source_system = cm.source_system
            and um.unit_code = dm.unit_key
            and cm.measure_key like coalesce(nullif(um.measure_key_pattern, ''), '%')
    ) as src on true
    left join {{ ref('unit') }} as su on su.unit_key = src.unit_key
    left join {{ ref('unit') }} as tu on tu.unit_key = tgt.unit_key
    {% if concept_key is not none %}where cm.concept_key = '{{ concept_key }}'{% endif %}
{% endmacro %}

{# A value converted by pure scale (D33): value × ratio, rounded to the decimals the value was published
   with (its shortest decimal text, exponent included) shifted by the ratio's power of ten, so 12.3
   thousand → 12300 and 1234567 USD → 1.234567 million USD, without float artefacts. #}
{% macro poc_rescale(value, ratio) %}
    round(
        cast({{ value }} as double) * {{ ratio }},
        cast(
            greatest(
                length(rtrim(regexp_extract(cast(cast({{ value }} as double) as varchar), '[.]([0-9]*)', 1), '0'))
                - coalesce(try_cast(regexp_extract(cast(cast({{ value }} as double) as varchar), '[eE]([-+]?[0-9]+)', 1) as integer), 0),
                0
            )
            + case
                when abs(-log10({{ ratio }}) - round(-log10({{ ratio }}))) < 1e-9 then round(-log10({{ ratio }}))
                else ceil(-log10({{ ratio }}))
            end
            as integer
        )
    )
{% endmacro %}
