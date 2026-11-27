-- Gold metrics: every value lies within a plausible range for its KPI, source and measure. The bounds
-- catch unit and scale errors (a value ×1000, a percentage stored as a share), not small mistakes.
-- Change a bound only with the publisher's own value as evidence, cited in a comment next to it.
-- A bound covers the measures of the KPI × source whose key matches its pattern ('%' = all of them);
-- measures in different units (million EUR and million national currency) get a row each, and the
-- patterns of one KPI × source never overlap. A measure without a bound fails, so a new source or
-- measure gets one.
-- The KPIs in positive_concepts are counts and levels that cannot be 0 or negative (#2807): for them the lower
-- bound is exclusive, so a 0 stored in place of a missing value fails. A series in published_zeros keeps a 0 that its
-- publisher prints, with the evidence next to it.
{#- Where the bounds and the metrics models are declared (docs/source-registration.md, "Value ranges"): a source's
    bounds and the metrics models of its KPIs are in warehouse/macros/value_range/<source_system>.sql,
    poc_value_range__<source_system>() returning {'bounds': [[concept_key, measure_key_pattern, value_min, value_max],
    ...], 'models': [<poc_metrics_ model>, ...]}, found by name like the Gold registrations of D34. This file holds no
    source's bounds or models (the last ones written here moved to their sources' files on 27 November). The union
    reads every model once, in the order the sources (by name) list them. #}
{%- set positive_concepts = [
    'population',
    'population_projection',
    'gdp_current_prices',
    'gdp_per_capita',
    'fx_rate',
] -%}
{#- [concept_key, source_system, measure_key, geography_key]. World Bank WDI NY.GDP.MKTP.CN, Serbia 1990-1992: the bulk
    CSV prints "0" and the API answers value 0 with "decimal": 0 (Dev run 45896947355, archive 20261116T010849Z);
    1989 and 1993-1994 are empty and 1995 is 63,443,500,000. Most likely the hyperinflation-era GDP restated in
    today's dinars rounds to 0. #}
{%- set published_zeros = [
    ['gdp_current_prices', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CN', 'RS'],
] -%}
{%- set registered = namespace(bounds=[], models=[], keys=[]) -%}
{%- for source in (poc_registered('poc_value_range__') if context is defined else []) -%}
    {%- set declared = context['poc_value_range__' ~ source]() -%}
    {%- if declared is not mapping or declared.keys() | list | sort != ['bounds', 'models'] -%}
        {%- do exceptions.raise_compiler_error(
            'poc_value_range__' ~ source ~ ': return exactly {"bounds": [...], "models": [...]}'
        ) -%}
    {%- endif -%}
    {%- for bound in declared.bounds -%}
        {%- set text = bound | map('string') | list -%}
        {%- if bound | length != 4
            or not modules.re.fullmatch('[a-z0-9_]+', text[0])
            or not modules.re.fullmatch('-?[0-9]+(\\.[0-9]+)?(e-?[0-9]+)?', text[2])
            or not modules.re.fullmatch('-?[0-9]+(\\.[0-9]+)?(e-?[0-9]+)?', text[3]) -%}
            {%- do exceptions.raise_compiler_error(
                'poc_value_range__' ~ source ~ ': a bound is [concept_key, measure_key_pattern, value_min, value_max]'
                ~ ' with numbers as text, e.g. [\'population\', \'%\', \'0\', \'5e7\'], not ' ~ bound
            ) -%}
        {%- endif -%}
        {%- set key = text[0] ~ '|' ~ source ~ '|' ~ text[1] -%}
        {%- if key in registered.keys -%}
            {%- do exceptions.raise_compiler_error('poc_value_range__' ~ source ~ ': bound ' ~ key ~ ' is declared twice') -%}
        {%- endif -%}
        {%- do registered.keys.append(key) -%}
        {%- do registered.bounds.append([text[0], source, text[1], text[2], text[3]]) -%}
    {%- endfor -%}
    {%- for model in declared.models -%}
        {%- if not (model is string and model.startswith('poc_metrics_')) -%}
            {%- do exceptions.raise_compiler_error(
                'poc_value_range__' ~ source ~ ': models lists poc_metrics_ models, not ' ~ model
            ) -%}
        {%- endif -%}
        {%- if model not in registered.models -%}
            {%- do registered.models.append(model) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor %}
with bounds (concept_key, source_system, measure_key_pattern, value_min, value_max) as (
    values
    {%- for bound in registered.bounds %}
    ('{{ bound[0] }}', '{{ bound[1] }}', '{{ bound[2] | replace("'", "''") }}', {{ bound[3] }}, {{ bound[4] }})
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
),

metrics as (
    {%- for model in registered.models %}
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref(model) }}
        {%- if not loop.last %}
    union all
        {%- endif %}
    {%- endfor %}
)

select
    m.concept_key,
    m.source_system,
    m.measure_key,
    m.geography_key,
    m.date_key,
    m.value,
    b.value_min,
    b.value_max
from metrics as m
left join bounds as b
    on
        m.concept_key = b.concept_key
        and m.source_system = b.source_system
        and m.measure_key like b.measure_key_pattern
where
    m.value < b.value_min
    or m.value > b.value_max
    or b.value_min is null
    or (
        m.value <= 0
        and m.concept_key in ({{ "'" ~ positive_concepts | join("', '") ~ "'" }})
        and not (m.value = 0 and concat_ws('|', m.concept_key, m.source_system, m.measure_key, m.geography_key) in (
            {%- for z in published_zeros %}'{{ z | join('|') }}'{{ ', ' if not loop.last }}{% endfor -%}
        ))
    )
