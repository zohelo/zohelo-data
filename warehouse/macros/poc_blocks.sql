{# Gold macros every source shares: the Polish NUTS and makroregion lists of the geography dimension, the
   cross-source concepts, the union of the Gold blocks and its helpers. A source's own blocks and lists live in
   warehouse/macros/sources/<source_system>.sql (decision D34, docs/source-registration.md). #}

{# NUTS 2016/2021 regions of Poland and the TERC voivodeship they equal; PL9 is Mazowieckie
   itself (NUTS 1) and its two NUTS 2 regions are children of the voivodeship. #}
{% macro poc_nuts_pl() %}
    select * from (
        values
        ('PL2', 'Makroregion południowy', null, 'makroregion'),
        ('PL4', 'Makroregion północno-zachodni', null, 'makroregion'),
        ('PL5', 'Makroregion południowo-zachodni', null, 'makroregion'),
        ('PL6', 'Makroregion północny', null, 'makroregion'),
        ('PL7', 'Makroregion centralny', null, 'makroregion'),
        ('PL8', 'Makroregion wschodni', null, 'makroregion'),
        ('PL9', 'Makroregion województwo mazowieckie', '14', 'wojewodztwo'),
        ('PL21', 'Małopolskie', '12', 'wojewodztwo'),
        ('PL22', 'Śląskie', '24', 'wojewodztwo'),
        ('PL41', 'Wielkopolskie', '30', 'wojewodztwo'),
        ('PL42', 'Zachodniopomorskie', '32', 'wojewodztwo'),
        ('PL43', 'Lubuskie', '08', 'wojewodztwo'),
        ('PL51', 'Dolnośląskie', '02', 'wojewodztwo'),
        ('PL52', 'Opolskie', '16', 'wojewodztwo'),
        ('PL61', 'Kujawsko-pomorskie', '04', 'wojewodztwo'),
        ('PL62', 'Warmińsko-mazurskie', '28', 'wojewodztwo'),
        ('PL63', 'Pomorskie', '22', 'wojewodztwo'),
        ('PL71', 'Łódzkie', '10', 'wojewodztwo'),
        ('PL72', 'Świętokrzyskie', '26', 'wojewodztwo'),
        ('PL81', 'Lubelskie', '06', 'wojewodztwo'),
        ('PL82', 'Podkarpackie', '18', 'wojewodztwo'),
        ('PL84', 'Podlaskie', '20', 'wojewodztwo'),
        ('PL91', 'Warszawski stołeczny', '14', 'region'),
        ('PL92', 'Mazowiecki regionalny', '14', 'region')
    ) as t (nuts_code, name_pl, woj_code, level_name)
{% endmacro %}

{# Which makroregion (NUTS 1) a voivodeship belongs to. #}
{% macro poc_woj_makroregion() %}
    select * from (
        values
        ('12', 'PL2'), ('24', 'PL2'), ('30', 'PL4'), ('32', 'PL4'), ('08', 'PL4'), ('02', 'PL5'), ('16', 'PL5'),
        ('04', 'PL6'), ('28', 'PL6'), ('22', 'PL6'), ('10', 'PL7'), ('26', 'PL7'), ('06', 'PL8'), ('18', 'PL8'),
        ('20', 'PL8')
    ) as t (woj_code, nuts1_code)
{% endmacro %}

{# Cross-source concepts: which measures mean the same thing. measure_key patterns are LIKE
   patterns against poc dim_measure.measure_key. #}
{% macro poc_concepts() %}
    {# KPI definitions are seeds (warehouse/seeds/kpi.csv, kpi_measure.csv, unit.csv). #}
    select
        k.kpi_key as concept_key,
        k.name_en,
        k.name_pl,
        u.name_en as unit,
        m.source_system,
        m.measure_key_pattern,
        m.definition
    from {{ ref('kpi_measure') }} as m
    inner join {{ ref('kpi') }} as k on m.kpi_key = k.kpi_key
    inner join {{ ref('unit') }} as u on k.unit_key = u.unit_key
    order by k.kpi_key, m.priority
{% endmacro %}

{# All Gold observations in the common shape: the union of the Gold blocks, each row stamped with its block model
   in a trailing column `gold_block` (the key of the per-block incremental models, #3204 PR B). A block without a
   table is left out (zero rows) only when the changed-only plan declared it waiting: warehouse/changed.py lists
   it under `empty_blocks` (its source tables do not exist yet and neither does its own table; a waiting
   block that has a table is never listed, the build stops instead) and warehouse/build.py passes that list
   as the dbt var `waiting_blocks`. Any other missing block (a partial catalog listing, a run killed
   between renames, an explicit-filter build) is a compiler error, so a source never drops out of Gold
   silently; a block built in this run that fails makes dbt skip every model reading it. At parse time
   every block is ref'd, so dbt records each dependency.
   `incremental`: the caller's is_incremental(). Then only the blocks of the dbt var `changed_blocks` are read
   (changed.py, passed by build.py); with none of them, an empty relation of the same columns. #}
{% macro poc_observations(incremental=false) %}
    {% set only = var('changed_blocks', none) if incremental else none %}
    {% set ns = namespace(first=true, empty=none, empty_model=none) %}
    {% for entry in poc_block_models().split() %}
        {% set model = entry.split(':')[0] %}
        {% set relation = ref(model) %}
        {% if not execute or not (poc_block_waiting(entry, relation) | trim) %}
            {% if only is none or model in only %}
                {% if not ns.first %}union all {% endif %}select *, '{{ model }}' as gold_block from {{ relation }}
                {% set ns.first = false %}
            {% elif ns.empty is none %}
                {% set ns.empty = relation %}
                {% set ns.empty_model = model %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if ns.first and ns.empty is not none %}
        select *, '{{ ns.empty_model }}' as gold_block from {{ ns.empty }} where false
    {% elif ns.first and context is not defined %}
        {# sqlfluff lints with plain Jinja, which finds no registered block (macros/source_registration.sql) #}
        select null as gold_block where false
    {% elif ns.first %}
        {% do exceptions.raise_compiler_error('poc_observations: no Gold block table exists') %}
    {% endif %}
{% endmacro %}

{# 'waiting' (rendered) when the block of `entry` (model:source_system) has no table and the plan declared
   it waiting; '' when its table exists; a compiler error when it has no table and was not declared. #}
{% macro poc_block_waiting(entry, relation) %}
    {%- set model = entry.split(':')[0] -%}
    {%- if load_relation(relation) is none -%}
        {%- if model in var('waiting_blocks', []) -%}
            {%- do log('Gold block ' ~ relation ~ ' is waiting (source tables do not exist yet): zero rows', info=true) -%}
            waiting
        {%- else -%}
            {%- do exceptions.raise_compiler_error(
                'Gold block ' ~ model ~ ' (' ~ relation ~ ') does not exist and changed.py did not declare it '
                ~ 'waiting (var waiting_blocks): build it first, e.g. select ' ~ model ~ '+'
            ) -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{# A Silver table the shared geography map reads only for a source's own codes, as a FROM item: the table
   when it exists, else an empty relation with `columns` ('name:type, …') when the changed-only plan declared
   the model waiting (its source has no Bronze yet; warehouse/build.py passes plan["waiting"] as the dbt var
   `waiting_models`). So a source before its first load adds no rows to the map instead of holding back the
   map and every block reading it (#2903). Any other missing table is a compiler error. The reading model lists
   the same names in config meta `optional_parents`: warehouse/changed.py does not let it wait for them, and
   ops/repo_check.py checks both lists agree. At parse time the model is ref'd, so dbt records the dependency. #}
{% macro poc_optional_ref(model, columns) %}
    {%- set relation = ref(model) -%}
    {%- if not execute or load_relation(relation) is not none -%}
        {{ relation }}
    {%- elif model in var('waiting_models', []) -%}
        {%- do log('Optional input ' ~ relation ~ ' is waiting (source tables do not exist yet): zero rows', info=true) -%}
        {%- set cols = [] -%}
        {%- for column in columns.split(',') -%}
            {%- do cols.append('cast(null as ' ~ column.split(':')[1] | trim ~ ') as ' ~ column.split(':')[0] | trim) -%}
        {%- endfor -%}
        (select {{ cols | join(', ') }} where false)
    {%- else -%}
        {%- do exceptions.raise_compiler_error(
            'Optional input ' ~ model ~ ' (' ~ relation ~ ') does not exist and changed.py did not declare it '
            ~ 'waiting (var waiting_models): build it first, e.g. select ' ~ model ~ '+'
        ) -%}
    {%- endif -%}
{% endmacro %}

{# The source_system of each waiting block (poc_block_waiting), quoted and comma-separated ('' at parse
   time). #}
{% macro poc_absent_blocks() %}
    {%- set absent = [] -%}
    {%- for entry in poc_block_models().split() -%}
        {%- if execute and poc_block_waiting(entry, ref(entry.split(':')[0])) | trim -%}
            {%- do absent.append("'" ~ entry.split(':')[1] ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{- absent | join(', ') -}}
{% endmacro %}

{# series_key: measure × geography × breakdown codes, readable; an unmatched area keeps its source code so
   two unmatched units never share a series. #}
{% macro poc_series_key() %}
    measure_key || '|' || geography_key
    || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
    || '|' || breakdown_json
{% endmacro %}
