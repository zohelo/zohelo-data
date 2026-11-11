{# The topics settings (warehouse/warehouse.yaml `topics:`), which warehouse/build.py passes to every build as the
   dbt var `topics`: topics_setting('weights.news') → 0.5. There are no defaults in code: a run without the var
   fails (dbt parse, which executes nothing, gets none). topics_as_of() is the run day in Warsaw, or the var
   topics_as_of (YYYY-MM-DD) for a replay or a trial. #}
{% macro topics_setting(path) %}
    {%- set value = var('topics', none) -%}
    {%- if value is none -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("dbt var 'topics' missing: warehouse/build.py passes warehouse.yaml topics") }}
        {%- endif -%}
        {{ return(none) }}
    {%- endif -%}
    {#- a plain set inside a for loop does not leave the loop in Jinja: walk the path with a namespace -#}
    {%- set ns = namespace(value=value) -%}
    {%- for part in path.split('.') -%}
        {%- set ns.value = ns.value[part] -%}
    {%- endfor -%}
    {{ return(ns.value) }}
{% endmacro %}

{% macro topics_as_of() %}
    {%- if var('topics_as_of', none) -%}
        date '{{ var("topics_as_of") }}'
    {%- else -%}
        cast(timezone('Europe/Warsaw', now()) as date)
    {%- endif -%}
{% endmacro %}
