{# Table name = the model's alias on prod (the PoC models drop their poc_ prefix inside the
   poc namespace); on any other target every model keeps its own name, because the scratch
   schema holds all namespaces at once and aliases would collide (gold.dim_geography and
   poc.dim_geography). #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if target.name == 'prod' and custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
