{# Table name = the model's alias on prod (the Gold v1 models drop their poc_ prefix); on any
   other target every model keeps its own name, because the scratch schema holds all
   namespaces at once and aliases could collide with Silver or other model names. #}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if target.name == 'prod' and custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
