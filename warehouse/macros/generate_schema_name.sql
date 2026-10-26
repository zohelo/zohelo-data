{# Namespace = the folder's schema (silver, gold) on prod; the target's scratch schema
   on any other target, so trial runs never touch real namespaces. Target poc_trial is the
   exception: the poc folder goes to its scratch schema, everything else keeps its real
   namespace so the PoC reads real Silver (profiles.yml). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- elif target.name == 'poc_trial' and custom_schema_name is not none and custom_schema_name | trim != 'poc' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
