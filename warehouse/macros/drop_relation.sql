{# Iceberg tables in the R2 Data Catalog reject DROP … CASCADE ("not supported for Iceberg
   tables"), which dbt-duckdb issues when a model replaces its previous build. Drop without
   CASCADE; nothing in the catalog depends on a table through views. #}
{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}
