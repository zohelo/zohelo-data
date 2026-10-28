{# Seeds are small code lists: every build drops and recreates the seed table instead of
   truncating it, so a seed that gains or loses a column (kpi_measure.unit_key, #2148) loads
   without a --full-refresh. dbt-duckdb's fast seed load is COPY … (HEADER TRUE), which fails
   when the CSV header and the existing table's columns differ. #}
{% macro duckdb__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model, agate_table)) }}
{% endmacro %}
