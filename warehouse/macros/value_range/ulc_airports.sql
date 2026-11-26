{# ULC airport traffic (#2967), quarterly counts as published; highest total (all airports) Q3 2025: 21,241,797
   passengers, 140,633 operations (Dev trial 43831892099). Documentation only (no metrics model for these KPIs yet). #}
{% macro poc_value_range__ulc_airports() %}
    {{ return({
        'bounds': [
            ['air_passengers', '%', '0', '5e7'],
            ['air_passenger_operations', '%', '0', '1e6'],
        ],
        'models': [],
    }) }}
{% endmacro %}
