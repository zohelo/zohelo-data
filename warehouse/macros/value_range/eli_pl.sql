{# ELI Poland (#483): acts published per month in Dziennik Ustaw, Poland, all types; only months with an act have a
   row, so the lowest value is 1; largest month 435 acts (April 2004; Dev trial 43856642960, every month 1918-2026).
   A month above 5,000 acts is a counting error. #}
{% macro poc_value_range__eli_pl() %}
    {{ return({
        'bounds': [
            ['legal_acts_published', '%', '1', '5000'],
        ],
        'models': ['poc_metrics_legal_acts_published'],
    }) }}
{% endmacro %}
