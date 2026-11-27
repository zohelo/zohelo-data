{# WHO data.who.int (#2408): years; highest 87.37 (all areas, 2000-2021, Dev trial 43159213097). Documentation only
   until metrics_life_expectancy is in `models`. Moved from tests/metrics_value_range.sql. #}
{% macro poc_value_range__who() %}
    {{ return({
        'bounds': [
            ['life_expectancy', '%', '0', '130'],
        ],
        'models': [],
    }) }}
{% endmacro %}
