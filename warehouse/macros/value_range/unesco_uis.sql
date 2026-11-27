{# UNESCO UIS (#2411), Dev trial 43159213097 (moved from tests/metrics_value_range.sql). gerd_gdp: % of GDP;
   highest 6.35 (all areas). tertiary_attainment: % of population 25+; capped at 100 (all areas max 73.91).
   Documentation only until metrics_gerd_gdp and metrics_tertiary_attainment are in `models`. #}
{% macro poc_value_range__unesco_uis() %}
    {{ return({
        'bounds': [
            ['gerd_gdp', '%', '0', '9.5'],
            ['tertiary_attainment', '%', '0', '100'],
        ],
        'models': [],
    }) }}
{% endmacro %}
