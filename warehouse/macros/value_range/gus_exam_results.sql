{# GUS mean eighth-grade exam results (#449): a share of the maximum number of points, so 0 to 100 by definition
   (Dev trial on the final code, see the PR, for the measured range). The KPIs have no metrics model yet, so the
   bounds wait ('models': []). #}
{% macro poc_value_range__gus_exam_results() %}
    {{ return({
        'bounds': [
            ['exam8_polish', 'gus_exam_results:exam8:pct_max_points:polish', '0', '100'],
            ['exam8_mathematics', 'gus_exam_results:exam8:pct_max_points:mathematics', '0', '100'],
            ['exam8_english', 'gus_exam_results:exam8:pct_max_points:english', '0', '100'],
        ],
        'models': [],
    }) }}
{% endmacro %}
