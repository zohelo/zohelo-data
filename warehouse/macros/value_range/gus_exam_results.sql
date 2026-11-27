{# GUS mean eighth-grade exam results (#449): a share of the maximum number of points, so 0 to 100 by definition
   (Dev trial 45901699841: 9 to 100 over all subjects and areas; totals: Polish 55-69, mathematics 40-63, English
   50-74). The KPIs have no metrics model yet, so the bounds wait ('models': []). #}
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
