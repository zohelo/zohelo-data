{# OpenAlex works with an author affiliated with an institution in Poland per publication year (#2996): 87,917 in 2024
   (Dev probe 45813299261); a year above 300,000 is a selection or counting error. #}
{% macro poc_value_range__openalex() %}
    {{ return({
        'bounds': [
            ['scholarly_works_pl_institution', '%', '0', '3e5'],
        ],
        'models': ['poc_metrics_scholarly_works_pl_institution'],
    }) }}
{% endmacro %}
