{# RAD-on / POL-on (#2990), counts on the download day; Poland is the largest area: 840 institutions of every status
   and 11,696 study programmes of every status in the register (Dev probe 43328619935). #}
{% macro poc_value_range__radon() %}
    {{ return({
        'bounds': [
            ['higher_education_institutions', '%', '0', '2000'],
            ['study_programmes', '%', '0', '30000'],
        ],
        'models': ['poc_metrics_higher_education_institutions', 'poc_metrics_study_programmes'],
    }) }}
{% endmacro %}
