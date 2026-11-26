{# MRPiPS-03 (#2910): persons granted social assistance, PL 2019 = 1,205,506 (Dev trial 43293740538). #}
{% macro poc_value_range__mrpips_social_assistance() %}
    {{ return({
        'bounds': [
            ['social_assistance_recipients', '%', '0', '1e7'],
        ],
        'models': ['poc_metrics_social_assistance_recipients'],
    }) }}
{% endmacro %}
