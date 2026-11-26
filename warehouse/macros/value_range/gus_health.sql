{# GUS Zdrowie i ochrona zdrowia (#2451), stocks on 31 December; Poland is the largest area: doctors 141,452 (2023),
   nurses 219,881 (2024), beds in general hospitals 188,820 (2012) (Dev trial on the final code, see the PR). The
   KPIs have no metrics model yet, so the bounds wait ('models': []). #}
{% macro poc_value_range__gus_health() %}
    {{ return({
        'bounds': [
            ['doctors_working', 'gus_health:health_care_resources:persons:doctors', '0', '3e5'],
            ['nurses_working', 'gus_health:health_care_resources:persons:nurses', '0', '5e5'],
            ['hospital_beds', 'gus_health:health_care_resources:beds:beds', '0', '5e5'],
        ],
        'models': [],
    }) }}
{% endmacro %}
