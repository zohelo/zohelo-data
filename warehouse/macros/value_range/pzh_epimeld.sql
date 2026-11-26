{# NIZP PZH-PIB EPIMELD (#443), incidence per 100,000 population as published, Poland and voivodeships 2005-2025;
   highest values in Dev trial 45909237330: chickenpox 907.9 (Lubuskie 2014), pertussis 153.18
   (Łódzkie 2024), measles 10.2 (Mazowieckie 2019). Documentation only (no metrics model for these KPIs yet). #}
{% macro poc_value_range__pzh_epimeld() %}
    {{ return({
        'bounds': [
            ['chickenpox_incidence', 'pzh_epimeld:annual:per_100k:%', '0', '3000'],
            ['pertussis_incidence', 'pzh_epimeld:annual:per_100k:%', '0', '1000'],
            ['measles_incidence', 'pzh_epimeld:annual:per_100k:%', '0', '500'],
        ],
        'models': [],
    }) }}
{% endmacro %}
