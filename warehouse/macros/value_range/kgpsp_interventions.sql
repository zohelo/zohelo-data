{# KG PSP standard tables (#2970): incidents a year, Poland and voivodeships, 2016-2025. Measured in Dev trial
   45756529767 (Gold build stage): fires 2,441 to 153,520 (Poland 2019); local hazards 6,176 to 428,041 (Poland 2021);
   false alarms 297 (Podlaskie 2016) to 53,398 (Poland 2024). #}
{% macro poc_value_range__kgpsp_interventions() %}
    {{ return({
        'bounds': [
            ['fire_service_fires', '%', '500', '4e5'],
            ['fire_service_local_hazards', '%', '1000', '1.5e6'],
            ['fire_service_false_alarms', '%', '50', '2e5'],
        ],
        'models': [
            'poc_metrics_fire_service_fires', 'poc_metrics_fire_service_local_hazards',
            'poc_metrics_fire_service_false_alarms',
        ],
    }) }}
{% endmacro %}
