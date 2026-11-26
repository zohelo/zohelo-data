{# KG PSP standard tables (#2970): incidents a year, Poland and voivodeships, 2016-2025. Measured in the Dev probes
   45746735646 (2016-2022 voivodeship workbooks): fires 2,885 (Podlaskie 2017) to 149,434 (Poland 2018); local hazards
   6,213 (Świętokrzyskie 2016) to 428,041 (Poland 2021); false alarms 297 (Podlaskie 2016) to 47,895 (Poland 2022). #}
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
