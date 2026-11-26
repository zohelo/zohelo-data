{# IRENASTAT (GL-035, #1268): Total Renewable, grid connection All, every country, 2000 on; placeholder bounds until the
   PR's Dev trial measures the extremes. #}
{% macro poc_value_range__irena_stat() %}
    {{ return({
        'bounds': [
            ['renewable_capacity', 'irena_stat:elecstat:MW:capacity.total_renewable', '0', '5e6'],
            ['renewable_generation', 'irena_stat:elecstat:GWh:generation.total_renewable', '0', '1e7'],
        ],
        'models': ['poc_metrics_renewable_capacity', 'poc_metrics_renewable_generation'],
    }) }}
{% endmacro %}
