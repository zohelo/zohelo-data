{# IRENASTAT (GL-035, #1268): Total Renewable, grid connection All, every country, 2000 on (Dev trial 45868633684, the
   metrics filter applied to build_irena_stat): capacity 0 MW (published zeros, e.g. Grenada 2007) to 2,258,015.77 MW
   (China 2025; Poland 2025 38,226.54); generation 0 GWh (e.g. Benin 2006) to 3,340,355.45 GWh (China 2024; Poland 2024
   52,674.16). #}
{% macro poc_value_range__irena_stat() %}
    {{ return({
        'bounds': [
            ['renewable_capacity', 'irena_stat:elecstat:MW:capacity.total_renewable', '0', '5e6'],
            ['renewable_generation', 'irena_stat:elecstat:GWh:generation.total_renewable', '0', '1e7'],
        ],
        'models': ['poc_metrics_renewable_capacity', 'poc_metrics_renewable_generation'],
    }) }}
{% endmacro %}
