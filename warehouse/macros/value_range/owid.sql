{# Our World in Data (CD-031, #2982): consumption-based CO2 in tonnes per person (Global Carbon Budget); the V-Dem
   electoral democracy index, 0 to 1 by definition; Maddison GDP per capita in international-$ at 2011 prices (bounds
   as checked in the Dev trial of #2982 on every Gold row). CO2 starts above 0: an exact 0 is a missing value for a
   populated country and poc_build_owid does not build it. poc_metrics_gdp_per_capita is shared with other sources. #}
{% macro poc_value_range__owid() %}
    {{ return({
        'bounds': [
            ['co2_consumption_per_capita', '%', '1e-6', '100'],
            ['electoral_democracy_index', '%', '0', '1'],
            ['gdp_per_capita', '%', '100', '300000'],
        ],
        'models': [
            'poc_metrics_co2_consumption_per_capita',
            'poc_metrics_electoral_democracy_index',
            'poc_metrics_gdp_per_capita',
        ],
    }) }}
{% endmacro %}
