{# PSE Reports API v2 (#2870): daily means of 15-minute values for Poland from 2024-06-14. Bounds are the physically
   plausible range, not the observed one: RCE (PLN/MWh) can be negative on days of surplus generation (15-minute
   minimum -62.06 on 2025-06-15, Dev probe 45790863668); demand, wind and PV are mean powers of the national system in
   MW, wide margins around their daily means. The Dev trial of #2870 checks every build row against them. #}
{% macro poc_value_range__pse_reports() %}
    {{ return({
        'bounds': [
            ['electricity_price_rce', '%', '-500', '3000'],
            ['electricity_demand', '%', '5000', '40000'],
            ['wind_generation', '%', '0', '15000'],
            ['pv_generation', '%', '0', '25000'],
        ],
        'models': [
            'poc_metrics_electricity_price_rce', 'poc_metrics_electricity_demand', 'poc_metrics_wind_generation',
            'poc_metrics_pv_generation',
        ],
    }) }}
{% endmacro %}
