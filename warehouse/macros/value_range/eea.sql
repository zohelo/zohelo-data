{# EEA GovReg (#2424): published in Gg (kt) CO2e, metrics in Mt CO2e (D33, ÷ 1000); highest member DE 1,253,128 kt
   = 1,253.1 Mt (total excl. LULUCF, 1990-2024, Dev trial 43282453427). Documentation only until metrics_ghg_emissions
   is in `models` (moved from tests/metrics_value_range.sql). #}
{% macro poc_value_range__eea() %}
    {{ return({
        'bounds': [
            ['ghg_emissions', 'eea:govreg_ghg_inventory:gg_co2e:Sectors/Totals_excl', '0', '2e3'],
        ],
        'models': [],
    }) }}
{% endmacro %}
