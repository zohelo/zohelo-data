{# FAOSTAT (#2402), Dev trial 43159213097 (moved from tests/metrics_value_range.sql). ghg_emissions: published
   in kt CO2eq, metrics in Mt CO2e since D33 (÷ 1000); World 54,253,565 kt = 54,253.565 Mt (all sectors without LULUCF,
   2024); documentation only until metrics_ghg_emissions is in `models`. area: published in 1000 ha, metrics in km²
   since D33 (× 10); World land area 13,040,161.55 thousand ha = 130,401,615.5 km²; documentation only (no metrics
   model). #}
{% macro poc_value_range__faostat() %}
    {{ return({
        'bounds': [
            ['ghg_emissions', '%', '0', '8.2e4'],
            ['area', '%', '0', '2e8'],
        ],
        'models': [],
    }) }}
{% endmacro %}
