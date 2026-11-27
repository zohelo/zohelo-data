{# URE energy price indicators (#2870): prices as published, Poland. Measured on the pages read on 26 Nov 2026 (Dev
   probe 45790234209): competitive market yearly 155.44 (2008) to 759.29 PLN/MWh (2023), quarterly
   160.60 (2017 Q1) to 889.69 PLN/MWh (2023 Q1); households 0.4862 (2019) to 0.9198 PLN/kWh (2025); gas imported from
   EU/EFTA states 30.89 (2020 Q2) to 886.88 PLN/MWh (2022 Q3). Bounds leave room for a price shock (about 3x). #}
{% macro poc_value_range__ure_prices() %}
    {{ return({
        'bounds': [
            ['electricity_price_competitive_annual', '%', '50', '3000'],
            ['electricity_price_competitive_quarterly', '%', '50', '3000'],
            ['electricity_price_households', '%', '0.1', '3'],
            ['gas_import_price_eu_efta', '%', '5', '3000'],
        ],
        'models': [
            'poc_metrics_electricity_price_competitive_annual', 'poc_metrics_electricity_price_competitive_quarterly',
            'poc_metrics_electricity_price_households', 'poc_metrics_gas_import_price_eu_efta',
        ],
    }) }}
{% endmacro %}
