{# KOWR FOB export prices (#457): EUR/t as published; 154 to 358 EUR/t over 1,090 quotations of March 2023 to November
   2026 (highest HRW US Gulf, 29.03.2023; Dev trial 45920665418); Rouen wheat 191 (08.10.2025) to 273 (29.03.2023).
   Documentation only until a poc_metrics_wheat_export_price_fob model exists; the build stage has
   tests/kowr_markets_value_range.sql. #}
{% macro poc_value_range__kowr_markets() %}
    {{ return({
        'bounds': [
            ['wheat_export_price_fob', 'kowr_markets:fob_export_prices:EUR_T:%', '50', '1000'],
        ],
        'models': [],
    }) }}
{% endmacro %}
