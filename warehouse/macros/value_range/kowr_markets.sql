{# KOWR FOB export prices (#457): EUR/t as published; highest in the probe's reports HRW US Gulf 358 EUR/t (quotation
   29.03.2023, Dev probe 45895465874), lowest Black Sea feed barley 167 EUR/t (02.07.2025). Documentation only until a
   poc_metrics_wheat_export_price_fob model exists; the build stage has tests/kowr_markets_value_range.sql. #}
{% macro poc_value_range__kowr_markets() %}
    {{ return({
        'bounds': [
            ['wheat_export_price_fob', 'kowr_markets:fob_export_prices:EUR_T:%', '50', '1000'],
        ],
        'models': [],
    }) }}
{% endmacro %}
