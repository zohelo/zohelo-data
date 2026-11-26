{# ARiMR direct payments (#2487): published in PLN, metrics in million PLN (D33, ÷ 1e6); highest Poland campaign 2022 =
   16,858,050,328 PLN = 16,858 mln (report of 30.09.2026, probe 43345651745). Documentation only until a
   poc_metrics_direct_payments_paid model exists; the build stage has tests/arimr_payments_value_range.sql. #}
{% macro poc_value_range__arimr_payments() %}
    {{ return({
        'bounds': [
            ['direct_payments_paid', '%', '0', '3e4'],
        ],
        'models': [],
    }) }}
{% endmacro %}
