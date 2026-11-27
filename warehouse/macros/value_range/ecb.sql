{# ECB (#2758): the ECB's HICP is Eurostat's HICP republished (TITLE_COMPL "Statistical Office of the European
   Commission (Eurostat)"), so it takes Eurostat's bound; EUR/PLN monthly average 4.3813 in 2026-10 and daily
   4.0712-4.4038 since 1999 (EXR, Dev trial 43099368908), NBP's bound (moved from tests/metrics_value_range.sql). #}
{% macro poc_value_range__ecb() %}
    {{ return({
        'bounds': [
            ['inflation', '%', '-100', '3000'],
            ['fx_rate', '%', '0', '100000'],
        ],
        'models': [
            'poc_metrics_inflation',
            'poc_metrics_fx_rate',
        ],
    }) }}
{% endmacro %}
