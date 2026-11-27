{# WTO Stats, OECD-WTO BaTIS BPM6 (#3317): Poland's total services exports and imports with the World, final value,
   US$ million as published; Dev trial 45935187389: exports 18,148.8 (2005) to 130,450.3 (2025), imports 14,946.0
   (2005) to 86,091.2 (2025). Documentation only: poc_metrics_exports_services is not listed, because its UNCTAD rows
   have no bound of their own yet (listing it would fail them); imports_services has no metrics model yet. #}
{% macro poc_value_range__wto_stats() %}
    {{ return({
        'bounds': [
            ['exports_services', 'wto_stats:batis:USD_MN:X.S.final', '0', '1e6'],
            ['imports_services', 'wto_stats:batis:USD_MN:M.S.final', '0', '1e6'],
        ],
        'models': [],
    }) }}
{% endmacro %}
