{# dane.gov.pl catalogue (#3263): datasets and distributions per month of last modification, Poland. A month can hold
   at most the whole catalogue (10,001 datasets, 672,460 distributions in Dev trial 45827739830); largest months
   2,942 datasets (2025-09) and 198,552 distributions (2026-01), 0 rows outside these bounds (same trial). #}
{% macro poc_value_range__dane_gov_pl() %}
    {{ return({
        'bounds': [
            ['open_data_datasets_modified', '%', '1', '30000'],
            ['open_data_distributions_modified', '%', '1', '2e6'],
        ],
        'models': ['poc_metrics_open_data_datasets_modified', 'poc_metrics_open_data_distributions_modified'],
    }) }}
{% endmacro %}
