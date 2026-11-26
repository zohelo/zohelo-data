{# dane.gov.pl catalogue (#3263): datasets and distributions per month of last modification, Poland. A month can hold
   at most the whole catalogue (10,001 datasets, 672,460 distributions in Dev probe 45818878323); largest months
   BOUNDS_EVIDENCE. #}
{% macro poc_value_range__dane_gov_pl() %}
    {{ return({
        'bounds': [
            ['open_data_datasets_modified', '%', '1', '30000'],
            ['open_data_distributions_modified', '%', '1', '2e6'],
        ],
        'models': ['poc_metrics_open_data_datasets_modified', 'poc_metrics_open_data_distributions_modified'],
    }) }}
{% endmacro %}
