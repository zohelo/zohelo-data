{# Crossref works with affiliation text naming Poland or Polska per publication year (#553): largest PL 2022 = 29,413
   works (Dev trial 43336945209). #}
{% macro poc_value_range__crossref() %}
    {{ return({
        'bounds': [
            ['scholarly_works_pl_affiliation', '%', '0', '2e5'],
        ],
        'models': ['poc_metrics_scholarly_works_pl_affiliation'],
    }) }}
{% endmacro %}
