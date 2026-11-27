{# Senate (#482): sitting days per month of finished sittings (a month has at most 31 days) and votes per month,
   Poland. Not yet measured (no trial got past the site's bot check, #482): probe 43326678967 saw 491 votes on 16
   sitting days (about 31 a day), so a month of 8 sitting days gives about 250; 2,000 leaves room for an amendment-
   heavy month and still catches a count of the wrong unit.
   A month without a sitting has no row, so the lower bound is 1. #}
{% macro poc_value_range__senat() %}
    {{ return({
        'bounds': [
            ['senate_sitting_days', '%', '1', '31'],
            ['senate_votes', '%', '1', '2000'],
        ],
        'models': ['poc_metrics_senate_sitting_days', 'poc_metrics_senate_votes'],
    }) }}
{% endmacro %}
