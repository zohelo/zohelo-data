{# Gold block (D34, docs/source-registration.md; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__mf_jst_budgets() %}
    {{ return({
        'block_model': 'poc_build_mf_jst_budgets',
        'blocks': [
            ['jst_budget', 'MF local-government budget summaries (year to date)'],
        ],
    }) }}
{% endmacro %}
