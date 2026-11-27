{# Gold block (D34, docs/source-registration.md; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__nbp() %}
    {{ return({
        'block_model': 'poc_build_nbp',
        'blocks': [
            ['fx_A', 'NBP exchange rates, table A'],
            ['fx_C', 'NBP exchange rates, table C'],
            ['gold', 'NBP gold fixings'],
        ],
    }) }}
{% endmacro %}
