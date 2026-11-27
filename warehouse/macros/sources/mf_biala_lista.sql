{# Gold block (D34, docs/source-registration.md; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__mf_biala_lista() %}
    {{ return({
        'block_model': 'poc_build_mf_biala_lista',
        'blocks': [
            ['flat_file', 'MF VAT white list, flat file'],
        ],
    }) }}
{% endmacro %}
