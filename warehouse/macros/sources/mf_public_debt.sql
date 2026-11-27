{# Gold block (D34, docs/source-registration.md; moved here from the shared lists of macros/poc_blocks.sql on 27 November). #}
{% macro poc_source__mf_public_debt() %}
    {{ return({
        'block_model': 'poc_build_public_finance',
        'activated': "select * from (values
            ('mf_public_debt', 'debt_gdp', 'Public debt in relation to GDP (MF)'),
            ('mf_public_debt', 'debt', 'Public debt (MF)'),
            ('mf_public_debt', 'edp_debt', 'General government (EDP) debt by instrument (MF)'),
            ('mf_treasury_debt', 'debt_instrument', 'State Treasury debt by instrument (MF)'),
            ('mf_state_budget', 'execution_ytd', 'State budget execution, cumulative (MF)'),
            ('mf_state_budget', 'plan', 'State budget act (MF)'),
            ('krus', 'benefits', 'KRUS pensions and benefits')
        ) as t (source_system, block_id, block_name)",
    }) }}
{% endmacro %}
