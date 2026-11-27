{# World Bank Enterprise Surveys (GL-010, #3315): all-firms totals of every economy, the metrics filter applied to
   build_world_bank_es (Dev trial TRIAL): BOUNDS_EVIDENCE #}
{% macro poc_value_range__world_bank_es() %}
    {{ return({
        'bounds': [
            ['firms_bribery_incidence', 'world_bank_es:wb_es:PT_F:WB_ES_T_GRAFT3', '0', '100'],
            ['firms_finance_constraint', 'world_bank_es:wb_es:PT_F:WB_ES_T_FIN16', '0', '100'],
            ['operating_licence_days', 'world_bank_es:wb_es:D:WB_ES_T_BUS2', '0', '365'],
        ],
        'models': [
            'poc_metrics_firms_bribery_incidence',
            'poc_metrics_firms_finance_constraint',
            'poc_metrics_operating_licence_days',
        ],
    }) }}
{% endmacro %}
