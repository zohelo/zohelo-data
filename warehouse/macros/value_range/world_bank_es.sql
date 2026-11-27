{# World Bank Enterprise Surveys (GL-010, #3315): all-firms totals of every economy, the metrics filter applied to
   build_world_bank_es (Dev trial 45932158707): bribery incidence 0 % (Sweden 2024) to 70.49 % (Liberia 2009; Poland
   2025 8.02); access to finance a major constraint 0 % (Kuwait 2025) to 74.97 % (Burkina Faso 2009; Poland 2025
   24.82); days to obtain an operating licence 1.21 (Poland 2019) to 193.11 (Korea 2024; Poland 2025 2.94), bound one
   year. #}
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
