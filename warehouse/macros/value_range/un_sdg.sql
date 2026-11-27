{# UN SDG (GL-018, #3316): women_in_parliament is SG_GEN_PARL, the share of seats held by women in the national
   parliament, per cent of all seats, 27 EU countries 2000-2026: 0.0 to 47.28 (Dev probe 45935909818; Poland 2026
   31.52). A share cannot leave 0-100. #}
{% macro poc_value_range__un_sdg() %}
    {{ return({
        'bounds': [
            ['women_in_parliament', 'un_sdg:sdg_observations:PERCENT:SG_GEN_PARL', '0', '100'],
        ],
        'models': ['poc_metrics_women_in_parliament'],
    }) }}
{% endmacro %}
