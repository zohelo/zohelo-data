{# GUGiK PRNG (#2971): localities in the register, Poland is the largest area: 124,236 (export 2026-11-12, Dev probe
   43328622271). #}
{% macro poc_value_range__gugik_prng() %}
    {{ return({
        'bounds': [
            ['localities', '%', '0', '5e5'],
        ],
        'models': ['poc_metrics_localities'],
    }) }}
{% endmacro %}
