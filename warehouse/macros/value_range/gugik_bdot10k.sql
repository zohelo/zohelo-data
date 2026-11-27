{# GUGiK BDOT10k (#3097): powiat packages in the index; Poland is the largest area: 380 (index read 26 Nov 2026, Dev
   45827286618); there are 380 powiats, so 500 leaves room for a reform without hiding a doubled index. #}
{% macro poc_value_range__gugik_bdot10k() %}
    {{ return({
        'bounds': [
            ['bdot10k_powiat_packages', '%', '1', '500'],
        ],
        'models': ['poc_metrics_bdot10k_powiat_packages'],
    }) }}
{% endmacro %}
