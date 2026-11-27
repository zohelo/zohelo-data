{# UPRP annual reports (#3265): Poland a year, 2015-2025, read locally from the Dev probe texts (45811725126,
   45813573663) and in the PR's Dev trial: invention applications 3,314 (2022) to 4,818 (2015); patents granted
   2,043 (2024) to 3,548 (2016); trademark applications 14,471 (2022) to 18,763 (2025); industrial design
   applications 839 (2025) to 1,214 (2021). #}
{% macro poc_value_range__uprp_statistics() %}
    {{ return({
        'bounds': [
            ['patent_applications', '%', '1000', '2e4'],
            ['patents_granted', '%', '500', '2e4'],
            ['trademark_applications', '%', '5000', '1e5'],
            ['industrial_design_applications', '%', '200', '1e4'],
        ],
        'models': [
            'poc_metrics_patent_applications', 'poc_metrics_patents_granted',
            'poc_metrics_trademark_applications', 'poc_metrics_industrial_design_applications',
        ],
    }) }}
{% endmacro %}
