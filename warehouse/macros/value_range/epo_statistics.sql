{# EPO patent statistics (#3320): counts per country of residence and year, 2016-2025 (2025 edition, Dev probe
   45924424161): published zeros for small countries (e.g. Angola, Aruba), Poland 393 (2016) to 692 (2024) applications
   and 180 (2016) to 327 (2025) grants; the largest series are the world 'Total' (WB:WLD), 159,087 to 201,974
   applications and 81,754 to 137,784 grants. Documentation only (no metrics models yet): the bounds wait for them. #}
{% macro poc_value_range__epo_statistics() %}
    {{ return({
        'bounds': [
            ['european_patent_applications', '%', '0', '4e5'],
            ['european_patents_granted', '%', '0', '3e5'],
        ],
        'models': [],
    }) }}
{% endmacro %}
