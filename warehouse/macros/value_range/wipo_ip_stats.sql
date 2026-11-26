{# WIPO IP Statistics (GL-032, #1262): patent grants, total of the technology fields, countries only, 2000-2022 (file of
   April 2024; Dev probe 45872152431 and the PR's Dev trial): by filing office 1 to 798,431 (China 2022; Poland 2022
   2,222), by applicant origin 1 to the largest origin total of the trial; counts are at least 1 (no zero rows). #}
{% macro poc_value_range__wipo_ip_stats() %}
    {{ return({
        'bounds': [
            ['patent_grants_filing_office', 'wipo_ip_stats:patent_grants:grants:by_office', '1', '2e6'],
            ['patent_grants_applicant_origin', 'wipo_ip_stats:patent_grants:grants:by_origin', '1', '2e6'],
        ],
        'models': ['poc_metrics_patent_grants_filing_office', 'poc_metrics_patent_grants_applicant_origin'],
    }) }}
{% endmacro %}
