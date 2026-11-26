{# WIPO IP Statistics (GL-032, #1262): patent grants, total of the technology fields, countries only, 2000-2022 (file of
   April 2024; Dev trial 45875926273, the metrics filter applied to build_wipo_ip_stats): by filing office 1 (Ireland
   2005) to 798,431 (China 2022; Poland 2022 2,222), by applicant origin 1 (Montenegro 2008) to 736,101 (China 2022;
   Poland 2022 2,750); counts are at least 1 (the files have no zero rows). #}
{% macro poc_value_range__wipo_ip_stats() %}
    {{ return({
        'bounds': [
            ['patent_grants_filing_office', 'wipo_ip_stats:patent_grants:grants:by_office', '1', '2e6'],
            ['patent_grants_applicant_origin', 'wipo_ip_stats:patent_grants:grants:by_origin', '1', '2e6'],
        ],
        'models': ['poc_metrics_patent_grants_filing_office', 'poc_metrics_patent_grants_applicant_origin'],
    }) }}
{% endmacro %}
