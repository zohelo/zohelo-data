{{ config(alias='metrics_government_drafts_published') }}
{# Gold PoC metrics view: concept government_drafts_published (macros/poc_metrics.sql); drafts published per month
   on the work list of the Council of Ministers (kprm_work_list). #}
{{ poc_concept_metrics('government_drafts_published') }}
