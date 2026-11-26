{{ config(alias='uokik_decision') }}
{# Gold entity table (documents, docs/model.md "Fact kinds"): one row per decision of the President of UOKiK in the
   decisions database (register PLG-020; concentration decisions, PLG-021, flagged), keyed by the view entry's @unid,
   from silver.uokik_decisions; date_key = the decision date. Number, department signature, subject-area category,
   year as the view gives them. The index names no enterprise identifier (KRS, NIP, REGON, LEI), so the table joins
   no entity table; party names are not stored (a party can be a natural person). #}
select
    unid,
    decision_number,
    department_code,
    department,
    decision_date as date_key,
    decision_year,
    category,
    category_key,
    is_concentration,
    link,
    'uokik_decisions' as source_system,
    source_version,
    raw_sha256
from {{ ref('uokik_decisions') }}
