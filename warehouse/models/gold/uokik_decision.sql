{{ config(alias='uokik_decision') }}
{# Gold entity table (documents, docs/model.md "Fact kinds"): one row per decision of the President of UOKiK in the
   decisions database (register PLG-020; concentration decisions, PLG-021, flagged), keyed by the view entry's @unid,
   from silver.uokik_decisions; date_key = the decision date. Number, department signature and year as the view gives
   them; categories = every subject-area category that lists the decision (Silver has one row per unid x category),
   sorted and joined with " | ". The decision's own fields are grouped, never picked: two numbers, dates or years for
   one unid give two rows and fail the unique test. raw_sha256 / source_version = the newest category document that
   lists it. The index names no enterprise identifier (KRS, NIP, REGON, LEI), so the table joins no entity table; party
   names are not stored (a party can be a natural person). #}
select
    unid,
    decision_number,
    department_code,
    decision_date as date_key,
    decision_year,
    string_agg(distinct category, ' | ' order by category) as categories,
    count(*) as category_count,
    is_concentration,
    link,
    'uokik_decisions' as source_system,
    max(source_version) as source_version,
    arg_max(raw_sha256, ingested_at) as raw_sha256
from {{ ref('uokik_decisions') }}
group by unid, decision_number, department_code, decision_date, decision_year, is_concentration, link
