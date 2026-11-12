{{ config(alias='senate_votes') }}
{# Gold entity table (register source, kpi false; docs/model.md "Fact kinds": events): one row per vote of the
   Senate of the Republic of Poland, term XI (route D36), keyed by the Senate's vote id; date_key = the sitting day's
   date. Subject and subtitle as printed (metadata only, no texts of bills), totals as printed on the vote's page
   (empty while that page is not read yet, results_landed false). Individual senators' votes are not kept. #}
select
    vote_id,
    sitting_id,
    sitting_number,
    day_number,
    day_date as date_key,
    vote_number,
    subject,
    subtitle,
    is_finished,
    results_landed,
    voted,
    votes_for,
    votes_against,
    abstained,
    'senat' as source_system,
    source_version,
    raw_sha256,
    result_raw_sha256
from {{ ref('senat_votes') }}
