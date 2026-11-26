-- dane_gov_pl (PLG-030): Silver holds datasets from exactly one Landing file, and every Bronze line is accounted for
-- (each kept row counts its harvest records, so their sum reaches the highest line number). One test per Silver model
-- (it reads no other model).
select
    'rows ' || count(*) || ', files ' || count(distinct raw_sha256) || ', records ' || coalesce(sum(harvest_records), 0)
    || ', highest line ' || coalesce(max(source_row_number), 0) as problem
from {{ ref('dane_gov_pl_datasets') }}
having count(*) = 0 or count(distinct raw_sha256) <> 1 or sum(harvest_records) < max(source_row_number)
