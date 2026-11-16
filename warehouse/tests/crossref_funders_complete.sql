-- Crossref Funder Registry entries located in Poland: Silver holds every entry the registry reports (one page,
-- total-results) and at least 100 (183 on 12 Nov 2026), so a lost page or a changed location filter fails here.
select
    count(*) as funders,
    max(registry_total) as registry_total
from {{ ref('crossref_funders') }}
having count(*) <> max(registry_total) or count(*) < 100
