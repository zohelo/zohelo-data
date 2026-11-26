-- dane_gov_pl (PLG-030): the theme table is not empty and comes from one Landing file. One test per Silver model (it
-- reads no other model).
select 'rows ' || count(*) || ', files ' || count(distinct raw_sha256) as problem
from {{ ref('dane_gov_pl_dataset_categories') }}
having count(*) = 0 or count(distinct raw_sha256) <> 1
