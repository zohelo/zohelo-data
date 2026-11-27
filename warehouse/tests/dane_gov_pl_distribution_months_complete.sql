-- dane_gov_pl (PLG-030): the distribution table is not empty, comes from one Landing file, every row counts at least
-- one distribution and never more sized distributions than distributions. One test per Silver model (it reads no
-- other model).
select
    'rows ' || count(*) || ', files ' || count(distinct raw_sha256) || ', rows without a distribution '
    || count(*) filter (where distributions < 1) || ', rows with more sized than counted '
    || count(*) filter (where sized > distributions) as problem
from {{ ref('dane_gov_pl_distribution_months') }}
having
    count(*) = 0 or count(distinct raw_sha256) <> 1 or count(*) filter (where distributions < 1) > 0
    or count(*) filter (where sized > distributions) > 0
