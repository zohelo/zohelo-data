-- NFZ waiting times (PLS-015) providers: every NFZ voivodeship branch lists providers, and every provider has a
-- name and at least one queue entry.
select province_code, count(*) as n, 'no providers' as problem
from (select lpad(cast(p as varchar), 2, '0') as province_code from range(1, 17) as t (p)) as g
anti join {{ ref('nfz_waiting_times_providers') }} as p using (province_code)
group by all
union all
select province_code, count(*), 'provider without name or queues'
from {{ ref('nfz_waiting_times_providers') }}
where coalesce(provider_name, '') = '' or queue_entries < 1
group by all
