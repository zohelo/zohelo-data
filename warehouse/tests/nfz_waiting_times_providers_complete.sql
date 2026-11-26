-- NFZ waiting times (PLS-015) providers: every NFZ voivodeship branch lists providers, and every provider has a
-- queue entry.
select province_code, count(*) as n, 'no providers' as problem
from (select lpad(cast(p as varchar), 2, '0') as province_code from range(1, 17) as t (p)) as g
anti join {{ ref('nfz_waiting_times_providers') }} as p using (province_code)
group by all
union all
select province_code, count(*), 'provider without queues'
from {{ ref('nfz_waiting_times_providers') }}
where queue_entries < 1
group by all
