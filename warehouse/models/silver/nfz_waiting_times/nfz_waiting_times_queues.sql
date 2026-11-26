{# NFZ waiting times (API Terminy Leczenia v1.4, register PLS-015): every queue of the newest complete monthly
   snapshot, one row per queue entry (provider x place x benefit x case; queue_id is the API's id). Typed rows of
   macro poc_nfz_waiting_times_snapshots (warehouse/macros/sources/nfz_waiting_times.sql); values as published.
   Grain: queue_id. Older months stay in Bronze and enter Silver only as the counts of nfz_waiting_times_monthly. #}
with snapshots as ({{ poc_nfz_waiting_times_snapshots() }}),


newest as (
    select max(s.snapshot_month) as snapshot_month
    from snapshots as s
)

select q.*
from snapshots as q
inner join newest as n on q.snapshot_month = n.snapshot_month
