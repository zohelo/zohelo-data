-- GUGiK KI EGiB (silver.gugik_egib_counts): every snapshot of a feature type holds the national count, the 16
-- voivodeships and the 380 powiats of the request list (ingestion/to_landing/gugik_egib/). A snapshot with fewer
-- (a request list cut short that still passed the Bronze checks) fails here.
with counts as (
    select
        snapshot_date,
        feature_type,
        count(*) filter (where scope = 'PL') as national,
        count(*) filter (where scope = 'voivodeship') as voivodeships,
        count(*) filter (where scope = 'powiat') as powiats
    from {{ ref('gugik_egib_counts') }}
    group by snapshot_date, feature_type
)

select *
from counts
where national <> 1 or voivodeships <> 16 or powiats <> 380
