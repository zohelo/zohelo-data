{# GUGiK KI EGiB aggregate WFS (register PL-ENV-006), from bronze.gugik_egib_hits: register counts of parcels
   (ms:dzialki) and buildings (ms:budynki) as the county EGiB services supplied them to the aggregate service, read with
   resultType=hits (numberMatched). Grain: snapshot_date x feature_type x scope x area_code; scope PL (unfiltered, every
   record of the service), voivodeship and powiat (records whose identifier, ID_DZIALKI or ID_BUDYNKU, starts with the
   TERYT code). snapshot_date = the date the round started (a parcel round of 397 requests can span two runs;
   requested_at is each request's own time). A round is landed in 17 parts (PL and 16 voivodeships), each its own
   entity <feature type>_<round date>_<part>; only complete rounds are kept here, a round still being read waits for
   its remaining parts. A count is what the county service supplied, not a census. #}
with newest as (
    select *
    from {{ source('gugik_egib', 'gugik_egib_hits') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

complete as (
    select split_part(entity, '_', 1) || '_' || split_part(entity, '_', 2) as round_id
    from newest
    group by 1
    having count(distinct split_part(entity, '_', 3)) = 17
)

select
    cast(strptime(split_part(n.entity, '_', 2), '%Y%m%d') as date) as snapshot_date,
    split_part(n.entity, '_', 1) as feature_type,
    n.type_name,
    n.property as filter_property,
    n.scope,
    case n.scope when 'PL' then 'PL' else n.code end as area_code,
    case
        when regexp_full_match(n.number_matched, '[0-9]+') then cast(n.number_matched as bigint)
        else error('gugik_egib_counts: numberMatched is not a count: ' || coalesce(n.number_matched, 'NULL'))
    end as record_count,
    cast(strptime(n.requested_at, '%Y%m%dT%H%M%SZ') as timestamp) as requested_at,
    n.entity,
    n.source_version,
    n.raw_sha256
from newest as n
inner join complete as c on split_part(n.entity, '_', 1) || '_' || split_part(n.entity, '_', 2) = c.round_id
