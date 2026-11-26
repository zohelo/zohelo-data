{# e-Zamówienia BZP landed days (bronze.uzp_bzp_notices): one row per landed (notice type, UTC publication day),
   entity <contract|result>_<YYYYMMDD>, from the newest Landing copy of each, with the notices it holds and the
   API's notice/stats count of the type and day. A day without notices is one marker row in Bronze (object_id
   null) and counts 0 notices here. gold.build_uzp_bzp takes a month only when every day of it has both types
   here, so a day that never landed keeps its month out of Gold. #}
with newest_file as (
    select *
    from {{ source('uzp_bzp', 'uzp_bzp_notices') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
)

select
    entity,
    case split_part(entity, '_', 1)
        when 'contract' then 'ContractNotice'
        when 'result' then 'TenderResultNotice'
    end as notice_type,
    cast(strptime(split_part(entity, '_', 2), '%Y%m%d') as date) as publication_date,
    count(object_id) as notices,
    max(stats_count) as stats_count,
    min(stats_count) as stats_count_min,
    max(raw_sha256) as raw_sha256,
    count(distinct raw_sha256) as files,
    max(source_version) as source_version,
    max(ingested_at) as ingested_at
from newest_file
group by entity
