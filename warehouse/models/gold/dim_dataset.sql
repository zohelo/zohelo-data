{# Catalogue: one row per dataset (a block a source publishes or the platform holds), from
   gold.coverage. Block ids are normalised as in poc_dim_measure: DBW yearly files collapse to
   their block, Eurostat ids are compared upper-case. Eurostat names come from
   silver.eurostat_dataflows (name_en), since coverage has none. A name that is empty or a
   Landing file name (<ts>Z__<ts>Z.<ext>) becomes '<source name_en> · <code>' from the source seed. #}
with coverage as (
    select
        source,
        case
            when source = 'gus_dbw' then regexp_replace(block_id, '(_[0-9]{4})?_pl$', '')
            when source = 'eurostat' then upper(block_id)
            else block_id
        end as block_id,
        case when source = 'gus_dbw' then regexp_replace(block_name, ' \([0-9]{4}\)$', '') else block_name end
            as block_name,
        status,
        published_version,
        landed_version,
        in_bronze,
        silver_rows,
        checked_at
    from {{ source('gold_coverage', 'coverage') }}
),

dataflows as (
    select
        upper(dataset_id) as code,
        any_value(name_en) as name_en
    from {{ ref('eurostat_dataflows') }}
    group by upper(dataset_id)
),

datasets as (
    select
        source || ':' || block_id as dataset_key,
        source as source_key,
        block_id as code,
        any_value(block_name) as name,
        min(status) as status,
        max(published_version) as published_version,
        max(landed_version) as landed_version,
        bool_and(in_bronze) as in_bronze,
        sum(silver_rows) as silver_rows,
        count(*) as files,
        max(checked_at) as checked_at
    from coverage
    group by source, block_id
),

named as (
    select
        d.*,
        case when d.source_key = 'eurostat' then coalesce(nullif(d.name, ''), f.name_en) else d.name end
            as source_name
    from datasets as d
    left join dataflows as f on d.source_key = 'eurostat' and d.code = f.code
)

select
    d.dataset_key,
    d.source_key,
    d.code,
    case
        when
            d.source_name is null
            or d.source_name = ''
            or regexp_matches(d.source_name, '^[0-9]{8}T[0-9]{6}Z__[0-9]{8}T[0-9]{6}Z\.[a-z]+$')
            then s.name_en || ' · ' || d.code
        else d.source_name
    end as name,
    d.status,
    d.published_version,
    d.landed_version,
    d.in_bronze,
    d.silver_rows,
    d.files,
    d.checked_at
from named as d
left join {{ ref('source') }} as s on d.source_key = s.source_key
