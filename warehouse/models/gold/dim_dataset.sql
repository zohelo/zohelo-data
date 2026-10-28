{# Catalogue: one row per dataset (a block a source publishes or the platform holds), from
   gold.coverage. Block ids are normalised as in poc_dim_measure: DBW yearly files collapse to
   their block, Eurostat ids are compared upper-case. #}
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
)

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
