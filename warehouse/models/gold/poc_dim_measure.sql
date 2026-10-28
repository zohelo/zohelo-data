{{ config(alias='dim_measure') }}
{# Gold PoC: the measure catalogue. Activated measures come from the build stages (one row
   per measure of each activated block); every other block each source publishes or holds
   comes from gold.coverage with its status, so a block the source does not offer is
   visible as a measure not offered, not as missing data.
   dataset_key = source_system:block_id, the key of gold.dim_dataset. #}
with built as (
    select
        measure_key,
        source_system,
        block_id,
        any_value(block_name) as block_name,
        any_value(measure_name) as measure_name,
        any_value(measure_codes) as measure_codes,
        any_value(unit_code) as unit_key,
        list(distinct frequency order by frequency) as frequencies,
        min(date_key) as first_date,
        max(date_key) as last_date,
        count(distinct {{ poc_series_key() }}) as series,
        count(*) as points,
        count(distinct geography_key) as areas
    from ({{ poc_observations() }})
    group by measure_key, source_system, block_id
),

activated_blocks as (
    select distinct
        source_system,
        block_id
    from built
),

coverage as (  -- the other blocks, one row per block: DBW files collapse to their block, ids compared case-free
    select
        c.source_system,
        c.block_id,
        any_value(c.block_name) as block_name,
        min(c.status) as status,
        sum(c.silver_rows) as silver_rows
    from (
        select
            source as source_system,
            case
                when source = 'gus_dbw' then regexp_replace(block_id, '(_[0-9]{4})?_pl$', '')
                when source = 'eurostat' then upper(block_id)
                else block_id
            end as block_id,
            case when source = 'gus_dbw' then regexp_replace(block_name, ' \([0-9]{4}\)$', '') else block_name end
                as block_name,
            status,
            silver_rows
        from {{ source('gold_coverage', 'coverage') }}
        where source in ('eurostat', 'gus_bdl', 'gus_dbw', 'world_bank_wdi', 'nbp')
    ) as c
    left join activated_blocks as a on c.source_system = a.source_system and c.block_id = a.block_id
    where a.block_id is null
    group by c.source_system, c.block_id
)

select
    b.measure_key,
    b.source_system || ':' || b.block_id as dataset_key,
    b.source_system,
    b.block_id,
    b.block_name,
    b.measure_name,
    b.measure_codes,
    b.unit_key,
    b.frequencies,
    b.first_date,
    b.last_date,
    b.series,
    b.points,
    b.areas,
    true as activated,
    'activated' as status,
    cast(null as bigint) as silver_rows
from built as b
union all
select
    source_system || ':' || block_id as measure_key,
    source_system || ':' || block_id as dataset_key,
    source_system,
    block_id,
    block_name,
    coalesce(block_name, block_id) as measure_name,
    null as measure_codes,
    null as unit_key,
    [] as frequencies,
    null as first_date,
    null as last_date,
    null as series,
    null as points,
    null as areas,
    false as activated,
    case status when 'loaded' then 'not activated' else status end as status,
    silver_rows
from coverage
