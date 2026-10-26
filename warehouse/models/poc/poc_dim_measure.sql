{{ config(alias='dim_measure') }}
{# Gold PoC: the measure catalogue. Activated measures come from the build stages (one row
   per measure of each activated block); every other block each source publishes or holds
   comes from gold.coverage with its status, so a block the source does not offer is
   visible as a measure not offered, not as missing data. #}
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

coverage as (
    select
        c.source as source_system,
        c.block_id,
        c.block_name,
        c.status,
        c.silver_rows
    from {{ source('gold_coverage', 'coverage') }} as c
    left join activated_blocks as a on c.source = a.source_system and c.block_id = a.block_id
    where a.block_id is null and c.source in ('eurostat', 'gus_bdl', 'gus_dbw', 'world_bank_wdi', 'nbp')
)

select
    b.measure_key,
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
