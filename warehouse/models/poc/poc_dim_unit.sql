{{ config(alias='dim_unit') }}
{# Gold PoC: units of measure as the sources name them, with a conformed unit where the
   mapping is known. unit_key is the source's own code (unique in the PoC sample). #}
with used as (
    select
        unit_code,
        any_value(unit_name) as unit_name,
        list(distinct source_system) as sources
    from ({{ poc_observations() }})
    group by unit_code
)

select
    unit_code as unit_key,
    unit_name,
    case
        when unit_code in ('NR', 'osoba', 'persons', 'osoby') or lower(unit_name) like '%osob%' then 'persons'
        when unit_code in ('PC_ACT', 'PC_POP', 'RCH_A', 'PC', '%') or unit_name like '%procent%' then 'percent'
        when unit_code = 'CP_MEUR' then 'EUR million'
        when unit_code = 'CP_EUR_HAB' then 'EUR per inhabitant'
        when unit_code = 'CLV_PCH_PRE' then 'percent change on previous period'
        when unit_code in ('zł', 'PLN') then 'PLN'
        when unit_code = 'PLN/g' then 'PLN per gram'
        when unit_code = 'ha' then 'hectare'
        when unit_code in ('km2', 'km²', 'sq. km') then 'square kilometre'
        when unit_code = 'mieszk.' or lower(unit_name) like '%mieszka%' then 'dwellings'
        else unit_name
    end as conformed_unit,
    sources
from used
union all
select
    '_Z' as unit_key,
    'Not applicable' as unit_name,
    'not applicable' as conformed_unit,
    [] as sources
