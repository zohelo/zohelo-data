{{ config(severity='warn') }}
-- uzp_bzp: currencies of valued result notices without a unit_map row of the source. gold.build_uzp_bzp leaves their
-- contract values out (it emits a currency measure only with a mapped unit), so a new currency code is listed here
-- on every build until its unit and unit_map rows are added. A warning, not an error: one notice in a new currency
-- must not stop the platform's weekly build of every source.
select
    n.contract_value_currency,
    count(*) as notices
from {{ ref('uzp_bzp_notices') }} as n
left join {{ ref('unit_map') }} as m
    on m.source_system = 'uzp_bzp' and n.contract_value_currency = m.unit_code
where n.contract_value is not null and m.unit_code is null
group by n.contract_value_currency
