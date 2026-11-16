{{ config(alias='build_nfz_pharmacy_refunds', tags=['gold_block']) }}
{# Gold build stage: NFZ pharmacy refunds (silver.nfz_ra_monthly_provisions), monthly, Poland and the 16
   voivodeships (poc_map_geography section nfz_pharmacy_refunds) x ATC selection. One measure per published value
   (macro poc_nfz_pharmacy_refunds_measures): measure_key nfz_pharmacy_refunds:monthly_provisions:<unit>:<field>;
   the ATC main group is the breakdown ({"atc": "<code>"}); the selection without ATC filter is the total
   (breakdown {}, is_total), which includes refunds outside the 14 groups (the API's group "-"), so the groups
   do not sum to it. Values, units and scale as published (D33); a NULL value is not loaded. Frequency M only. #}
with obs as (
    select * from {{ ref('nfz_ra_monthly_provisions') }}
),

long as (
    unpivot obs
    on quantity, refund, donation, contribution_of_patient, value
    into name field value obs_value
),

measures as ({{ poc_nfz_pharmacy_refunds_measures() }}),

geo_map as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'nfz_pharmacy_refunds'
)

select
    'nfz_pharmacy_refunds' as source_system,
    'monthly_provisions' as block_id,
    'NFZ pharmacy refunds, monthly, by voivodeship and ATC group' as block_name,
    'nfz_pharmacy_refunds:monthly_provisions:' || m.unit_code || ':' || l.field as measure_key,
    m.measure_name,
    l.field as measure_codes,
    m.unit_code,
    m.unit_name,
    l.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    l.period_start as date_key,
    strftime(l.period_start, '%Y-%m') as period_label,
    case when l.atc_code = '_T' then '{}' else cast(to_json({ 'atc': l.atc_code }) as varchar) end as breakdown_json,
    case when l.atc_code = '_T' then null else cast(to_json({ 'atc': l.atc_name }) as varchar) end
        as breakdown_label_json,
    cast(null as varchar) as sex_code,
    l.atc_code = '_T' as is_total,
    l.obs_value as value,
    'ok' as observation_status_key,
    l.source_version,
    l.raw_sha256
from long as l
inner join measures as m on l.field = m.field
left join geo_map as g on l.area_code = g.source_code
