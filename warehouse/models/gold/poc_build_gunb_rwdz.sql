{{ config(alias='build_gunb_rwdz', tags=['gold_block']) }}
{# Gold build stage: GUNB RWDZ building-permit decisions per month (register PL-ENV-033), from the entity table
   silver.gunb_rwdz_records (one row per register record, latest file of each voivodeship). Event counts:
   measure gunb_rwdz:permits:decisions = register records whose decision was issued in the month (date_key = first
   day of the month of decision_date). Breakdown investment_kind: the register's own two kinds, 'jednorodzinny'
   (Budynek mieszkalny jednorodzinny) and 'inny' (any other building works), '_U' when not stated, and the total
   '_T'. Areas: Poland, the voivodeship of the file (every record of a file belongs to its voivodeship, column
   wojewodztwo checked by the loader) and the powiat of the record's TERYT code (TERC 4 digits); a record without
   a TERYT code (about 6 % of lines) counts for Poland and its voivodeship only, so powiats do not add up to their
   voivodeship. Months from 2020-01 (register entry: smallest Gold selection) up to the last month before the
   oldest file version (the current month is still being filled). Records whose lines disagree on the decision
   date (Silver: null, named in conflicting_columns) are not counted. #}
with records as (
    select
        voivodeship_code,
        left(terc_code, 4) as powiat_code,
        case
            when investment_kind = 'Budynek mieszkalny jednorodzinny' then 'jednorodzinny'
            when investment_kind = 'Obiekt budowlany inny niż budynek mieszkalny jednorodzinny' then 'inny'
            else '_U'
        end as investment_kind,
        date_trunc('month', decision_date) as month_start,
        source_version,
        raw_sha256
    from {{ source('gunb_rwdz_silver', 'gunb_rwdz_records') }}
    where decision_date is not null
),

cutoff as (  -- first day of the month of the oldest file version: that month and later are left out
    select date_trunc('month', min(cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date))) as first_open_month
    from records
),

selected as (
    select r.*
    from records as r
    cross join cutoff as c
    where r.month_start >= date '2020-01-01' and r.month_start < c.first_open_month
),

located as (  -- one row per record and area level
    select
        'PL' as area_code,
        investment_kind,
        month_start,
        source_version,
        raw_sha256
    from selected
    union all
    select
        voivodeship_code as area_code,
        investment_kind,
        month_start,
        source_version,
        raw_sha256
    from selected
    union all
    select
        powiat_code as area_code,
        investment_kind,
        month_start,
        source_version,
        raw_sha256
    from selected
    where powiat_code is not null
),

counted as (
    select
        area_code,
        investment_kind,
        month_start,
        count(*) as decisions,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from located
    group by area_code, investment_kind, month_start
    union all
    select
        area_code,
        '_T' as investment_kind,
        month_start,
        count(*) as decisions,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from located
    group by area_code, month_start
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gunb_rwdz'
)

select
    'gunb_rwdz' as source_system,
    'permits' as block_id,
    'GUNB RWDZ building-permit decisions' as block_name,
    'gunb_rwdz:permits:decisions' as measure_key,
    'Building-permit decisions issued (GUNB RWDZ register)' as measure_name,
    'decisions' as measure_codes,
    'decisions' as unit_code,
    'decisions' as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    cast(c.month_start as date) as date_key,
    strftime(c.month_start, '%Y-%m') as period_label,
    '{"investment_kind": "' || c.investment_kind || '"}' as breakdown_json,
    case
        when c.investment_kind = 'jednorodzinny' then '{"investment_kind": "Budynek mieszkalny jednorodzinny"}'
        when c.investment_kind = 'inny'
            then '{"investment_kind": "Obiekt budowlany inny niż budynek mieszkalny jednorodzinny"}'
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.investment_kind = '_T' as is_total,
    cast(c.decisions as double) as value,
    'ok' as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
left join geo_map as g on c.area_code = g.source_code
