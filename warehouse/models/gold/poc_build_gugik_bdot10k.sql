{{ config(alias='build_gugik_bdot10k', tags=['gold_block']) }}
{# Gold build stage (#3097): GUGiK BDOT10k, topographic objects database (register PL-ENV-003), the catalogue of the
   powiat download packages (silver.bdot10k_powiat_packages, latest-only: one copy of the index). Rule: each powiat
   package counts once in Poland and once in its voivodeship (first 2 TERYT digits), by the year of its current
   edition (update_year, from Data_aktualizacji); date = the date the index copy was read (snapshot_date, frequency D).
   A package re-issued later moves to the newer year: the rows say how old the current editions are on that date,
   not how many packages were issued in a year. Block powiat_packages:
   - gugik_bdot10k:powiat_packages:packages:count, packages, breakdown edition_year ('_T' every year, is_total);
   - gugik_bdot10k:powiat_packages:packages:share, % of the area's packages whose current edition is from that year
     (no total row). Only years that occur in an area get a row; a year without packages there is a true zero, not a
     suppressed cell, and is not written. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_bdot10k'
),

packages as (
    select
        package_id,
        voivodeship_code,
        update_year,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('bdot10k_powiat_packages') }}
),

-- one row: a second index copy in Silver repeats every count row and fails the grain test
provenance as (
    select
        snapshot_date,
        source_version,
        raw_sha256
    from packages
    group by all
),

membership as (
    select
        'PL' as area_code,
        package_id,
        update_year
    from packages
    union all
    select
        voivodeship_code,
        package_id,
        update_year
    from packages
),

totals as (
    select
        area_code,
        count(package_id) as n
    from membership
    group by area_code
),

by_year as (
    select
        area_code,
        cast(update_year as varchar) as edition_year,
        count(package_id) as n
    from membership
    group by area_code, update_year
),

measures as (
    select
        area_code,
        edition_year,
        'count' as measure_codes,
        cast(n as double) as value
    from by_year
    union all
    select
        area_code,
        '_T' as edition_year,
        'count' as measure_codes,
        cast(n as double) as value
    from totals
    union all
    select
        y.area_code,
        y.edition_year,
        'share' as measure_codes,
        round(100.0 * y.n / t.n, 2) as value
    from by_year as y
    inner join totals as t on y.area_code = t.area_code
)

select
    'gugik_bdot10k' as source_system,
    'powiat_packages' as block_id,
    'GUGiK BDOT10k powiat download packages' as block_name,
    'gugik_bdot10k:powiat_packages:packages:' || m.measure_codes as measure_key,
    case m.measure_codes
        when 'count' then 'BDOT10k powiat packages by year of the current edition'
        else 'Share of BDOT10k powiat packages by year of the current edition'
    end as measure_name,
    m.measure_codes,
    case m.measure_codes when 'count' then 'packages' else 'pct' end as unit_code,
    case m.measure_codes when 'count' then 'packages' else 'per cent' end as unit_name,
    m.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    p.snapshot_date as date_key,
    cast(p.snapshot_date as varchar) as period_label,
    cast(json_object('edition_year', m.edition_year) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.edition_year = '_T' as is_total,
    m.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from measures as m
cross join provenance as p
left join geo as g on m.area_code = g.source_code
