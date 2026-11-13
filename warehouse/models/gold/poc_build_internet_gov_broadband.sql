{{ config(alias='build_internet_gov_broadband', tags=['gold_block']) }}
{# Gold build stage: internet.gov.pl fixed broadband coverage of address points (register PL-ENV-034), quarterly
   observations of silver.internet_gov_broadband_coverage. Block 'coverage'; one frequency (Q), date_key = first day
   of the reference quarter (stock at its last day). Measures (unit as published: address points, or per cent):
   - covered: address points within fixed broadband coverage (territory table; Poland and the 16 voivodeships);
   - covered_share: covered / all address points x 100, own calculation, rounded to 2 decimals as the report rounds
     its shares (both numbers are printed in the same table; the report prints no share per voivodeship);
   - covered_100plus, covered_1000plus: Poland, sum of the bandwidth classes from 100 Mb/s and from 1000 Mb/s
     upwards (own calculation: the classes are exclusive rows that add up to SUMA);
   - covered_100plus_share, covered_1000plus_share: those sums / all address points of Poland x 100 (own
     calculation; the report's printed shares in the bandwidth table are of the address points in coverage, not of
     all address points, so they are not used). The report has no suppression or missing markers; every row
     carries a value. #}
with s as (
    select * from {{ ref('internet_gov_broadband_coverage') }}
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'internet_gov_broadband'
),

territory as (
    select
        reference_date,
        area_code,
        address_points,
        address_points_total,
        source_version,
        raw_sha256
    from s
    where table_code = 'territory'
),

bandwidth as (
    select
        b.reference_date,
        sum(b.address_points) filter (where b.bandwidth_mbps >= 100) as covered_100plus,
        sum(b.address_points) filter (where b.bandwidth_mbps >= 1000) as covered_1000plus
    from s as b
    where b.table_code = 'bandwidth' and b.class_code <> '_T'
    group by b.reference_date
),

poland as (
    select
        t.reference_date,
        t.address_points_total,
        b.covered_100plus,
        b.covered_1000plus,
        t.source_version,
        t.raw_sha256
    from territory as t
    inner join bandwidth as b on t.reference_date = b.reference_date
    where t.area_code = 'PL'
),

observations as (
    select reference_date, area_code, 'covered' as measure_code, 'address_points' as unit_code,
        cast(address_points as double) as value, source_version, raw_sha256
    from territory
    union all
    select reference_date, area_code, 'covered_share', 'pct',
        round(100.0 * address_points / address_points_total, 2), source_version, raw_sha256
    from territory
    union all
    select reference_date, 'PL', 'covered_100plus', 'address_points', cast(covered_100plus as double),
        source_version, raw_sha256
    from poland
    union all
    select reference_date, 'PL', 'covered_1000plus', 'address_points', cast(covered_1000plus as double),
        source_version, raw_sha256
    from poland
    union all
    select reference_date, 'PL', 'covered_100plus_share', 'pct', round(100.0 * covered_100plus / address_points_total, 2),
        source_version, raw_sha256
    from poland
    union all
    select reference_date, 'PL', 'covered_1000plus_share', 'pct',
        round(100.0 * covered_1000plus / address_points_total, 2), source_version, raw_sha256
    from poland
)

select
    'internet_gov_broadband' as source_system,
    'coverage' as block_id,
    'internet.gov.pl fixed broadband coverage of address points (quarterly)' as block_name,
    'internet_gov_broadband:coverage:' || o.unit_code || ':' || o.measure_code as measure_key,
    case o.measure_code
        when 'covered' then 'Address points within fixed broadband coverage, end of quarter'
        when 'covered_share' then 'Address points within fixed broadband coverage, % of all address points, end of quarter'
        when 'covered_100plus' then 'Address points within fixed broadband coverage of at least 100 Mb/s, end of quarter'
        when 'covered_1000plus' then 'Address points within fixed broadband coverage of at least 1 Gb/s, end of quarter'
        when 'covered_100plus_share'
            then 'Address points within fixed broadband coverage of at least 100 Mb/s, % of all address points, end of quarter'
        when 'covered_1000plus_share'
            then 'Address points within fixed broadband coverage of at least 1 Gb/s, % of all address points, end of quarter'
    end as measure_name,
    o.measure_code as measure_codes,
    o.unit_code,
    case o.unit_code when 'address_points' then 'address points' when 'pct' then 'per cent' end as unit_name,
    o.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'Q' as frequency,
    cast(date_trunc('quarter', o.reference_date) as date) as date_key,
    cast(year(o.reference_date) as varchar) || '-Q' || cast(quarter(o.reference_date) as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on o.area_code = g.source_code
