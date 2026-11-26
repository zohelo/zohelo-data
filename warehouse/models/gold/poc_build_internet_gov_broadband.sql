{{ config(alias='build_internet_gov_broadband', tags=['gold_block']) }}
{# Gold build stage: internet.gov.pl fixed broadband coverage of address points (register PL-ENV-034), quarterly
   observations of silver.internet_gov_broadband_coverage. Block 'coverage'; one frequency (Q), date_key = first day
   of the reference quarter (stock on its last day). Every value is printed in the report, except the two
   bandwidth sums; no share is calculated here:
   - covered, total: address points in coverage and all address points (territory table; Poland = SUMA row and the
     16 voivodeships);
   - covered_100plus, covered_1000plus: Poland, address points in coverage of at least 100 Mb/s and at least
     1000 Mb/s, the sum of the printed bandwidth classes from 100 and from 1000 upwards (the classes are exclusive
     rows; the register's rule);
   - actual, wholesale_only, theoretical_only, no_coverage: Poland, address points by kind of coverage as printed
     (service type table), each with the printed share of all address points as its own measure (<code>_share,
     per cent).
   The report has no suppression or missing marker; "0" and "0,00%" are printed zeros. #}
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

bandwidth as (
    select
        reference_date,
        sum(address_points) filter (where bandwidth_mbps >= 100) as covered_100plus,
        sum(address_points) filter (where bandwidth_mbps >= 1000) as covered_1000plus,
        source_version,
        raw_sha256
    from s
    where table_code = 'bandwidth' and class_code <> '_T'
    group by reference_date, source_version, raw_sha256  -- one edition file per reference date (Silver)
),

service as (
    select
        reference_date,
        case class_code
            when 'rzeczywisty' then 'actual'
            when 'hurtowy' then 'wholesale_only'
            when 'teoretyczny' then 'theoretical_only'
            when 'brak_zasiegu' then 'no_coverage'
        end as measure_code,
        address_points,
        address_points_share_pct,
        source_version,
        raw_sha256
    from s
    where table_code = 'service_type' and class_code <> '_T'
),

observations as (
    select
        reference_date,
        area_code,
        'covered' as measure_code,
        'address_points' as unit_code,
        cast(address_points as double) as value,
        source_version,
        raw_sha256
    from s
    where table_code = 'territory'
    union all
    select
        reference_date,
        area_code,
        'total' as measure_code,
        'address_points' as unit_code,
        cast(address_points_total as double) as value,
        source_version,
        raw_sha256
    from s
    where table_code = 'territory'
    union all
    select
        reference_date,
        'PL' as area_code,
        'covered_100plus' as measure_code,
        'address_points' as unit_code,
        cast(covered_100plus as double) as value,
        source_version,
        raw_sha256
    from bandwidth
    union all
    select
        reference_date,
        'PL' as area_code,
        'covered_1000plus' as measure_code,
        'address_points' as unit_code,
        cast(covered_1000plus as double) as value,
        source_version,
        raw_sha256
    from bandwidth
    union all
    select
        reference_date,
        'PL' as area_code,
        measure_code,
        'address_points' as unit_code,
        cast(address_points as double) as value,
        source_version,
        raw_sha256
    from service
    union all
    select
        reference_date,
        'PL' as area_code,
        measure_code || '_share' as measure_code,
        'pct' as unit_code,
        cast(address_points_share_pct as double) as value,
        source_version,
        raw_sha256
    from service
)

select
    'internet_gov_broadband' as source_system,
    'coverage' as block_id,
    'internet.gov.pl fixed broadband coverage of address points (quarterly)' as block_name,
    'internet_gov_broadband:coverage:' || o.unit_code || ':' || o.measure_code as measure_key,
    case o.measure_code
        when 'covered' then 'Address points within fixed broadband coverage, end of quarter'
        when 'total' then 'All address points, end of quarter'
        when 'covered_100plus'
            then 'Address points within fixed broadband coverage of at least 100 Mb/s, end of quarter'
        when 'covered_1000plus'
            then 'Address points within fixed broadband coverage of at least 1 Gb/s, end of quarter'
        when 'actual' then 'Address points with actual fixed broadband service, end of quarter'
        when 'wholesale_only' then 'Address points with wholesale fixed broadband coverage only, end of quarter'
        when 'theoretical_only' then 'Address points with theoretical fixed broadband coverage only, end of quarter'
        when 'no_coverage' then 'Address points without fixed broadband coverage, end of quarter'
        when 'actual_share' then 'Address points with actual fixed broadband service, % of all address points'
        when 'wholesale_only_share'
            then 'Address points with wholesale fixed broadband coverage only, % of all address points'
        when 'theoretical_only_share'
            then 'Address points with theoretical fixed broadband coverage only, % of all address points'
        when 'no_coverage_share' then 'Address points without fixed broadband coverage, % of all address points'
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
