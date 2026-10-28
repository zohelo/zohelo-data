{{ config(alias='measure_dimensions') }}
{# Gold PoC: which dimensions each activated measure offers (scenarios 2 and 3): geography
   levels, time (frequency and range) and every breakdown with its members. Derived from
   dim_series here; in v1 from the sources' structure definitions. #}
with series as (
    select
        s.*,
        g.level_name
    from {{ ref('poc_dim_series') }} as s
    left join {{ ref('poc_dim_geography') }} as g on s.geography_key = g.geography_key
),

geography as (
    select
        measure_key,
        'geography' as dimension,
        list(distinct level_name order by level_name) as members,
        count(distinct geography_key) as member_count,
        bool_or(geography_key = '_X') as has_unmatched
    from series
    group by measure_key
),

time_dim as (
    select
        measure_key,
        'time' as dimension,
        list(distinct frequency order by frequency) as members,
        count(distinct frequency) as member_count,
        false as has_unmatched
    from series
    group by measure_key
),

breakdowns as (
    select
        measure_key,
        key as dimension,
        list(
            distinct json_extract_string(breakdown_json, '$.' || key)
            order by json_extract_string(breakdown_json, '$.' || key)
        ) as members,
        count(distinct json_extract_string(breakdown_json, '$.' || key)) as member_count,
        false as has_unmatched
    from (
        select
            measure_key,
            breakdown_json,
            unnest(json_keys(breakdown_json)) as key
        from series
    ) as keyed
    group by measure_key, key
)

select
    measure_key,
    dimension,
    members,
    member_count,
    has_unmatched
from (
    select * from geography
    union all
    select * from time_dim
    union all
    select * from breakdowns
) as united
