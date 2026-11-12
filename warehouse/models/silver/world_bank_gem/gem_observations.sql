{# World Bank Global Economic Monitor (API source 15, bronze.gem_observations): one row per declared series ×
   EU27 member × period of the newest Landing copy of each series, typed. One answer mixes annual (2025),
   quarterly (2025Q3) and monthly (2025M07) codes of the same series: frequency A, Q or M and the first day of
   the period come from the code; another code fails the build. Values as published (series-specific units:
   %, index, LCU per US$, constant US$). A duplicated row fails the unique test. #}
with newest as (
    select *
    from {{ source('world_bank_gem', 'gem_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        *,
        case
            when regexp_full_match(time_code, '[0-9]{4}') then 'A'
            when regexp_full_match(time_code, '[0-9]{4}Q[1-4]') then 'Q'
            when regexp_full_match(time_code, '[0-9]{4}M(0[1-9]|1[0-2])') then 'M'
        end as frequency
    from newest
)

select
    entity,
    series_code,
    series_name,
    country_code,
    country_name,
    time_code,
    coalesce(frequency, error('gem_observations: unknown period code ' || coalesce(time_code, 'null'))) as frequency,
    case frequency
        when 'A' then make_date(cast(time_code as integer), 1, 1)
        when 'Q' then make_date(cast(left(time_code, 4) as integer), cast(right(time_code, 1) as integer) * 3 - 2, 1)
        when 'M' then make_date(cast(left(time_code, 4) as integer), cast(right(time_code, 2) as integer), 1)
    end as period_start,
    cast(value_text as double) as value,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from typed
