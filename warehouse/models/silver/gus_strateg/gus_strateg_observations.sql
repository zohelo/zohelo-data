{# GUS STRATEG (register PLS-003): observed values of the SOR indicators (gus_strateg_indicators) from the newest copy
   of entity sor (bronze.gus_strateg_records, record_kind 'value'): level 0 (EU aggregates and EU countries, Poland
   100000000000) and level 2 (voivodeships, 10 + TERC voivodeship code + 00000000), by year, as published. value is the
   published text read as a number when it is one (digits, optional sign and decimal point); any other text keeps
   value null and value_status 'not_numeric' and never becomes 0 (none in the trial of the PR); a period that is not
   a year fails the build. The unit is the indicator's (SOR list). Grain: indicator_id x dimension_id x level_id x
   territory_code x period_year. #}
with records as (
    select *
    from {{ source('gus_strateg', 'gus_strateg_records') }}
    where record_kind = 'value'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        cast(r.indicator_id as integer) as indicator_id,
        cast(r.dimension_id as integer) as dimension_id,
        r.level_id,
        r.territory_code,
        case
            when regexp_full_match(r.period, '[0-9]{4}') then cast(r.period as integer)
            else error('gus_strateg: period ' || coalesce(r.period, 'null') || ' in ' || r.member)
        end as period_year,
        r.value as value_text,
        case when regexp_full_match(r.value, '-?[0-9]+(\.[0-9]+)?') then cast(r.value as double) end as value,
        r.raw_sha256,
        r.source_version
    from records as r
)

select
    t.indicator_id,
    t.dimension_id,
    t.level_id,
    t.territory_code,
    t.period_year,
    t.value,
    t.value_text,
    case when t.value is null then 'not_numeric' else 'ok' end as value_status,
    i.unit_code,
    t.raw_sha256,
    t.source_version
from typed as t
left join {{ ref('gus_strateg_indicators') }} as i
    on t.indicator_id = i.indicator_id and t.dimension_id = i.dimension_id
