{# GUS STRATEG (register PLS-003): observed values of the SOR indicators (gus_strateg_indicators) from the newest copy
   of entity sor (bronze.gus_strateg_records, record_kind 'value'): level 0 (EU aggregates and EU countries, Poland
   100000000000) and level 2 (voivodeships, 10 + TERC voivodeship code + 00000000), by year, as published. value is the
   published text read as a number when it is one (digits, optional sign and decimal point); any other text keeps
   value null and value_status 'not_numeric' and never becomes 0 (none of 20,869 in Dev trial 45768290198: STRATEG
   leaves most missing values out of real_values and prints no marker). But it fills some years without voivodeship
   data with zeros: in that trial the at-risk-of-poverty rate after transfers (531), the extreme poverty rate (2409)
   and the share of farms of 1-5 ha (2404) are 0 in all 16 voivodeships in 7, 5 and 6 years, which no real rate is.
   A year in which every voivodeship value of an indicator is 0 is therefore no data: value null, value_status
   'zero_filled', value_text as published (288 rows in that trial). Single zeros stay values (railway lines above
   160 km/h in Poland before 2014, Malta's energy production ratio, budget balances of 0.0). A period that is not a
   year fails the build. The unit is the indicator's (SOR list). Grain: indicator_id x dimension_id x level_id x
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
),

zero_years as (
    -- indicator x year whose voivodeship values are all 0 (Poland, also coded at level 2, does not count)
    select
        indicator_id,
        dimension_id,
        period_year
    from typed
    where level_id = '2' and territory_code <> '100000000000'
    group by all
    having count(*) filter (where value = 0) = count(*)
)

select
    t.indicator_id,
    t.dimension_id,
    t.level_id,
    t.territory_code,
    t.period_year,
    case when z.indicator_id is null then t.value end as value,
    t.value_text,
    case
        when t.value is null then 'not_numeric'
        when z.indicator_id is not null then 'zero_filled'
        else 'ok'
    end as value_status,
    i.unit_code,
    t.raw_sha256,
    t.source_version
from typed as t
left join zero_years as z
    on
        t.level_id = '2'
        and t.territory_code <> '100000000000'
        and t.indicator_id = z.indicator_id
        and t.dimension_id = z.dimension_id
        and t.period_year = z.period_year
left join {{ ref('gus_strateg_indicators') }} as i
    on t.indicator_id = i.indicator_id and t.dimension_id = i.dimension_id
