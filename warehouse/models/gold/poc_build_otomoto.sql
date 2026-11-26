{{ config(alias='build_otomoto', tags=['gold_block']) }}
{# Gold build stage: OTOMOTO Insights (register CD-034, #3264), block 'insights', from silver.otomoto_series: Poland,
   monthly, one series per measure and segment. measure_key otomoto:insights:<unit code>:<measure code>
   (otomoto:insights:listings:active_listings, otomoto:insights:jan2025_100:users_index, ...:buyers_index); the segment
   (the report's tab, as published) goes to breakdown_json, e.g. {"segment":"Samochody używane"}; no segment is a
   total. Monthly only, so one frequency per measure. Values and units as published (D33): listings as counted,
   indices with January 2025 = 100. Values that are not numbers (value null in Silver) are not loaded; a month after
   the build day cannot be an observation and is left out. Licence unresolved: internal (licence gate). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'otomoto'
)

select
    'otomoto' as source_system,
    'insights' as block_id,
    'OTOMOTO Insights: active listings and traffic by segment' as block_name,
    'otomoto:insights:' || s.unit_code || ':' || s.measure_code as measure_key,
    case s.measure_code
        when 'active_listings' then 'Average daily active listings on OTOMOTO'
        when 'users_index' then 'Monthly users of OTOMOTO, January 2025 = 100'
        when 'buyers_index' then 'Monthly buyers on OTOMOTO, January 2025 = 100'
    end as measure_name,
    s.measure_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'listings' then 'ogłoszenia (średnia dzienna liczba aktywnych)'
        when 'jan2025_100' then '% okresu bazowego 01.2025'
    end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    s.period_month as date_key,
    strftime(s.period_month, '%Y-%m') as period_label,
    cast(to_json(map(['segment'], [s.segment_label])) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    false as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('otomoto_series') }} as s
left join geo as g on g.source_code = 'PL'
where s.value is not null and s.period_month <= current_date
