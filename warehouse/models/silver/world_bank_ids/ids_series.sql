{# IDS series: every series of the series member of the newest archive (code, name, definitions, topic,
   source, aggregation method, licence), plus every series the observation member uses without a row in
   that member, with its name from the observations and in_series_metadata = false. The 7 Nov 2026 archive
   has one such series, DT.TDS.DECT.GN.ZS "Total debt service (% of GNI)", 6,305 observations (prod run
   43157805279, probe 43160144676), and one metadata series without observations (DT.NFL.NEBR.CD). #}
with newest as (
    select * from {{ source('world_bank_ids', 'ids_metadata') }}
    where
        source_version
        = (select max(latest.source_version) from {{ source('world_bank_ids', 'ids_metadata') }} as latest)
        and lower(member) like '%seriesmetadata.csv' and lower(member) not like '%country%'
),

described as (
    select
        trim(max(case when field = 'Code' then value_text end)) as series_code,
        max(case when field = 'Indicator Name' then value_text end) as series_name,
        max(case when field = 'Short definition' then value_text end) as short_definition,
        max(case when field = 'Long definition' then value_text end) as long_definition,
        max(case when field = 'Topic' then value_text end) as topic,
        max(case when field = 'Source' then value_text end) as source_note,
        max(case when field = 'Aggregation method' then value_text end) as aggregation_method,
        max(case when field = 'License Type' then value_text end) as license_type,
        max(raw_sha256) as raw_sha256
    from newest
    group by member, source_row_number
    having max(case when field = 'Code' then value_text end) is not null
),

observed as (
    select
        series_code,
        max(series_name) as series_name,
        max(raw_sha256) as raw_sha256
    from {{ ref('ids_observations') }}
    group by series_code
)

select
    *,
    true as in_series_metadata
from described
union all
select
    o.series_code,
    o.series_name,
    cast(null as varchar) as short_definition,
    cast(null as varchar) as long_definition,
    cast(null as varchar) as topic,
    cast(null as varchar) as source_note,
    cast(null as varchar) as aggregation_method,
    cast(null as varchar) as license_type,
    o.raw_sha256,
    false as in_series_metadata
from observed as o
anti join described as d on o.series_code = d.series_code
