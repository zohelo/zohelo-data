{# Our World in Data chart metadata (register CD-031; source bronze.owid_charts, the newest copy of each chart's
   https://ourworldindata.org/grapher/<slug>.metadata.json): one row per chart and column, with the chart's title
   and citation (the attribution of the chart) and the column's unit, lastUpdated (the source's version of the
   series), nextUpdate, citationShort and indicator id, as published. Grain: chart × short_name. #}
select
    chart,
    short_name,
    column_title,
    chart_title,
    chart_citation,
    original_chart_url,
    title_short,
    unit,
    short_unit,
    try_cast(last_updated as date) as last_updated,
    last_updated as last_updated_text,
    next_update,
    timespan,
    value_type,
    owid_variable_id,
    citation_short,
    full_metadata_url,
    date_downloaded,
    raw_sha256,
    source_version,
    ingested_at
from {{ source('owid', 'owid_charts') }}
qualify rank() over (partition by entity order by ingested_at desc) = 1
