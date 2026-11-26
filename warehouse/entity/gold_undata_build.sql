-- Gold build stage of the UN Statistical Yearbook tables (UNdata), by rule (docs/gold-coverage.md): every row
-- of silver.undata_syb_values with a value, every table, series and area, in the common observation shape
-- (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's
-- files as one block. Relations:
--   silver    the data files of silver.undata_syb_values (table × area × year × series, newest edition)
--   geo_map   the result of gold_undata_map.sql: UN M49 codes → countries, World and the EU27 members
--   measure_key = undata:undata_syb_values:<unit>:<table>:<series>@A: the Yearbook names a series by its text,
--                 which carries the unit in its last brackets ("GDP in current prices (millions of US
--                 dollars)"); the series text is the measure code as published ('|' would split the series
--                 key and becomes '/'). No breakdowns: a Yearbook series is one measure.
--   unit        = the text in the series name's last brackets as published (`millions of US dollars`,
--                 `percentage of GDP`: the unit_map.csv codes of UNdata, D33), else `as named`
--   date_key    = 1 January of the year (the Yearbook prints selected years)
--   geography   = by M49 code through the map; M49 regions and other aggregates have no Gold area and are kept
--                 as _X with their code
--   status      = as the curated build stage: a footnote sentence that is not an estimate note qualifies the
--                 definition, coverage or period (`definition_differs`); estimate notes only `estimated`; none ok
-- Not loaded: a cell without a number; a row footnoted as a projection (not an observation).
with placed as (
    select
        s.*,
        replace(s.series, '|', '/') as series_code,
        coalesce(nullif(trim(regexp_extract(s.series, '\(([^()]*)\)\s*$', 1)), ''), 'as named') as unit
    from silver as s
    where s.value is not null and s.year between 1800 and 2200 and s.series is not null
)

select
    'undata' as source_system,
    'undata_syb_values' as block_id,
    'UN Statistical Yearbook tables (UNdata)' as block_name,
    'undata:undata_syb_values:' || p.unit || ':' || p.entity || ':' || p.series_code || '@A' as measure_key,
    p.series as measure_name,
    p.entity || ':' || p.series_code as measure_codes,
    p.unit as unit_code,
    p.unit as unit_name,
    p.area_code as geography_source_code,
    case when p.area_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    'A' as frequency,
    make_date(p.year, 1, 1) as date_key,
    cast(p.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    case
        when nullif(trim(p.footnotes), '') is null then 'ok'
        when trim(regexp_replace(p.footnotes, '[^.;]*[Ee]stimat[^.;]*[.;]?', '', 'g'), ' .;') <> ''
            then 'definition_differs'
        else 'estimated'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.area_code = g.source_code
where coalesce(p.footnotes, '') not ilike '%project%'
