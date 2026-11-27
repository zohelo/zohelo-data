-- Gold build stage of the World Bank Education Statistics (OECD PISA mean scores, API source 12), by rule
-- (docs/gold-coverage.md): every row of silver.edstats_pisa with a value, every subject, economy and assessment year,
-- in the common observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by
-- warehouse/observations.py to the table's files as one block. Relations:
--   silver    the data files of silver.edstats_pisa (subject × indicator × economy × assessment year, newest copy)
--   geo_map   gold.map_geography rows of EdStats (countries by ISO 3166-1 alpha-3, poc_geography__world_bank_edstats)
--   measure_key = world_bank_edstats:edstats_pisa:score:<base indicator>@A: one measure per subject (LO.PISA.MAT,
--                 LO.PISA.REA, LO.PISA.SCI), as the curated stage; the female (.FE) and male (.MA) indicators are
--                 the sex breakdown of that measure, {"sex": "T|F|M"} (T = the base indicator, is_total), never
--                 measures of their own
--   unit        = score (PISA scale points, as published; D33)
--   date_key    = 1 January of the assessment year (every three years, 2000-2018); a year outside 1800–2200 is not
--                 loaded
--   geography   = by ISO alpha-3 through map_geography; the OECD average (OED) and any code Gold has no country for
--                 stay _X with their code
--   status      = `ok` when obs_status is empty (every row today); any other flag is `flagged`, never silently ok
-- Not loaded: a cell without a number (no value in Silver; nothing is 0-filled, nor a value that is not a finite
-- number), and a year after the year of the source's last update (source_last_updated), which would not be an
-- assessment. An indicator that is not a subject or its .FE/.MA variant rejects the block (none today).
with placed as (
    select
        s.*,
        regexp_replace(s.indicator_id, '\.(FE|MA)$', '') as base_indicator,
        case when s.year between 1800 and 2200 then make_date(s.year, 1, 1) end as date_key,
        try_cast(nullif(regexp_extract(cast(s.source_last_updated as varchar), '(19|20)\d{2}'), '') as integer)
            as version_year
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
)

select
    'world_bank_edstats' as source_system,
    'edstats_pisa' as block_id,
    'OECD PISA mean scores via World Bank EdStats, every subject and economy' as block_name,
    -- an indicator that is not a subject or its .FE/.MA variant has no measure_key: the block is rejected, never cut
    case
        when regexp_full_match(p.indicator_id, 'LO\.PISA\.(MAT|REA|SCI)(\.FE|\.MA)?')
            then 'world_bank_edstats:edstats_pisa:score:' || p.base_indicator || '@A'
    end as measure_key,
    'PISA mean score, ' || case right(p.base_indicator, 3) when 'MAT' then 'mathematics' when 'REA' then 'reading'
        else 'science' end as measure_name,
    p.base_indicator as measure_codes,
    'score' as unit_code,
    'PISA score points' as unit_name,
    coalesce(p.country_iso3, p.country_id) as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    p.date_key,
    cast(p.year as varchar) as period_label,
    '{"sex": "' || p.sex_code || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    p.sex_code,
    p.sex_code = 'T' as is_total,
    p.value,
    case when p.obs_status is null or p.obs_status = '' then 'ok' else 'flagged' end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on coalesce(p.country_iso3, p.country_id) = g.source_code
where p.date_key is not null and (p.version_year is null or p.year <= p.version_year)
