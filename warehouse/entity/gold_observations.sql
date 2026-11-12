-- Gold observations of ONE block (docs/gold-coverage.md): the fact rows of the block's build stage
-- (`build`: the 21-column common observation shape), with the columns of gold.fact_*. One row per
-- series × period; warehouse/observations.py rejects the block when series_key × date_key repeats.
-- series_key as in the macro poc_series_key: measure | area (an unmatched area keeps its code) | breakdown.
select
    measure_key
    || '|'
    || geography_key
    || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
    || '|'
    || breakdown_json as series_key,
    measure_key,
    geography_key,
    date_key,
    frequency,
    value,
    observation_status_key,
    source_system,
    block_id,
    period_label,
    source_version,
    raw_sha256
from build
