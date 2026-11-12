-- Gold geography codes of ONE block (docs/gold-coverage.md): one row per source geography code the
-- block's build stage (`build`) holds, with the area it was mapped to (`_X`: unmatched, `_Z`: the block
-- has no geography) and its points. warehouse/observations.py compares these rows with
-- gold.map_geography, so a changed mapping rebuilds only the blocks that hold the code; it is also the
-- list of unmatched codes per block.
select
    any_value(block_id) as block_id,
    geography_source_code,
    any_value(geography_key) as geography_key,
    count(distinct geography_key) as geography_keys,
    count(*) as points
from build
group by geography_source_code
