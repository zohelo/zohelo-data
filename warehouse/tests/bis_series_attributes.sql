-- BIS (#2763 review): the series attributes the BIS sends on rows without a period (WS_EER UNIT_MEASURE, WS_TC
-- titles) are filled onto the series' observations in silver.bis_observations. Each series must carry one value
-- per attribute; returns the series where the rows disagree (series_attribute_variants > 1).
select distinct
    entity,
    series_key,
    series_attribute_variants
from {{ ref('bis_observations') }}
where series_attribute_variants > 1
