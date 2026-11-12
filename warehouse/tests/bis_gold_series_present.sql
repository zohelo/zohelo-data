-- BIS (#2861): gold.build_bis takes the series of macro poc_bis_measures by their series code. A code the BIS
-- changes or withdraws would drop the measure without any other test failing (bis_gold_units only checks the series
-- that matched). Returns the listed series codes that match no row with a value in silver.bis_observations.
with measures as ({{ poc_bis_measures() }}),

series as (
    select
        o.entity,
        o.series_key,
        o.unit_measure,
        case o.entity
            when 'cbpol' then ''
            when 'eer'
                then
                    json_extract_string(o.dimensions_json, '$.EER_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.EER_BASKET')
            when 'spp'
                then
                    json_extract_string(o.dimensions_json, '$.VALUE')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_MEASURE')
            when 'tc'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.VALUATION')
                    || '.' || json_extract_string(o.dimensions_json, '$.UNIT_TYPE')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_ADJUST')
            when 'credit_gap'
                then
                    json_extract_string(o.dimensions_json, '$.TC_BORROWERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.TC_LENDERS')
                    || '.' || json_extract_string(o.dimensions_json, '$.CG_DTYPE')
            when 'dsr' then json_extract_string(o.dimensions_json, '$.DSR_BORROWERS')
        end as series_code
    from {{ ref('bis_observations') }} as o
    where o.obs_value is not null
    group by all
)

select
    m.entity,
    m.series_code
from measures as m
left join series as s on m.entity = s.entity and m.series_code = s.series_code
group by m.entity, m.series_code
having count(s.series_key) = 0
